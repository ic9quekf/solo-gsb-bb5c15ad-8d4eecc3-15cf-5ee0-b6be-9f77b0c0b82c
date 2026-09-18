import json
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import Body, FastAPI, Path, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .chain import GENESIS_HASH, record_hash
from .db import get_conn, init_db

app = FastAPI(title="Webhook Event Receiver & Replay")


@app.on_event("startup")
def startup() -> None:
    init_db()


# ---------- models ----------

class EventIn(BaseModel):
    event_id: str = Field(..., min_length=1, max_length=255)
    occurred_at: datetime
    payload: dict[str, Any]


class ReplayIn(BaseModel):
    event_ids: list[str] = Field(..., min_length=1, max_length=1000)


class PullIn(BaseModel):
    limit: int = Field(100, ge=1, le=1000)
    lease_seconds: int = Field(30, ge=1, le=86400)


class AckIn(BaseModel):
    lease_token: str = Field(..., min_length=1, max_length=255)


class CompactIn(BaseModel):
    cutoff_seq: int = Field(..., ge=0)
    dry_run: bool = False


# ---------- helpers ----------

def normalize_dt(dt: datetime) -> str:
    """归一化为 UTC ISO8601 字符串, 保证可按字典序做时间范围比较。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def lease_conflict(detail: str) -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": detail})


def row_to_event(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "seq": row["id"],  # 首次接收顺序号
        "event_id": row["event_id"],
        "occurred_at": row["occurred_at"],
        "payload": json.loads(row["payload"]),
        "received_at": row["received_at"],
    }


# ---------- endpoints ----------

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/events", status_code=201)
def create_event(event: EventIn):
    """接收单个事件, 按 event_id 幂等: 重复投递不产生重复记录, 也不改变哈希链。"""
    received_at = utc_now()
    occurred_at = normalize_dt(event.occurred_at)
    try:
        with get_conn() as conn:
            # BEGIN IMMEDIATE 提前获取写锁, 保证"读链头 -> 写入"在同一事务内原子完成
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT hash FROM events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                prev_hash = row["hash"]
            else:
                # 全部历史事件已被压缩: 新事件必须衔接压缩锚点而非创世点
                anchor = conn.execute(
                    "SELECT anchor_hash FROM retention_anchor WHERE id = 1"
                ).fetchone()
                prev_hash = anchor["anchor_hash"] if anchor else GENESIS_HASH
            cur = conn.execute(
                "INSERT INTO events (event_id, occurred_at, payload, received_at)"
                " VALUES (?, ?, ?, ?)",
                (
                    event.event_id,
                    occurred_at,
                    json.dumps(event.payload, ensure_ascii=False),
                    received_at,
                ),
            )
            seq = cur.lastrowid
            conn.execute(
                "UPDATE events SET prev_hash = ?, hash = ? WHERE id = ?",
                (
                    prev_hash,
                    record_hash(
                        seq=seq,
                        event_id=event.event_id,
                        occurred_at=occurred_at,
                        payload=event.payload,
                        received_at=received_at,
                        prev_hash=prev_hash,
                    ),
                    seq,
                ),
            )
    except sqlite3.IntegrityError:
        # event_id 已存在 -> 幂等返回, 不写入重复记录, 链保持不变
        return JSONResponse(
            status_code=200,
            content={"status": "duplicate", "event_id": event.event_id},
        )
    return {"status": "created", "event_id": event.event_id}


@app.get("/events")
def list_events(
    from_: Optional[datetime] = Query(None, alias="from", description="occurred_at 下界(含)"),
    to: Optional[datetime] = Query(None, description="occurred_at 上界(含)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """按 occurred_at 时间范围分页查询。"""
    clauses, params = [], []
    if from_ is not None:
        clauses.append("occurred_at >= ?")
        params.append(normalize_dt(from_))
    if to is not None:
        clauses.append("occurred_at <= ?")
        params.append(normalize_dt(to))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with get_conn() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) AS c FROM events {where}", params
        ).fetchone()["c"]
        rows = conn.execute(
            f"SELECT * FROM events {where} ORDER BY occurred_at, id LIMIT ? OFFSET ?",
            (*params, page_size, (page - 1) * page_size),
        ).fetchall()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [row_to_event(r) for r in rows],
    }


@app.post("/replay")
def replay(req: ReplayIn):
    """批量重放: 命中的事件按首次接收顺序输出, 未命中的标记为 skipped。"""
    # 请求内去重, 保证重复 id 不产生重复输出
    requested = list(dict.fromkeys(req.event_ids))

    found: dict[str, dict[str, Any]] = {}
    with get_conn() as conn:
        placeholders = ",".join("?" * len(requested))
        rows = conn.execute(
            f"SELECT * FROM events WHERE event_id IN ({placeholders}) ORDER BY id",
            requested,
        ).fetchall()
    for r in rows:
        found[r["event_id"]] = row_to_event(r)

    results: list[dict[str, Any]] = []
    # 命中部分: 严格按首次接收顺序(seq)输出
    for event in sorted(found.values(), key=lambda e: e["seq"]):
        results.append({"event_id": event["event_id"], "status": "replayed", "event": event})
    # 未命中部分: 按请求顺序追加, 标记 skipped
    for eid in requested:
        if eid not in found:
            results.append({"event_id": eid, "status": "skipped", "reason": "not_found"})

    replayed = sum(1 for r in results if r["status"] == "replayed")
    return {
        "replayed": replayed,
        "skipped": len(results) - replayed,
        "results": results,
    }


# ---------- consumer cursor: pull / ack ----------

@app.post("/consumers/{consumer_id}/pull")
def pull(
    consumer_id: str = Path(..., min_length=1, max_length=255),
    req: PullIn = Body(default_factory=PullIn),
):
    """按首次接收顺序拉取尚未确认的连续事件并原子租用。

    - 首次 consumer 的游标为 0, 即从 seq=1 开始投递。
    - 存在未过期租约时, 重复拉取原样返回同一批次(不续租、不换 token)。
    - 租约过期后同一批次可被重新投递(换发新 token)。
    - 队列为空(没有游标之后的事件)时返回空批次, 不创建租约。
    """
    now = utc_now()
    # BEGIN IMMEDIATE 与事件写入互斥, 保证"读批次 -> 建租约"原子完成
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        # 首次出现的 consumer 自动建档, 游标从 0 开始
        conn.execute(
            "INSERT OR IGNORE INTO consumers (consumer_id, cursor_seq) VALUES (?, 0)",
            (consumer_id,),
        )
        consumer = conn.execute(
            "SELECT cursor_seq, lease_token, lease_from_seq, lease_to_seq,"
            " lease_expires_at FROM consumers WHERE consumer_id = ?",
            (consumer_id,),
        ).fetchone()

        active = (
            consumer["lease_token"] is not None
            and datetime.fromisoformat(consumer["lease_expires_at"]) > datetime.fromisoformat(now)
        )
        if active:
            # 租约未过期: 重复拉取必须返回同一批次
            rows = conn.execute(
                "SELECT * FROM events WHERE id BETWEEN ? AND ? ORDER BY id",
                (consumer["lease_from_seq"], consumer["lease_to_seq"]),
            ).fetchall()
            return {
                "consumer_id": consumer_id,
                "events": [row_to_event(r) for r in rows],
                "lease_token": consumer["lease_token"],
                "lease_expires_at": consumer["lease_expires_at"],
            }

        if consumer["lease_token"] is not None:
            # 上一租约已过期: 同一批次可重新投递(沿用原区间, 换发新 token)
            rows = conn.execute(
                "SELECT * FROM events WHERE id BETWEEN ? AND ? ORDER BY id",
                (consumer["lease_from_seq"], consumer["lease_to_seq"]),
            ).fetchall()
            token = secrets.token_urlsafe(32)
            expires_at = (
                datetime.fromisoformat(now) + timedelta(seconds=req.lease_seconds)
            ).isoformat()
            conn.execute(
                "UPDATE consumers SET lease_token = ?, lease_expires_at = ?"
                " WHERE consumer_id = ?",
                (token, expires_at, consumer_id),
            )
            return {
                "consumer_id": consumer_id,
                "events": [row_to_event(r) for r in rows],
                "lease_token": token,
                "lease_expires_at": expires_at,
            }

        # 从未租用: 从游标之后取一段连续事件
        rows = conn.execute(
            "SELECT * FROM events WHERE id > ? ORDER BY id LIMIT ?",
            (consumer["cursor_seq"], req.limit),
        ).fetchall()
        if not rows:
            # 空队列: 返回空批次且不创建租约
            return {
                "consumer_id": consumer_id,
                "events": [],
                "lease_token": None,
                "lease_expires_at": None,
            }

        from_seq, to_seq = rows[0]["id"], rows[-1]["id"]
        token = secrets.token_urlsafe(32)
        expires_at = (
            datetime.fromisoformat(now) + timedelta(seconds=req.lease_seconds)
        ).isoformat()
        conn.execute(
            "UPDATE consumers SET lease_token = ?, lease_from_seq = ?,"
            " lease_to_seq = ?, lease_expires_at = ? WHERE consumer_id = ?",
            (token, from_seq, to_seq, expires_at, consumer_id),
        )
        return {
            "consumer_id": consumer_id,
            "events": [row_to_event(r) for r in rows],
            "lease_token": token,
            "lease_expires_at": expires_at,
        }


@app.post("/consumers/{consumer_id}/ack")
def ack(
    consumer_id: str = Path(..., min_length=1, max_length=255),
    req: AckIn = Body(...),
):
    """确认当前租约批次: 仅接受未过期且与当前租约一致的 token, 游标推进到批次末尾。

    过期、伪造或旧 token 一律 409; 游标只随整批确认前移, 天然禁止越序确认。
    """
    now = datetime.now(timezone.utc)
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        consumer = conn.execute(
            "SELECT cursor_seq, lease_token, lease_from_seq, lease_to_seq,"
            " lease_expires_at FROM consumers WHERE consumer_id = ?",
            (consumer_id,),
        ).fetchone()
        # 未知 consumer / 从未拉取过: 没有可确认的租约
        if consumer is None or consumer["lease_token"] is None:
            return lease_conflict("no active lease")
        if consumer["lease_token"] != req.lease_token:
            # 伪造或旧 token(旧批次过期后已被新租约取代)
            return lease_conflict("invalid lease token")
        if datetime.fromisoformat(consumer["lease_expires_at"]) <= now:
            # 租约已过期, 该批次可被重新投递, 旧 token 不得再确认
            return lease_conflict("lease expired")

        to_seq = consumer["lease_to_seq"]
        conn.execute(
            "UPDATE consumers SET cursor_seq = ?, lease_token = NULL,"
            " lease_from_seq = NULL, lease_to_seq = NULL, lease_expires_at = NULL"
            " WHERE consumer_id = ?",
            (to_seq, consumer_id),
        )
    return {
        "consumer_id": consumer_id,
        "acknowledged": True,
        "cursor_seq": to_seq,
    }


# ---------- retention: compaction ----------

def _get_anchor(conn: sqlite3.Connection) -> tuple[int, str]:
    row = conn.execute(
        "SELECT anchor_seq, anchor_hash FROM retention_anchor WHERE id = 1"
    ).fetchone()
    # 旧库可能没有锚点行: 视为创世点
    return (row["anchor_seq"], row["anchor_hash"]) if row else (0, GENESIS_HASH)


def _safe_through_seq(conn: sqlite3.Connection) -> int:
    """安全压缩边界: 有 consumer 时为所有已登记 consumer 游标最小值,
    无 consumer 时为当前最大 seq(空库为 0)。"""
    min_cursor = conn.execute("SELECT MIN(cursor_seq) AS m FROM consumers").fetchone()["m"]
    if min_cursor is not None:
        return min_cursor
    return conn.execute("SELECT MAX(id) AS m FROM events").fetchone()["m"] or 0


def _compact_conflict(safe_through_seq: int) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": "cutoff_seq exceeds safe retention boundary",
            "safe_through_seq": safe_through_seq,
        },
    )


@app.post("/retention/compact")
def retention_compact(req: CompactIn):
    """安全归档(压缩)哈希链前缀:

    - 只可压缩"当前锚点后一条 ~ cutoff_seq"的连续前缀, 即删除 (anchor_seq, cutoff_seq]。
    - cutoff_seq 不得越过安全边界: 已登记 consumer 的最小 cursor_seq;
      无 consumer 时以当前最大 seq 为界; 越界返回 409 并给出 safe_through_seq。
    - 执行压缩时把最后删除记录的 seq/hash 持久化为新链锚点, 之后 /integrity 从锚点续校。
    - dry_run 只返回预计删除数与边界, 不写库。
    - 重复提交已覆盖区间幂等: 区间内已无记录, 删除数为 0, 锚点不变。
    """
    with get_conn() as conn:
        anchor_seq, _ = _get_anchor(conn)
        safe_through = _safe_through_seq(conn)
        if req.cutoff_seq > safe_through:
            return _compact_conflict(safe_through)

        if req.dry_run:
            would_delete = conn.execute(
                "SELECT COUNT(*) AS c FROM events WHERE id > ? AND id <= ?",
                (anchor_seq, req.cutoff_seq),
            ).fetchone()["c"]
            return {
                "dry_run": True,
                "anchor_seq": anchor_seq,
                "cutoff_seq": req.cutoff_seq,
                "safe_through_seq": safe_through,
                "would_delete": would_delete,
            }

        # 与事件写入互斥: 写事务内重读边界, 防止与新事件写入/游标推进并发竞争
        conn.execute("BEGIN IMMEDIATE")
        anchor_seq, _ = _get_anchor(conn)
        safe_through = _safe_through_seq(conn)
        if req.cutoff_seq > safe_through:
            # sqlite3 上下文管理器会提交; 此时尚未做任何修改, 提交无副作用
            return _compact_conflict(safe_through)

        # 待删区间的最后一条记录(seq 最大者)的 hash 即新链锚点
        last = conn.execute(
            "SELECT id, hash FROM events WHERE id > ? AND id <= ? ORDER BY id DESC"
            " LIMIT 1",
            (anchor_seq, req.cutoff_seq),
        ).fetchone()
        deleted = conn.execute(
            "DELETE FROM events WHERE id > ? AND id <= ?",
            (anchor_seq, req.cutoff_seq),
        ).rowcount
        if last is not None:
            conn.execute(
                "UPDATE retention_anchor SET anchor_seq = ?, anchor_hash = ?"
                " WHERE id = 1",
                (last["id"], last["hash"]),
            )
            anchor_seq, anchor_hash = last["id"], last["hash"]
        else:
            # 重复提交已覆盖区间(或 cutoff_seq 不超过锚点): 幂等空操作, 锚点不变
            anchor_seq, anchor_hash = _get_anchor(conn)

    return {
        "dry_run": False,
        "deleted": deleted,
        "anchor_seq": anchor_seq,
        "anchor_hash": anchor_hash,
        "cutoff_seq": req.cutoff_seq,
        "safe_through_seq": safe_through,
    }


def _chain_break(checked: int, seq: int, event_id: str, reason: str) -> dict[str, Any]:
    return {
        "verified": False,
        "checked": checked,
        "first_break": {"seq": seq, "event_id": event_id, "reason": reason},
    }


@app.get("/integrity")
def integrity():
    """从压缩锚点之后继续校验哈希链: 序号连续性、前后哈希衔接、记录内容哈希。

    被 /retention/compact 压缩掉的前缀以锚点(最后删除记录的 seq/hash)为信任起点,
    因此第一条剩余记录的 prev_hash 必须等于锚点 hash; 全部事件已压缩(只剩锚点)时
    仍返回 verified=true(checked=0)。无锚点时从创世点(seq=0 / 空串)校验全链。
    """
    with get_conn() as conn:
        anchor_seq, prev_hash = _get_anchor(conn)
        rows = conn.execute(
            "SELECT id, event_id, occurred_at, payload, received_at, prev_hash, hash"
            " FROM events WHERE id > ? ORDER BY id",
            (anchor_seq,),
        ).fetchall()

    checked = 0
    prev_seq: Optional[int] = anchor_seq
    for row in rows:
        seq = row["id"]
        # 序号连续性: 剩余首条必须恰好是锚点后一条, 之后逐一递增
        if seq != prev_seq + 1:
            return _chain_break(checked, seq, row["event_id"], "seq_gap")
        # 前后哈希衔接: prev_hash 必须等于上一条记录的 hash(首条剩余记录为锚点 hash)
        if row["prev_hash"] is None or row["prev_hash"] != prev_hash:
            return _chain_break(checked, seq, row["event_id"], "prev_hash_mismatch")
        # 事件字段: 按固定 JSON 规范重算哈希并与存储值比对
        if row["hash"] is None or row["hash"] != record_hash(
            seq=seq,
            event_id=row["event_id"],
            occurred_at=row["occurred_at"],
            payload=json.loads(row["payload"]),
            received_at=row["received_at"],
            prev_hash=row["prev_hash"],
        ):
            return _chain_break(checked, seq, row["event_id"], "hash_mismatch")
        checked += 1
        prev_seq = seq
        prev_hash = row["hash"]

    return {"verified": True, "checked": checked, "first_break": None}
