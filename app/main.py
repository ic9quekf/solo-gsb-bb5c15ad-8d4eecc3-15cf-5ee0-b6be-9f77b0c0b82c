import json
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import Body, FastAPI, Path, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .chain import record_hash
from .db import get_chain_anchor, get_conn, init_db

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
                # 事件表为空: 若曾压缩归档则从链锚点续链, 否则为创世空串
                _, prev_hash = get_chain_anchor(conn)
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


# ---------- retention: 安全归档压缩 ----------

@app.post("/retention/compact")
def compact(req: CompactIn):
    """压缩归档 (当前锚点, cutoff_seq] 的连续事件前缀。

    - 安全边界: cutoff_seq 不得超过所有已登记 consumer 的最小 cursor_seq;
      无 consumer 时以当前最大 seq 为边界。越界返回 409 并给出 safe_through_seq。
    - 执行时持久化最后一条被删记录的 seq/hash 作为链锚点, /integrity 从锚点继续校验。
    - dry_run=true 只返回预计删除数与边界, 不写库。
    - cutoff_seq 已被既往压缩覆盖时为幂等空操作, 不再校验安全边界。
    """
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        anchor_seq, anchor_hash = get_chain_anchor(conn)

        # 安全边界: 已登记 consumer 的最小游标; 一个都没有时为当前最大 seq
        row = conn.execute("SELECT MIN(cursor_seq) AS m FROM consumers").fetchone()
        if row["m"] is not None:
            safe_through_seq = row["m"]
        else:
            row = conn.execute("SELECT MAX(id) AS m FROM events").fetchone()
            safe_through_seq = row["m"] or 0

        # 幂等: 请求区间已被既往压缩覆盖, 无记录可删, 直接返回
        if req.cutoff_seq <= anchor_seq:
            result: dict[str, Any] = {
                "dry_run": req.dry_run,
                "from_seq": anchor_seq + 1,
                "through_seq": anchor_seq,  # from > through 表示空区间
                "safe_through_seq": safe_through_seq,
            }
            if req.dry_run:
                return {"status": "dry_run", **result, "would_delete": 0}
            return {
                "status": "noop",
                **result,
                "deleted": 0,
                "anchor_seq": anchor_seq,
                "anchor_hash": anchor_hash,
            }

        if req.cutoff_seq > safe_through_seq:
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "cutoff_seq exceeds safe retention boundary",
                    "cutoff_seq": req.cutoff_seq,
                    "safe_through_seq": safe_through_seq,
                },
            )

        # 连续前缀: 从锚点后一条起逐条连续, 遇断档即止
        rows = conn.execute(
            "SELECT id, hash FROM events WHERE id > ? AND id <= ? ORDER BY id",
            (anchor_seq, req.cutoff_seq),
        ).fetchall()
        prefix: list[sqlite3.Row] = []
        expected = anchor_seq + 1
        for r in rows:
            if r["id"] != expected:
                break
            prefix.append(r)
            expected += 1

        from_seq = anchor_seq + 1
        through_seq = prefix[-1]["id"] if prefix else anchor_seq

        if req.dry_run:
            # 只返回预计删除数与边界, 不写库
            return {
                "status": "dry_run",
                "dry_run": True,
                "would_delete": len(prefix),
                "from_seq": from_seq,
                "through_seq": through_seq,
                "safe_through_seq": safe_through_seq,
            }

        if prefix:
            conn.execute(
                "DELETE FROM events WHERE id BETWEEN ? AND ?",
                (from_seq, through_seq),
            )
            # 持久化链锚点: 最后一条被删记录的 seq/hash
            anchor_seq, anchor_hash = through_seq, prefix[-1]["hash"]
            conn.execute(
                "INSERT INTO chain_anchor (id, anchor_seq, anchor_hash) VALUES (1, ?, ?)"
                " ON CONFLICT(id) DO UPDATE SET anchor_seq = excluded.anchor_seq,"
                " anchor_hash = excluded.anchor_hash",
                (anchor_seq, anchor_hash),
            )

        return {
            "status": "compacted" if prefix else "noop",
            "dry_run": False,
            "deleted": len(prefix),
            "from_seq": from_seq,
            "through_seq": through_seq,
            "anchor_seq": anchor_seq,
            "anchor_hash": anchor_hash,
            "safe_through_seq": safe_through_seq,
        }


def _chain_break(checked: int, seq: int, event_id: str, reason: str) -> dict[str, Any]:
    return {
        "verified": False,
        "checked": checked,
        "first_break": {"seq": seq, "event_id": event_id, "reason": reason},
    }


@app.get("/integrity")
def integrity():
    """从链锚点开始校验哈希链: 序号连续性、前后哈希衔接、记录内容哈希。

    压缩归档后, 锚点(最后被删记录的 seq/hash)即校验起点; 全部事件已压缩时
    没有待校验记录, 仍返回 verified=true。
    """
    with get_conn() as conn:
        anchor_seq, anchor_hash = get_chain_anchor(conn)
        rows = conn.execute(
            "SELECT id, event_id, occurred_at, payload, received_at, prev_hash, hash"
            " FROM events WHERE id > ? ORDER BY id",
            (anchor_seq,),
        ).fetchall()

    checked = 0
    prev_seq = anchor_seq
    prev_hash = anchor_hash
    for row in rows:
        seq = row["id"]
        # 序号连续性: 相邻记录 seq 必须逐一递增(首条须为锚点 seq + 1)
        if seq != prev_seq + 1:
            return _chain_break(checked, seq, row["event_id"], "seq_gap")
        # 前后哈希衔接: prev_hash 必须等于上一条记录的 hash(首条为锚点哈希)
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

    return {
        "verified": True,
        "checked": checked,
        "first_break": None,
        "anchor_seq": anchor_seq,
    }
