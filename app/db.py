import json
import os
import sqlite3
from contextlib import contextmanager

from .chain import GENESIS_HASH, record_hash

DB_PATH = os.getenv("DB_PATH", "events.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,  -- 首次接收顺序
    event_id    TEXT NOT NULL UNIQUE,               -- 幂等键
    occurred_at TEXT NOT NULL,                      -- 事件发生时间(UTC ISO8601)
    payload     TEXT NOT NULL,                      -- 原始 JSON
    received_at TEXT NOT NULL,                      -- 首次接收时间(UTC ISO8601)
    prev_hash   TEXT,                               -- 上一条记录的哈希(首条为空串)
    hash        TEXT                                -- 本记录哈希(固定 JSON 规范的 SHA-256)
);
CREATE INDEX IF NOT EXISTS idx_events_occurred_at ON events (occurred_at);

CREATE TABLE IF NOT EXISTS consumers (
    consumer_id      TEXT PRIMARY KEY,
    cursor_seq       INTEGER NOT NULL DEFAULT 0,  -- 已确认到的 seq; 下次从 cursor_seq+1 投递(首次为 1)
    lease_token      TEXT,                        -- 当前租约令牌(无租约时为 NULL)
    lease_from_seq   INTEGER,                     -- 租约批次起始 seq
    lease_to_seq     INTEGER,                     -- 租约批次末尾 seq
    lease_expires_at TEXT                         -- 租约过期时间(UTC ISO8601)
);

CREATE TABLE IF NOT EXISTS chain_anchor (
    id          INTEGER PRIMARY KEY CHECK (id = 1),  -- 恒为 1 的单行表
    anchor_seq  INTEGER NOT NULL,                    -- 已压缩归档到的最大 seq
    anchor_hash TEXT NOT NULL                        -- 最后被压缩记录的哈希, 作为后续校验的 prev_hash 起点
);
"""


def get_chain_anchor(conn: sqlite3.Connection) -> tuple[int, str]:
    """读取链锚点(压缩归档删到的最大 seq 及其哈希); 从未压缩时为 (0, 创世空串)。"""
    row = conn.execute(
        "SELECT anchor_seq, anchor_hash FROM chain_anchor WHERE id = 1"
    ).fetchone()
    if row is None:
        return 0, GENESIS_HASH
    return row["anchor_seq"], row["anchor_hash"]


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        _migrate_chain_columns(conn)
        _backfill_chain(conn)


def _migrate_chain_columns(conn: sqlite3.Connection) -> None:
    """兼容旧库: 缺失链字段时补列。"""
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(events)")}
    if "prev_hash" not in cols:
        conn.execute("ALTER TABLE events ADD COLUMN prev_hash TEXT")
    if "hash" not in cols:
        conn.execute("ALTER TABLE events ADD COLUMN hash TEXT")


def _backfill_chain(conn: sqlite3.Connection) -> None:
    """为历史数据补齐哈希链(按首次接收顺序重算, 已有哈希的记录原样衔接)。

    从链锚点之后开始: 已压缩归档的记录不在表内, 锚点哈希作为续链起点。
    """
    anchor_seq, prev_hash = get_chain_anchor(conn)
    rows = conn.execute(
        "SELECT id, event_id, occurred_at, payload, received_at, hash"
        " FROM events WHERE id > ? ORDER BY id",
        (anchor_seq,),
    ).fetchall()
    for row in rows:
        if row["hash"]:
            prev_hash = row["hash"]
            continue
        h = record_hash(
            seq=row["id"],
            event_id=row["event_id"],
            occurred_at=row["occurred_at"],
            payload=json.loads(row["payload"]),
            received_at=row["received_at"],
            prev_hash=prev_hash,
        )
        conn.execute(
            "UPDATE events SET prev_hash = ?, hash = ? WHERE id = ?",
            (prev_hash, h, row["id"]),
        )
        prev_hash = h


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()
