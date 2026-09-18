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

CREATE TABLE IF NOT EXISTS retention_anchor (
    id          INTEGER PRIMARY KEY CHECK (id = 1),  -- 恒为单行
    anchor_seq  INTEGER NOT NULL,                    -- 最后被压缩删除记录的 seq(初始 0 = 创世点)
    anchor_hash TEXT NOT NULL                        -- 最后被压缩删除记录的 hash(初始为创世空串)
);
"""


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        # 压缩锚点初始化为创世点(seq=0 / 空串), 旧库启动时自动补建
        conn.execute(
            "INSERT OR IGNORE INTO retention_anchor (id, anchor_seq, anchor_hash)"
            " VALUES (1, 0, ?)",
            (GENESIS_HASH,),
        )
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
    """为历史数据补齐哈希链(按首次接收顺序重算, 已有哈希的记录原样衔接)。"""
    rows = conn.execute(
        "SELECT id, event_id, occurred_at, payload, received_at, hash"
        " FROM events ORDER BY id"
    ).fetchall()
    prev_hash = GENESIS_HASH
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
