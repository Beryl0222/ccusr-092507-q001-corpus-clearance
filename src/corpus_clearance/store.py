"""SQLite 事件存储、幂等键、投影更新与后台作业表。

写入采用 ``BEGIN IMMEDIATELY`` 串行化：跨机构并发提交决定时，业务前置检查
（guard）在写事务内基于已提交投影执行，配合部分唯一索引，保证同一申请只能
产生一个有效放行决定。投影与事件在同一事务内更新，可随时由事件流重建。
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from collections.abc import Iterator, Sequence
from typing import Any, Callable

from . import projections

SCHEMA_VERSION = "1"

_DDL = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    version INTEGER NOT NULL,
    payload TEXT NOT NULL,
    UNIQUE (aggregate_id, version)
);

CREATE INDEX IF NOT EXISTS idx_events_aggregate ON events (aggregate_type, aggregate_id, version);
CREATE INDEX IF NOT EXISTS idx_events_type_time ON events (event_type, seq);

CREATE TABLE IF NOT EXISTS aggregates (
    aggregate_id TEXT PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    version INTEGER NOT NULL,
    last_seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    idempotency_key TEXT PRIMARY KEY,
    first_seq INTEGER NOT NULL,
    last_seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL,
    due_at TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 20,
    locked_by TEXT,
    locked_until TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs (status, due_at);
"""


def append_within_tx(
    conn: sqlite3.Connection,
    events: Sequence[dict[str, Any]],
    *,
    idempotency_key: str | None = None,
    guard: Callable[[sqlite3.Connection], None] | None = None,
) -> list[dict[str, Any]]:
    """在已打开的写事务内原子追加事件并更新投影。

    ``guard`` 在写锁内、新事件落库前执行，用于基于最新投影做业务前置校验。
    命中幂等键时返回该键首次产生的事件；注意调用方事务可能还包含其他写入，
    因此仅在业务写入与幂等点重合的服务方法中使用幂等键。
    """
    if not events:
        return []

    if idempotency_key is not None:
        row = conn.execute(
            "SELECT first_seq, last_seq FROM idempotency_keys WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
        if row is not None:
            return _load_seq_range(conn, row["first_seq"], row["last_seq"])

    if guard is not None:
        guard(conn)

    first_seq: int | None = None
    for event in events:
        current = conn.execute(
            "SELECT version FROM aggregates WHERE aggregate_id = ?",
            (event["aggregate_id"],),
        ).fetchone()
        expected = 1 if current is None else current["version"] + 1
        if event["version"] != expected:
            from .domain import ConflictError

            raise ConflictError(
                "聚合版本冲突，可能存在并发写入，请基于最新状态重试",
                details={
                    "aggregate_id": event["aggregate_id"],
                    "expected_version": expected,
                    "submitted_version": event["version"],
                },
            )
        cur = conn.execute(
            "INSERT INTO events(event_id, event_type, aggregate_type, aggregate_id, "
            "occurred_at, version, payload) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                event["event_id"],
                event["event_type"],
                event["aggregate_type"],
                event["aggregate_id"],
                event["occurred_at"],
                event["version"],
                json.dumps(event["payload"], ensure_ascii=False),
            ),
        )
        seq = cur.lastrowid
        first_seq = seq if first_seq is None else first_seq
        conn.execute(
            "INSERT INTO aggregates(aggregate_id, aggregate_type, version, last_seq) "
            "VALUES (?, ?, ?, ?) ON CONFLICT(aggregate_id) DO UPDATE SET "
            "version = excluded.version, last_seq = excluded.last_seq",
            (event["aggregate_id"], event["aggregate_type"], event["version"], seq),
        )
        projections.apply(conn, event)

    if idempotency_key is not None:
        conn.execute(
            "INSERT INTO idempotency_keys(idempotency_key, first_seq, last_seq) "
            "VALUES (?, ?, ?)",
            (idempotency_key, first_seq, seq),
        )
    return list(events)


def _load_seq_range(conn: sqlite3.Connection, first_seq: int, last_seq: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM events WHERE seq BETWEEN ? AND ? ORDER BY seq",
        (first_seq, last_seq),
    ).fetchall()
    return [row_to_event(row) for row in rows]


def row_to_event(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "event_id": row["event_id"],
        "event_type": row["event_type"],
        "aggregate_type": row["aggregate_type"],
        "aggregate_id": row["aggregate_id"],
        "occurred_at": row["occurred_at"],
        "version": row["version"],
        "payload": json.loads(row["payload"]),
    }


class EventStore:
    """单个 SQLite 文件上的事件库；可被多进程/多线程并发打开。"""

    def __init__(self, path: str) -> None:
        self.path = path

    # -- 连接 ----------------------------------------------------------------

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def init(self) -> None:
        conn = self.connect()
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(_DDL)
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (SCHEMA_VERSION,),
            )
            conn.commit()
            projections.create_tables(conn)
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def tx(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            if immediate:
                conn.execute("BEGIN IMMEDIATE")
            else:
                conn.execute("BEGIN")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @contextmanager
    def ro(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()

    # -- 事件写入（便捷封装） --------------------------------------------------

    def append_many(
        self,
        events: Sequence[dict[str, Any]],
        *,
        idempotency_key: str | None = None,
        guard: Callable[[sqlite3.Connection], None] | None = None,
    ) -> list[dict[str, Any]]:
        with self.tx() as conn:
            return append_within_tx(
                conn, events, idempotency_key=idempotency_key, guard=guard
            )

    def append(
        self,
        event: dict[str, Any],
        *,
        idempotency_key: str | None = None,
        guard: Callable[[sqlite3.Connection], None] | None = None,
    ) -> dict[str, Any]:
        return self.append_many([event], idempotency_key=idempotency_key, guard=guard)[0]

    # -- 读取 ----------------------------------------------------------------

    def events_for(self, aggregate_type: str, aggregate_id: str) -> list[dict[str, Any]]:
        with self.ro() as conn:
            rows = conn.execute(
                "SELECT * FROM events WHERE aggregate_type = ? AND aggregate_id = ? ORDER BY version",
                (aggregate_type, aggregate_id),
            ).fetchall()
            return [row_to_event(row) for row in rows]

    def all_events(self) -> list[dict[str, Any]]:
        with self.ro() as conn:
            rows = conn.execute("SELECT * FROM events ORDER BY seq").fetchall()
            return [row_to_event(row) for row in rows]

    def last_seq(self) -> int:
        with self.ro() as conn:
            row = conn.execute("SELECT COALESCE(MAX(seq), 0) AS s FROM events").fetchone()
            return int(row["s"])

    def find_idempotent(self, idempotency_key: str) -> list[dict[str, Any]] | None:
        with self.ro() as conn:
            row = conn.execute(
                "SELECT first_seq, last_seq FROM idempotency_keys WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            return None if row is None else _load_seq_range(conn, row["first_seq"], row["last_seq"])

    # -- 后台作业 -------------------------------------------------------------

    def run_due_jobs(
        self,
        handlers: dict[str, Callable[[sqlite3.Connection, dict, str], None]],
        *,
        worker_id: str,
    ) -> int:
        """认领并执行一批到期作业，每个作业独立事务提交/回滚。"""
        from . import jobs as jobs_mod

        conn = self.connect()
        try:
            return jobs_mod.run_due(conn, handlers, worker_id=worker_id)
        finally:
            conn.close()

    # -- 投影重建 -------------------------------------------------------------

    def rebuild_projections(self) -> int:
        """丢弃并由全部事件重建聚合表与读模型，返回重放事件数。"""
        with self.tx() as conn:
            conn.execute("DELETE FROM aggregates")
            projections.reset(conn)
            count = 0
            rows = conn.execute("SELECT * FROM events ORDER BY seq").fetchall()
            for row in rows:
                event = row_to_event(row)
                conn.execute(
                    "INSERT INTO aggregates(aggregate_id, aggregate_type, version, last_seq) "
                    "VALUES (?, ?, ?, ?) ON CONFLICT(aggregate_id) DO UPDATE SET "
                    "version = excluded.version, aggregate_type = excluded.aggregate_type, "
                    "last_seq = excluded.last_seq",
                    (event["aggregate_id"], event["aggregate_type"], event["version"], row["seq"]),
                )
                projections.apply(conn, event)
                count += 1
            return count
