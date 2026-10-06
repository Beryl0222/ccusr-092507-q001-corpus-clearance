"""SQLite 事件存储、读模型投影与持久作业队。

事件是唯一事实来源；投影表在每次写事件的同一事务内增量更新，
服务启动时再全量重放一次以保证重启后状态一致。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
  seq            INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id       TEXT NOT NULL UNIQUE,
  event_type     TEXT NOT NULL,
  aggregate_type TEXT NOT NULL,
  aggregate_id   TEXT NOT NULL,
  version        INTEGER NOT NULL,
  occurred_at    TEXT NOT NULL,
  payload        TEXT NOT NULL,
  UNIQUE(aggregate_id, version)
);

-- 相同批次 + 相同内容指纹的重传直接返回首次结果
CREATE TABLE IF NOT EXISTS ingest_dedup (
  batch_id     TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  asset_id     TEXT NOT NULL,
  version      INTEGER NOT NULL,
  quarantined  INTEGER NOT NULL,
  seq          INTEGER NOT NULL,
  PRIMARY KEY (batch_id, content_hash)
);

-- 一个申请只能有一个有效决定；新决定在同一事务内作废旧决定
CREATE TABLE IF NOT EXISTS active_decisions (
  request_id  TEXT PRIMARY KEY,
  decision_id TEXT NOT NULL,
  seq         INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
  id          TEXT PRIMARY KEY,
  kind        TEXT NOT NULL,
  ref_key     TEXT NOT NULL,
  payload     TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'pending',
  run_after   TEXT NOT NULL,
  lease_owner TEXT,
  lease_until TEXT,
  attempts    INTEGER NOT NULL DEFAULT 0,
  last_error  TEXT,
  created_seq INTEGER NOT NULL,
  UNIQUE(kind, ref_key)
);

-- 命令幂等：同一 Idempotency-Key 返回首次响应
CREATE TABLE IF NOT EXISTS command_idempotency (
  idempotency_key TEXT PRIMARY KEY,
  status_code     INTEGER NOT NULL,
  body            TEXT NOT NULL,
  seq             INTEGER NOT NULL
);

-- ── 读模型投影 ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS p_orgs (
  org_id TEXT PRIMARY KEY,
  name   TEXT NOT NULL,
  roles  TEXT NOT NULL,
  seq    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS p_asset_versions (
  asset_id     TEXT NOT NULL,
  version      INTEGER NOT NULL,
  contributor  TEXT NOT NULL,
  source_id    TEXT NOT NULL,
  batch_id     TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  grant_print  TEXT NOT NULL,
  body_ref     TEXT,
  flags        TEXT NOT NULL,
  status       TEXT NOT NULL,
  seq          INTEGER NOT NULL,
  PRIMARY KEY (asset_id, version)
);

CREATE TABLE IF NOT EXISTS p_assets (
  asset_id        TEXT PRIMARY KEY,
  current_version INTEGER NOT NULL,
  status          TEXT NOT NULL,
  disputed        INTEGER NOT NULL DEFAULT 0,
  updated_seq     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS p_grants (
  grant_id      TEXT PRIMARY KEY,
  asset_id      TEXT NOT NULL,
  asset_version INTEGER NOT NULL,
  granter_org   TEXT NOT NULL,
  territories   TEXT NOT NULL,
  languages     TEXT NOT NULL,
  products      TEXT NOT NULL,
  valid_from    TEXT NOT NULL,
  retain_until  TEXT NOT NULL,
  status        TEXT NOT NULL,
  withdrawn_at  TEXT,
  reason        TEXT,
  updated_seq   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS p_slices (
  slice_id     TEXT PRIMARY KEY,
  asset_id     TEXT NOT NULL,
  asset_version INTEGER NOT NULL,
  ordinal      INTEGER NOT NULL,
  content_hash TEXT NOT NULL,
  text         TEXT,
  flags        TEXT NOT NULL,
  status       TEXT NOT NULL,
  updated_seq  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS p_slice_grants (
  slice_id TEXT NOT NULL,
  grant_id TEXT NOT NULL,
  PRIMARY KEY (slice_id, grant_id)
);

CREATE TABLE IF NOT EXISTS p_reviews (
  slice_id    TEXT NOT NULL,
  duty        TEXT NOT NULL,
  reviewer_id TEXT NOT NULL,
  passed      INTEGER NOT NULL,
  note        TEXT,
  seq         INTEGER NOT NULL,
  PRIMARY KEY (slice_id, duty)
);

CREATE TABLE IF NOT EXISTS p_requests (
  request_id     TEXT PRIMARY KEY,
  applicant_org  TEXT NOT NULL,
  purpose        TEXT NOT NULL,
  territory      TEXT NOT NULL,
  language       TEXT NOT NULL,
  product        TEXT NOT NULL,
  slice_ids      TEXT NOT NULL,
  materials_hash TEXT NOT NULL,
  status         TEXT NOT NULL,
  created_seq    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS p_decisions (
  decision_id   TEXT PRIMARY KEY,
  request_id    TEXT NOT NULL,
  verdict       TEXT NOT NULL,
  active        INTEGER NOT NULL,
  territory     TEXT NOT NULL,
  language      TEXT NOT NULL,
  product       TEXT NOT NULL,
  valid_from    TEXT NOT NULL,
  valid_until   TEXT,
  basis         TEXT NOT NULL,
  materials_hash TEXT NOT NULL,
  decider_user  TEXT NOT NULL,
  decider_org   TEXT NOT NULL,
  reason        TEXT,
  seq           INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS p_datasets (
  dataset_id     TEXT PRIMARY KEY,
  product        TEXT NOT NULL,
  creator_org    TEXT NOT NULL,
  status         TEXT NOT NULL,
  published_basis TEXT,
  created_seq    INTEGER NOT NULL,
  updated_seq    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS p_dataset_slices (
  dataset_id TEXT NOT NULL,
  slice_id   TEXT NOT NULL,
  PRIMARY KEY (dataset_id, slice_id)
);

CREATE TABLE IF NOT EXISTS p_dataset_decisions (
  dataset_id  TEXT NOT NULL,
  decision_id TEXT NOT NULL,
  PRIMARY KEY (dataset_id, decision_id)
);

CREATE TABLE IF NOT EXISTS p_obligations (
  obligation_id TEXT PRIMARY KEY,
  dataset_id    TEXT NOT NULL,
  ref_key       TEXT NOT NULL,
  kind          TEXT NOT NULL,
  detail        TEXT NOT NULL,
  status        TEXT NOT NULL,
  created_seq   INTEGER NOT NULL,
  fulfilled_seq INTEGER,
  UNIQUE(dataset_id, ref_key)
);
"""

PROJECTION_TABLES = [
    "p_orgs",
    "p_asset_versions",
    "p_assets",
    "p_grants",
    "p_slices",
    "p_slice_grants",
    "p_reviews",
    "p_requests",
    "p_decisions",
    "p_datasets",
    "p_dataset_slices",
    "p_dataset_decisions",
    "p_obligations",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_utc(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("时间必须携带时区")
    return dt.astimezone(timezone.utc)


class EventStore:
    """线程安全的 SQLite 事件存储。"""

    def __init__(self, path: str = ":memory:", projector: Callable[[sqlite3.Connection, dict[str, Any]], None] | None = None):
        self.path = path
        self._lock = threading.RLock()
        self._depth = 0
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._projector = projector
        with self._lock:
            self.conn.executescript(SCHEMA)
            self._migrate()

    def _migrate(self) -> None:
        """对已存在的数据库补齐后加列（新库由 SCHEMA 直接建出）。"""
        columns = {r["name"] for r in self.conn.execute("PRAGMA table_info(p_assets)")}
        if "disputed" not in columns:
            self.conn.execute("ALTER TABLE p_assets ADD COLUMN disputed INTEGER NOT NULL DEFAULT 0")
        grant_cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(p_grants)")}
        if "withdrawn_at" not in grant_cols:
            self.conn.execute("ALTER TABLE p_grants ADD COLUMN withdrawn_at TEXT")

    @contextmanager
    def transaction(self):
        """可重入的立即写事务；最外层提交，异常回滚。

        业务上的“检查后写入”必须在同一事务内完成，才能用单写者锁
        把跨机构并发放行收敛为一个有效决定。
        """
        with self._lock:
            if self._depth == 0:
                self.conn.execute("BEGIN IMMEDIATE")
            self._depth += 1
            try:
                yield self.conn
            except Exception:
                if self._depth == 1:
                    self.conn.execute("ROLLBACK")
                self._depth -= 1
                raise
            else:
                self._depth -= 1
                if self._depth == 0:
                    self.conn.execute("COMMIT")

    # ── 线程安全读方法（所有连接访问都必须持锁）────────────────
    def fetchone(self, sql: str, params: tuple = ()):
        with self._lock:
            return self.conn.execute(sql, params).fetchone()

    def fetchall(self, sql: str, params: tuple = ()):
        with self._lock:
            return self.conn.execute(sql, params).fetchall()

    def execute(self, sql: str, params: tuple = ()):
        with self._lock:
            return self.conn.execute(sql, params)

    # ── 事件 ────────────────────────────────────────────────────
    def append(
        self,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        payload: dict[str, Any],
        occurred_at: datetime | None = None,
    ) -> dict[str, Any]:
        """在写事务内追加事件并同步更新投影，返回落库事件。"""
        with self.transaction():
            row = self.conn.execute(
                "SELECT COALESCE(MAX(version), 0) AS v FROM events WHERE aggregate_id=?",
                (aggregate_id,),
            ).fetchone()
            version = row["v"] + 1
            ts = (occurred_at or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
            event = {
                "event_id": f"{aggregate_type}:{aggregate_id}:{version}",
                "event_type": event_type,
                "aggregate_type": aggregate_type,
                "aggregate_id": aggregate_id,
                "version": version,
                "occurred_at": ts,
                "payload": payload,
            }
            cur = self.conn.execute(
                "INSERT INTO events(event_id,event_type,aggregate_type,aggregate_id,version,occurred_at,payload)"
                " VALUES(?,?,?,?,?,?,?)",
                (
                    event["event_id"],
                    event_type,
                    aggregate_type,
                    aggregate_id,
                    version,
                    ts,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                ),
            )
            event["seq"] = cur.lastrowid
            if self._projector is not None:
                self._projector(self.conn, event)
            return event

    def events(self, aggregate_id: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if aggregate_id is None:
                rows = self.conn.execute("SELECT * FROM events ORDER BY seq").fetchall()
            else:
                rows = self.conn.execute(
                    "SELECT * FROM events WHERE aggregate_id=? ORDER BY seq", (aggregate_id,)
                ).fetchall()
        return [self._row_to_event(r) for r in rows]

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> dict[str, Any]:
        event = {
            "event_id": row["event_id"],
            "event_type": row["event_type"],
            "aggregate_type": row["aggregate_type"],
            "aggregate_id": row["aggregate_id"],
            "version": row["version"],
            "occurred_at": row["occurred_at"],
            "payload": json.loads(row["payload"]),
            "seq": row["seq"],
        }
        return event

    def rebuild_projections(self) -> int:
        """清空投影并按 seq 重放全部事件（重启后继续未完成状态的基础）。"""
        assert self._projector is not None
        with self.transaction():
            assert self._projector is not None
            for table in PROJECTION_TABLES:
                self.conn.execute(f"DELETE FROM {table}")
            # 去重与有效决定索引也由事件重建，作业队与命令幂等保留
            self.conn.execute("DELETE FROM ingest_dedup")
            self.conn.execute("DELETE FROM active_decisions")
            rows = self.conn.execute("SELECT * FROM events ORDER BY seq").fetchall()
            for row in rows:
                self._projector(self.conn, self._row_to_event(row))
            return len(rows)

    # ── 摄取去重 ────────────────────────────────────────────────
    def find_ingest(self, batch_id: str, content_hash: str) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM ingest_dedup WHERE batch_id=? AND content_hash=?",
                (batch_id, content_hash),
            ).fetchone()

    def remember_ingest(
        self, batch_id: str, content_hash: str, asset_id: str, version: int, quarantined: bool, seq: int
    ) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT OR IGNORE INTO ingest_dedup(batch_id,content_hash,asset_id,version,quarantined,seq)"
                " VALUES(?,?,?,?,?,?)",
                (batch_id, content_hash, asset_id, version, 1 if quarantined else 0, seq),
            )

    # ── 作业队 ──────────────────────────────────────────────────
    def enqueue_job(
        self,
        job_id: str,
        kind: str,
        ref_key: str,
        payload: dict[str, Any],
        run_after: datetime,
    ) -> None:
        """登记作业；同 kind+ref_key 已存在则保留最早的待办，不重复入队。"""
        with self._lock:
            row = self.conn.execute(
                "SELECT seq FROM events ORDER BY seq DESC LIMIT 1"
            ).fetchone()
            last_seq = row["seq"] if row else 0
            self.conn.execute(
                "INSERT INTO jobs(id,kind,ref_key,payload,status,run_after,created_seq) VALUES(?,?,?,?,'pending',?,?)"
                " ON CONFLICT(kind,ref_key) DO NOTHING",
                (job_id, kind, ref_key, json.dumps(payload, ensure_ascii=False), to_utc(run_after).isoformat(), last_seq),
            )

    def due_jobs(self, now: datetime, lease_owner: str, lease_seconds: int = 60) -> list[sqlite3.Row]:
        """领取到期作业，把崩溃中断的租约重新置为待办（重启续跑）。"""
        with self.transaction():
            now_utc = to_utc(now)
            now_iso = now_utc.isoformat()
            lease_dt = (now_utc + timedelta(seconds=lease_seconds)).isoformat()
            self.conn.execute(
                "UPDATE jobs SET status='pending', lease_owner=NULL, lease_until=NULL, "
                "attempts=attempts+1 WHERE status='leased' AND lease_until < ?",
                (now_iso,),
            )
            rows = self.conn.execute(
                "SELECT * FROM jobs WHERE status='pending' AND run_after <= ? ORDER BY created_seq, rowid",
                (now_iso,),
            ).fetchall()
            claimed = []
            for row in rows:
                cur = self.conn.execute(
                    "UPDATE jobs SET status='leased', lease_owner=?, lease_until=?, attempts=attempts+1 "
                    "WHERE id=? AND status='pending'",
                    (lease_owner, lease_dt, row["id"]),
                )
                if cur.rowcount:
                    claimed.append(self.conn.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone())
            return claimed

    def finish_job(self, job_id: str) -> None:
        with self._lock:
            self.conn.execute(
                "UPDATE jobs SET status='done', lease_owner=NULL, lease_until=NULL, last_error=NULL WHERE id=?",
                (job_id,),
            )

    def fail_job(self, job_id: str, error: str, retry_after: datetime) -> None:
        with self._lock:
            row = self.conn.execute("SELECT attempts FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row and row["attempts"] >= 10:
                self.conn.execute(
                    "UPDATE jobs SET status='dead', lease_owner=NULL, lease_until=NULL, last_error=? WHERE id=?",
                    (error[:500], job_id),
                )
            else:
                self.conn.execute(
                    "UPDATE jobs SET status='pending', lease_owner=NULL, lease_until=NULL,"
                    " run_after=?, last_error=? WHERE id=?",
                    (to_utc(retry_after).isoformat(), error[:500], job_id),
                )

    def list_jobs(self) -> list[sqlite3.Row]:
        with self._lock:
            return self.conn.execute("SELECT * FROM jobs ORDER BY created_seq, rowid").fetchall()

    # ── 命令幂等 ────────────────────────────────────────────────
    def get_idempotent(self, key: str) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM command_idempotency WHERE idempotency_key=?", (key,)
            ).fetchone()

    def put_idempotent(self, key: str, status_code: int, body: dict[str, Any]) -> None:
        with self._lock:
            row = self.conn.execute("SELECT seq FROM events ORDER BY seq DESC LIMIT 1").fetchone()
            last_seq = row["seq"] if row else 0
            self.conn.execute(
                "INSERT OR IGNORE INTO command_idempotency(idempotency_key,status_code,body,seq)"
                " VALUES(?,?,?,?)",
                (key, status_code, json.dumps(body, ensure_ascii=False), last_seq),
            )

    def close(self) -> None:
        with self._lock:
            self.conn.close()
