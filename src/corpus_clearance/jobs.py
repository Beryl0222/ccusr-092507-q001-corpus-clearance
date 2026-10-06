"""持久化后台作业：到期扫描与撤回传播。

作业状态机：``pending`` →（到期被认领）→ ``running`` → ``done``；
失败按退避重新变为 ``pending``，超过次数变 ``dead``。认领带租约
（``locked_until``），服务重启后租约过期的运行中作业会被重新认领，
从而"继续未完成的到期扫描和撤回传播"。
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import timedelta
from typing import Any, Callable

from .domain import now_utc

LEASE_SECONDS = 60
MAX_ATTEMPTS = 20


def _iso(dt) -> str:
    return dt.isoformat()


def schedule(
    conn: sqlite3.Connection,
    job_type: str,
    payload: dict[str, Any],
    *,
    due_at,
    job_id: str | None = None,
) -> str:
    """在调用方事务内登记作业。已存在同 id 作业时不重复登记。"""
    job_id = job_id or f"job-{uuid.uuid4().hex}"
    now = _iso(now_utc())
    conn.execute(
        "INSERT INTO jobs(job_id, job_type, status, due_at, payload, created_at, updated_at) "
        "VALUES (?, ?, 'pending', ?, ?, ?, ?) ON CONFLICT(job_id) DO NOTHING",
        (job_id, job_type, _iso(due_at) if hasattr(due_at, "isoformat") else due_at,
         json.dumps(payload, ensure_ascii=False), now, now),
    )
    return job_id


def due_jobs(conn: sqlite3.Connection, *, worker_id: str, limit: int = 8) -> list[sqlite3.Row]:
    """原子认领到期作业（含租约过期的 running 作业）。"""
    now = now_utc()
    conn.execute(
        "UPDATE jobs SET status = 'running', locked_by = ?, locked_until = ?, attempts = attempts + 1, "
        "updated_at = ? WHERE job_id IN (SELECT job_id FROM jobs WHERE "
        "(status = 'pending' AND due_at <= ?) OR "
        "(status = 'running' AND locked_until IS NOT NULL AND locked_until < ?) "
        "ORDER BY due_at LIMIT ?)",
        (worker_id, _iso(now + timedelta(seconds=LEASE_SECONDS)), _iso(now),
         _iso(now), _iso(now), limit),
    )
    return conn.execute(
        "SELECT * FROM jobs WHERE status = 'running' AND locked_by = ? ORDER BY due_at",
        (worker_id,),
    ).fetchall()


def heartbeat(conn: sqlite3.Connection, job_id: str, *, worker_id: str) -> None:
    conn.execute(
        "UPDATE jobs SET locked_until = ?, updated_at = ? WHERE job_id = ? AND locked_by = ?",
        (_iso(now_utc() + timedelta(seconds=LEASE_SECONDS)), _iso(now_utc()), job_id, worker_id),
    )


def complete(conn: sqlite3.Connection, job_id: str, *, worker_id: str) -> None:
    conn.execute(
        "UPDATE jobs SET status = 'done', locked_by = NULL, locked_until = NULL, "
        "last_error = NULL, updated_at = ? WHERE job_id = ? AND locked_by = ?",
        (_iso(now_utc()), job_id, worker_id),
    )


def fail(conn: sqlite3.Connection, job_id: str, error: str, *, worker_id: str) -> None:
    row = conn.execute("SELECT attempts, max_attempts FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
    now = now_utc()
    if row is not None and row["attempts"] >= row["max_attempts"]:
        conn.execute(
            "UPDATE jobs SET status = 'dead', locked_by = NULL, locked_until = NULL, "
            "last_error = ?, updated_at = ? WHERE job_id = ?",
            (error, _iso(now), job_id),
        )
        return
    # 指数退避，最少 5 秒后重试
    delay = min(300, max(5, 2 ** min(row["attempts"], 8)))
    conn.execute(
        "UPDATE jobs SET status = 'pending', locked_by = NULL, locked_until = NULL, "
        "due_at = ?, last_error = ?, updated_at = ? WHERE job_id = ?",
        (_iso(now + timedelta(seconds=delay)), error, _iso(now), job_id),
    )


def stats(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status").fetchall()
    return {row["status"]: row["n"] for row in rows}


def ensure_recurring(
    conn: sqlite3.Connection,
    job_id: str,
    job_type: str,
    payload: dict[str, Any],
    *,
    due_at,
) -> None:
    """登记周期作业；已完成的同一作业重新挂起到期，运行中的保持不动。

    服务重启时调用：缺失则补登记，``done`` 则重新到期，``pending/running``
    维持原状（未完成的扫描继续）。
    """
    now = _iso(now_utc())
    due = _iso(due_at) if hasattr(due_at, "isoformat") else due_at
    conn.execute(
        "INSERT INTO jobs(job_id, job_type, status, due_at, payload, created_at, updated_at) "
        "VALUES (?, ?, 'pending', ?, ?, ?, ?) ON CONFLICT(job_id) DO NOTHING",
        (job_id, job_type, due, json.dumps(payload, ensure_ascii=False), now, now),
    )
    conn.execute(
        "UPDATE jobs SET status = 'pending', due_at = ?, payload = ?, locked_by = NULL, "
        "locked_until = NULL, last_error = NULL, attempts = 0, updated_at = ? "
        "WHERE job_id = ? AND status = 'done'",
        (due, json.dumps(payload, ensure_ascii=False), now, job_id),
    )


def run_due(
    conn: sqlite3.Connection,
    handlers: dict[str, Callable[[sqlite3.Connection, dict, str], None]],
    *,
    worker_id: str,
) -> int:
    """认领并执行一批到期作业。

    认领先独立提交；每个作业在自己的事务中运行，失败按退避挂回或判死，
    不影响同批其他作业。
    """
    with conn:  # 认领事务
        jobs = due_jobs(conn, worker_id=worker_id)
    for row in jobs:
        payload = json.loads(row["payload"])
        handler = handlers.get(row["job_type"])
        try:
            if handler is None:
                raise RuntimeError(f"没有登记作业处理器: {row['job_type']}")
            conn.execute("BEGIN IMMEDIATE")
            result = handler(conn, payload, row["job_id"])
            if isinstance(result, dict) and result.get("reschedule_at") is not None:
                # 周期作业：业务事件与重挂在同一事务提交，不会丢失下一轮
                due = result["reschedule_at"]
                conn.execute(
                    "UPDATE jobs SET status = 'pending', due_at = ?, locked_by = NULL, "
                    "locked_until = NULL, attempts = 0, updated_at = ? WHERE job_id = ?",
                    (_iso(due), _iso(now_utc()), row["job_id"]),
                )
            else:
                complete(conn, row["job_id"], worker_id=worker_id)
            conn.commit()
        except Exception as exc:  # noqa: BLE001 - 作业失败须持久化并继续后续作业
            conn.rollback()
            with conn:  # 独立事务记录失败
                fail(conn, row["job_id"], repr(exc), worker_id=worker_id)
    return len(jobs)
