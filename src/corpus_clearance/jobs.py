"""持久作业处理器与循环调度器。

作业种类：
- ``grant_expiry``：权利依据保留期届满 → 过期 + 冻结受影响切片与派生物；
- ``withdrawal``：权利撤回的切片/派生物冻结传播。

作业状态持久化在 SQLite；进程重启后：
1. 投影按事件全量重放（见 EventStore.rebuild_projections）；
2. 未完成（pending/leased 中断）作业由新进程领取；
3. 处理器全部幂等，重放不产生重复冻结或重复义务。
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from datetime import datetime, timedelta, timezone

from .errors import DomainError
from .store import to_utc

log = logging.getLogger("corpus_clearance.jobs")


class JobRunner:
    """同步执行一批到期作业。可独立于 HTTP 进程使用（CLI/测试）。"""

    def __init__(self, service, lease_seconds: int = 60):
        self.service = service
        self.lease_seconds = lease_seconds
        self.owner = f"runner-{uuid.uuid4().hex[:8]}"

    def run_due(self, now: datetime | None = None) -> dict[str, int]:
        now = now or self.service.now()
        claimed = self.service.store.due_jobs(now, self.owner, self.lease_seconds)
        counts = {"withdrawal": 0, "grant_expiry": 0, "other": 0, "failed": 0}
        for job in claimed:
            try:
                self._handle(job)
                self.service.store.finish_job(job["id"])
                kind = job["kind"]
                counts[kind if kind in counts else "other"] += 1
            except _RetryLater as retry:
                # 未来生效的撤回提前被领取：回到生效时刻再执行
                self.service.store.fail_job(job["id"], "等待撤回生效", retry.until)
            except Exception as exc:  # 作业失败留痕并重试，最多 10 次进 dead
                if isinstance(exc, DomainError) and getattr(exc, "code", "") == "grant_not_found":
                    # 数据已不存在的作业直接完成，避免无限重试
                    self.service.store.finish_job(job["id"])
                    continue
                retry_at = self.service.now() + timedelta(seconds=min(300, 2 ** min(counts["failed"], 6)))
                self.service.store.fail_job(job["id"], repr(exc), retry_at)
                counts["failed"] += 1
                log.exception("作业 %s 处理失败", job["id"])
        return counts

    def _handle(self, job) -> None:
        payload = json.loads(job["payload"])
        if job["kind"] == "withdrawal":
            effective = payload.get("effective_at")
            if effective and to_utc(effective) > self.service.now():
                # 尚未到生效时刻，提前领取的撤回重新排队
                raise _RetryLater(to_utc(effective))
            self.service.propagate_withdrawal(
                payload["grant_id"], payload["reason"], effective or self.service.now()
            )
        elif job["kind"] == "grant_expiry":
            # 处理器幂等：若已过期则 propagate 无新事件
            row = self.service.store.fetchone(
                "SELECT * FROM p_grants WHERE grant_id=?", (payload["grant_id"],)
            )
            if row is None:
                return
            if row["status"] == "active" and to_utc(row["retain_until"]) <= self.service.now():
                with self.service.store.transaction():
                    fresh = self.service.store.fetchone(
                        "SELECT status FROM p_grants WHERE grant_id=?", (payload["grant_id"],)
                    )
                    if fresh["status"] == "active":
                        self.service.store.append(
                            "GRANT_EXPIRED",
                            "rights_grant",
                            payload["grant_id"],
                            {"grant_id": payload["grant_id"], "retain_until": row["retain_until"]},
                        )
                self.service.propagate_withdrawal(payload["grant_id"], "保留期届满", self.service.now())
        else:
            raise ValueError(f"未知作业种类 {job['kind']}")


class _RetryLater(Exception):
    def __init__(self, until: datetime):
        self.until = until


class BackgroundScheduler(threading.Thread):
    """周期性执行到期作业的守护线程。"""

    def __init__(self, runner: JobRunner, *, interval_seconds: float = 1.0, **kwargs):
        super().__init__(daemon=True, name="corpus-clearance-scheduler", **kwargs)
        self.runner = runner
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                self.runner.run_due()
            except Exception:
                log.exception("调度轮询异常")
            self._stop.wait(self.interval_seconds)

    def run_once(self) -> dict[str, int]:
        return self.runner.run_due()
