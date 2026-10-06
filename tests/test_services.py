"""服务层业务规则测试。"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from corpus_clearance.domain import (
    AuthorizationError,
    ConflictError,
    DomainError,
    NotFoundError,
    ValidationFailure,
    sha256_hex,
)
from corpus_clearance.services import SWEEP_JOB_ID, ClearanceService
from corpus_clearance.store import EventStore


def dt(y: int, m: int, d: int, *, h: int = 0) -> datetime:
    return datetime(y, m, d, h, tzinfo=timezone(timedelta(hours=8)))


class ServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "test.db")
        self.store = EventStore(self.db_path)
        self.store.init()
        self.svc = ClearanceService(self.store)
        self.svc.schedule_expiry_sweep()
        self.svc.register_org("xhs", "通讯社")
        self.svc.register_org("hnr", "海南日报")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    # -- 装置 ----------------------------------------------------------------

    def _ingest(self, asset_id: str = "a1", *, collector: str = "hnr",
                body: str = "正文", auth: str = "授权v1") -> dict:
        return self.svc.ingest_asset(
            batch_id="b1", collector_id=collector, collector_org_id=collector,
            asset_id=asset_id, content_hash=sha256_hex(body),
            authorization_fingerprint=sha256_hex(auth),
        )

    def _approved_slice(
        self, *, asset_id: str = "a1", right_id: str = "r1",
        territories=("CN",), languages=("zh",), products=("train", "govqa"),
        embargo=None, not_after=None, purpose_tags=("news",),
        desens_reviewer="redactor-a", fact_reviewer="fact-b",
        slice_hash: str = "段落1", decider="officer-zhang",
        applicant="team", decided_at: datetime | None = dt(2026, 9, 10),
    ) -> tuple[str, dict]:
        """建立一条通过双审并取得批准的切片，返回 (slice_id, decision)。"""
        self._ingest(asset_id)
        self.svc.declare_right(
            right_id=right_id, asset_id=asset_id, declarer_id="hnr",
            territories=list(territories), languages=list(languages),
            products=list(products), purpose_tags=list(purpose_tags),
            embargo_not_before=embargo, not_after=not_after,
        )
        sid = self.svc.cut_slices(asset_id, [{"content_hash": sha256_hex(slice_hash)}])[0]
        self.svc.review_slice(slice_id=sid, review_kind="desensitization",
                              reviewer_id=desens_reviewer, passed=True)
        self.svc.review_slice(slice_id=sid, review_kind="fact",
                              reviewer_id=fact_reviewer, passed=True)
        req = self.svc.create_request(
            applicant_id=applicant, purpose="news", slice_ids=[sid],
            territories=list(territories), languages=list(languages),
            products=list(products), materials={"v": 1},
        )
        decision = self.svc.decide(
            request_id=req["request_id"], decider_id=decider,
            approved=True, occurred_at=decided_at,
        )
        return sid, decision

    def run_jobs(self) -> int:
        return self.store.run_due_jobs(self.svc.job_handlers(), worker_id="test-worker")

    # -- 入库 / 重传 / 争议 ---------------------------------------------------

    def test_org_must_exist_before_ingest(self) -> None:
        with self.assertRaises(ValidationFailure):
            self.svc.ingest_asset(batch_id="b", collector_id="c",
                                  collector_org_id="unknown-org",
                                  content_hash=sha256_hex("x"))

    def test_same_batch_and_hash_is_safe_retransmission(self) -> None:
        first = self._ingest()
        self.assertEqual(first["status"], "ingested")
        again = self._ingest()
        self.assertEqual(again["status"], "retransmitted")
        self.assertEqual(again["asset_id"], first["asset_id"])
        # 只产生一个入库事件
        events = self.store.events_for("source_asset", "a1")
        self.assertEqual(1, len(events))

    def test_same_id_different_body_is_quarantined(self) -> None:
        self._ingest(body="原始正文")
        result = self.svc.ingest_asset(
            batch_id="b1", collector_id="hnr", collector_org_id="hnr",
            asset_id="a1", content_hash=sha256_hex("不同正文"),
            authorization_fingerprint=sha256_hex("授权v1"),
        )
        self.assertEqual("disputed", result["status"])
        self.assertTrue(result["quarantined"])
        with self.assertRaises(ConflictError):
            self.svc.declare_right(right_id="r1", asset_id="a1", declarer_id="hnr",
                                   territories=["CN"], languages=["zh"])

    def test_same_id_different_authorization_is_quarantined(self) -> None:
        self._ingest(auth="授权v1")
        result = self._ingest(auth="授权v2-范围缩小")
        self.assertEqual("disputed", result["status"])

    # -- 权利声明范围 ---------------------------------------------------------

    def test_collector_cannot_claim_beyond_own_scope(self) -> None:
        self._ingest(collector="hnr")
        with self.assertRaises(AuthorizationError):
            self.svc.declare_right(right_id="rx", asset_id="a1", declarer_id="xhs",
                                   territories=["*"], languages=["*"])

    def test_embargo_after_expiry_rejected(self) -> None:
        self._ingest()
        with self.assertRaises(ValidationFailure):
            self.svc.declare_right(
                right_id="r1", asset_id="a1", declarer_id="hnr",
                territories=["CN"], languages=["zh"],
                embargo_not_before=dt(2027, 1, 1), not_after=dt(2026, 12, 1),
            )

    def test_right_slice_must_belong_to_asset(self) -> None:
        self._ingest("a1")
        self._ingest("a2", body="另一篇")
        foreign = self.svc.cut_slices("a2", [{"content_hash": sha256_hex("x")}])[0]
        with self.assertRaises(ValidationFailure):
            self.svc.declare_right(right_id="r1", asset_id="a1", declarer_id="hnr",
                                   territories=["CN"], languages=["zh"],
                                   slice_ids=[foreign])

    # -- 双职责审校 -----------------------------------------------------------

    def test_approval_requires_both_review_kinds(self) -> None:
        self._ingest()
        self.svc.declare_right(right_id="r1", asset_id="a1", declarer_id="hnr",
                               territories=["CN"], languages=["zh"], products=["train"])
        sid = self.svc.cut_slices("a1", [{"content_hash": sha256_hex("s")}])[0]
        self.svc.review_slice(slice_id=sid, review_kind="desensitization",
                              reviewer_id="u1", passed=True)
        req = self.svc.create_request(
            applicant_id="t", purpose="news", slice_ids=[sid],
            territories=["CN"], languages=["zh"], products=["train"], materials={"v": 1})
        with self.assertRaises(ValidationFailure) as ctx:
            self.svc.decide(request_id=req["request_id"], decider_id="boss", approved=True)
        self.assertEqual(["review_incomplete"], ctx.exception.details["blockers"][sid])

    def test_reviewers_must_differ_by_duty(self) -> None:
        self._ingest()
        self.svc.declare_right(right_id="r1", asset_id="a1", declarer_id="hnr",
                               territories=["CN"], languages=["zh"], products=["train"])
        sid = self.svc.cut_slices("a1", [{"content_hash": sha256_hex("s")}])[0]
        self.svc.review_slice(slice_id=sid, review_kind="desensitization",
                              reviewer_id="same-person", passed=True)
        self.svc.review_slice(slice_id=sid, review_kind="fact",
                              reviewer_id="same-person", passed=True)
        req = self.svc.create_request(
            applicant_id="t", purpose="news", slice_ids=[sid],
            territories=["CN"], languages=["zh"], products=["train"], materials={"v": 1})
        with self.assertRaises(ValidationFailure) as ctx:
            self.svc.decide(request_id=req["request_id"], decider_id="boss", approved=True)
        self.assertEqual(["reviewer_must_differ"], ctx.exception.details["blockers"][sid])

    def test_failed_review_blocks_approval_but_latest_passing_record_wins(self) -> None:
        self._ingest()
        self.svc.declare_right(right_id="r1", asset_id="a1", declarer_id="hnr",
                               territories=["CN"], languages=["zh"], products=["train"])
        sid = self.svc.cut_slices("a1", [{"content_hash": sha256_hex("s")}])[0]
        self.svc.review_slice(slice_id=sid, review_kind="desensitization",
                              reviewer_id="u1", passed=False)
        self.svc.review_slice(slice_id=sid, review_kind="fact", reviewer_id="u2", passed=True)
        req = self.svc.create_request(
            applicant_id="t", purpose="news", slice_ids=[sid],
            territories=["CN"], languages=["zh"], products=["train"], materials={"v": 1})
        with self.assertRaises(ValidationFailure):
            self.svc.decide(request_id=req["request_id"], decider_id="boss", approved=True)
        # 脱敏整改后由本人复核通过
        self.svc.review_slice(slice_id=sid, review_kind="desensitization",
                              reviewer_id="u1", passed=True, notes="已重新脱敏")
        decision = self.svc.decide(request_id=req["request_id"], decider_id="boss",
                                   approved=True, occurred_at=dt(2026, 9, 10))
        self.assertTrue(decision["approved"])

    # -- 唯一有效决定 ---------------------------------------------------------

    def test_only_one_effective_decision_under_concurrent_approval(self) -> None:
        sid, decision = self._approved_slice()
        req_id = self.store.events_for("release_decision", decision["decision_id"])[0]["payload"]["request_id"]
        with self.assertRaises(ConflictError):
            self.svc.decide(request_id=req_id, decider_id="other-officer",
                            approved=False, reason="重复决定")

    def test_reject_requires_reason(self) -> None:
        self._ingest()
        sid = self.svc.cut_slices("a1", [{"content_hash": sha256_hex("s")}])[0]
        req = self.svc.create_request(
            applicant_id="t", purpose="news", slice_ids=[sid],
            territories=["CN"], languages=["zh"], products=["train"], materials={"v": 1})
        with self.assertRaises(ValidationFailure):
            self.svc.decide(request_id=req["request_id"], decider_id="boss", approved=False)

    # -- 材料变更 -------------------------------------------------------------

    def test_changed_materials_invalidate_old_approval(self) -> None:
        sid, _ = self._approved_slice()
        # 找到原申请
        request_id = self.svc.list_requests()[0]["request_id"]
        q = self.svc.slice_clearance(slice_id=sid, date="2026-10-01",
                                     territory="CN", language="zh", product="train")
        self.assertTrue(q["usable"])

        self.svc.supersede_request(old_request_id=request_id, applicant_id="team",
                                   materials={"v": 2, "scope": "expanded"})
        q2 = self.svc.slice_clearance(slice_id=sid, date="2026-10-01",
                                      territory="CN", language="zh", product="train")
        self.assertFalse(q2["usable"])
        self.assertIn("no_effective_decision", q2["blockers"])

    def test_supersede_requires_fingerprint_change(self) -> None:
        self._approved_slice()
        request_id = self.svc.list_requests()[0]["request_id"]
        with self.assertRaises(ConflictError):
            self.svc.supersede_request(old_request_id=request_id, applicant_id="team",
                                       materials={"v": 1})

    def test_supersede_by_other_applicant_forbidden(self) -> None:
        self._approved_slice()
        request_id = self.svc.list_requests()[0]["request_id"]
        with self.assertRaises(AuthorizationError):
            self.svc.supersede_request(old_request_id=request_id,
                                       applicant_id="someone-else", materials={"v": 9})

    # -- 资格判定 -------------------------------------------------------------

    def test_clearance_dimensions_and_embargo_expiry(self) -> None:
        sid, _ = self._approved_slice(
            embargo=dt(2026, 10, 1), not_after=dt(2026, 12, 1),
            decided_at=dt(2026, 10, 2),
        )
        ok = self.svc.slice_clearance(slice_id=sid, date="2026-10-02",
                                      territory="CN", language="zh", product="train")
        self.assertTrue(ok["usable"])
        embargoed = self.svc.slice_clearance(slice_id=sid, date="2026-09-30",
                                             territory="CN", language="zh", product="train")
        self.assertFalse(embargoed["usable"])
        self.assertIn("no_covering_right", embargoed["blockers"])
        expired = self.svc.slice_clearance(slice_id=sid, date="2026-12-02",
                                           territory="CN", language="zh", product="train")
        self.assertFalse(expired["usable"])
        # 授权只覆盖 zh/CN/train+govqa
        us = self.svc.slice_clearance(slice_id=sid, date="2026-10-02",
                                      territory="US", language="zh", product="train")
        self.assertFalse(us["usable"])
        en = self.svc.slice_clearance(slice_id=sid, date="2026-10-02",
                                      territory="CN", language="en", product="train")
        self.assertFalse(en["usable"])
        intl = self.svc.slice_clearance(slice_id=sid, date="2026-10-02",
                                        territory="CN", language="zh", product="intl-pack")
        self.assertFalse(intl["usable"])

    # -- 撤回传播 -------------------------------------------------------------

    def test_withdraw_freezes_only_affected_slices_and_derivations(self) -> None:
        # a1/r1 下两个切片；另设资产 a2 不应被波及
        self._ingest("a1")
        self.svc.declare_right(right_id="r1", asset_id="a1", declarer_id="hnr",
                               territories=["CN"], languages=["zh"], products=["train"])
        self.svc.cut_slices("a1", [
            {"content_hash": sha256_hex("p1")},
            {"content_hash": sha256_hex("p2")},
        ])
        sid_a, sid_b = [r["slice_id"] for r in self.svc.trace_asset("a1")["slices"]]
        for sid in (sid_a, sid_b):
            self.svc.review_slice(slice_id=sid, review_kind="desensitization",
                                  reviewer_id="ra", passed=True)
            self.svc.review_slice(slice_id=sid, review_kind="fact",
                                  reviewer_id="rb", passed=True)

        # 第二条权利只覆盖 sid_b
        self.svc.declare_right(right_id="r2", asset_id="a1", declarer_id="hnr",
                               territories=["CN"], languages=["zh"], products=["train"],
                               slice_ids=[sid_b])
        self._ingest("a2", body="其他报道")
        self.svc.declare_right(right_id="r3", asset_id="a2", declarer_id="hnr",
                               territories=["CN"], languages=["zh"], products=["train"])
        sid_other = self.svc.cut_slices("a2", [{"content_hash": sha256_hex("o")}])[0]

        # 数据集：train 含两切片；derived 由 train 派生；other-ds 含 a2 切片
        self.svc.register_dataset(dataset_id="ds-train", name="训练集", product="train",
                                  slice_ids=[sid_a, sid_b])
        self.svc.register_dataset(dataset_id="ds-derived", name="微调集", product="train",
                                  slice_ids=[])
        self.svc.record_derivation(parent_dataset_id="ds-train",
                                   child_dataset_id="ds-derived")
        self.svc.register_dataset(dataset_id="ds-other", name="其他集", product="train",
                                  slice_ids=[sid_other])

        # sid_a 在撤回前已发布 → 应有处置义务；sid_b 未发布
        self.svc.record_publication(slice_id=sid_a, published_at=dt(2026, 9, 20),
                                    basis={"channel": "终端"})

        result = self.svc.withdraw_right(
            right_id="r1", effective_at=dt(2026, 10, 1),
            reason="通讯社撤回", actor_id="legal",
        )
        self.assertIn(sid_a, result["affected_slice_ids"])
        self.run_jobs()

        qa = self.svc.slice_clearance(slice_id=sid_a, date="2026-10-02",
                                      territory="CN", language="zh", product="train")
        self.assertIn("slice_frozen", qa["blockers"])
        # sid_b 仍有 r2 覆盖，不冻结
        qb = self.svc.slice_clearance(slice_id=sid_b, date="2026-10-02",
                                      territory="CN", language="zh", product="train")
        self.assertNotIn("slice_frozen", qb["blockers"])
        # a2 完全不受影响
        qo = self.svc.slice_clearance(slice_id=sid_other, date="2026-10-02",
                                      territory="CN", language="zh", product="train")
        self.assertNotIn("slice_frozen", qo["blockers"])

        train = self.svc.dataset_manifest("ds-train")
        derived = self.svc.dataset_manifest("ds-derived")
        other = self.svc.dataset_manifest("ds-other")
        self.assertTrue(train["frozen"])
        self.assertTrue(derived["frozen"])  # 冻结沿派生边传播
        self.assertFalse(other["frozen"])

        dispositions = self.svc.list_open_dispositions()
        self.assertEqual(1, len(dispositions))
        self.assertEqual(sid_a, dispositions[0]["slice_id"])
        self.assertEqual("post_publication_action", dispositions[0]["kind"])
        # 发布时的依据仍可从发布事件中读取
        publications = [p for s in train["slices"] for p in s["publications"]]
        self.assertEqual(1, len(publications))

        # 履行处置义务
        self.svc.fulfill_disposition(
            disposition_id=dispositions[0]["disposition_id"],
            fulfilled_by="legal-2", note="已下线",
        )
        self.assertEqual([], self.svc.list_open_dispositions())

    def test_withdraw_is_idempotent(self) -> None:
        sid, _ = self._approved_slice()
        self.svc.withdraw_right(right_id="r1", effective_at=dt(2026, 10, 1),
                                reason="x")
        again = self.svc.withdraw_right(right_id="r1", effective_at=dt(2026, 10, 1),
                                        reason="x")
        self.assertEqual("already_withdrawn", again["status"])

    # -- 到期作业 -------------------------------------------------------------

    def test_right_expiry_job_deactivates_right(self) -> None:
        sid, _ = self._approved_slice(not_after=dt(2026, 10, 1))
        # 到期作业存在
        with self.store.ro() as conn:
            row = conn.execute("SELECT status FROM jobs WHERE job_id = ?",
                               ("expire-r1",)).fetchone()
            self.assertIsNotNone(row)
        # 直接把到期时间提前到过去并运行
        with self.store.tx() as conn:
            conn.execute("UPDATE jobs SET due_at = ? WHERE job_id = ?",
                         ("2020-01-01T00:00:00+00:00", "expire-r1"))
        self.run_jobs()
        q = self.svc.slice_clearance(slice_id=sid, date="2026-10-02",
                                     territory="CN", language="zh", product="train")
        self.assertIn("no_covering_right", q["blockers"])

    def test_expiry_sweep_expires_due_rights_and_reschedules(self) -> None:
        sid, _ = self._approved_slice(not_after=dt(2026, 9, 15))
        # sweep 初始到期（schedule_expiry_sweep 登记为立即到期）
        processed = self.run_jobs()
        self.assertGreaterEqual(processed, 1)
        q = self.svc.slice_clearance(slice_id=sid, date="2026-10-02",
                                     territory="CN", language="zh", product="train")
        self.assertIn("no_covering_right", q["blockers"])
        # 下一轮 sweep 已挂起
        with self.store.ro() as conn:
            row = conn.execute("SELECT status FROM jobs WHERE job_id = ?",
                               (SWEEP_JOB_ID,)).fetchone()
            self.assertEqual("pending", row["status"])

    # -- 血缘反查与重建 --------------------------------------------------------

    def test_dataset_manifest_traces_origin_and_restrictions(self) -> None:
        sid, decision = self._approved_slice()
        self.svc.register_dataset(dataset_id="ds1", name="数据集", product="train",
                                  slice_ids=[sid])
        manifest = self.svc.dataset_manifest("ds1")
        self.assertEqual(1, manifest["slice_count"])
        item = manifest["slices"][0]
        self.assertEqual("a1", item["source_asset"]["asset_id"])
        self.assertEqual("hnr", item["source_asset"]["collector_id"])
        self.assertEqual({"desensitization", "fact"}, set(item["reviews"]))
        self.assertEqual("r1", item["rights"][0]["right_id"])
        self.assertEqual(decision["decision_id"], item["approvals"][0]["decision_id"])
        self.assertEqual("officer-zhang", item["approvals"][0]["decider_id"])
        self.assertEqual([], item["pending_dispositions"])

    def test_rebuild_projections_preserves_read_model(self) -> None:
        sid, _ = self._approved_slice()
        self.svc.register_dataset(dataset_id="ds1", name="数据集", product="train",
                                  slice_ids=[sid])
        self.svc.record_publication(slice_id=sid, published_at=dt(2026, 9, 20),
                                    basis={"channel": "x"})
        self.svc.withdraw_right(right_id="r1", effective_at=dt(2026, 10, 1), reason="x")
        self.run_jobs()
        before = self.svc.dataset_manifest("ds1")
        count = self.store.rebuild_projections()
        after = self.svc.dataset_manifest("ds1")
        self.assertGreater(count, 0)
        self.assertEqual(before["frozen"], after["frozen"])
        self.assertEqual(len(before["slices"][0]["publications"]),
                         len(after["slices"][0]["publications"]))
        self.assertEqual(len(before["slices"][0]["approvals"]),
                         len(after["slices"][0]["approvals"]))

    # -- 作业租约恢复（模拟崩溃） ----------------------------------------------

    def test_stale_running_job_is_reclaimed_after_restart(self) -> None:
        sid, _ = self._approved_slice(not_after=dt(2026, 9, 15))
        # 模拟 worker 认领后崩溃：running 且租约过期
        with self.store.tx() as conn:
            conn.execute(
                "UPDATE jobs SET status = 'running', locked_by = 'dead-worker', "
                "locked_until = ?, attempts = 1 WHERE job_id = ?",
                ("2020-01-01T00:00:00+00:00", "expire-r1"),
            )
        # “重启”：全新的服务实例，立即跑批
        restarted = ClearanceService(EventStore(self.db_path))
        n = EventStore(self.db_path).run_due_jobs(
            restarted.job_handlers(), worker_id="new-worker")
        self.assertGreaterEqual(n, 1)
        with self.store.ro() as conn:
            row = conn.execute("SELECT status, locked_by FROM jobs WHERE job_id = ?",
                               ("expire-r1",)).fetchone()
            self.assertEqual("done", row["status"])
            self.assertIsNone(row["locked_by"])


if __name__ == "__main__":
    unittest.main()
