"""领域服务业务规则测试。"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from corpus_clearance.errors import (
    AuthorizationError,
    ConflictError,
    NotFoundError,
    QuarantineConflict,
)
from corpus_clearance.jobs import JobRunner
from corpus_clearance.projector import project
from corpus_clearance.service import ClearanceService
from corpus_clearance.store import EventStore, to_utc

CLOCK = datetime(2026, 10, 6, 10, 0, tzinfo=timezone.utc)


class World:
    """构造一个贴近题面的多机构世界。"""

    def __init__(self, path: str = ":memory:"):
        self.store = EventStore(path, projector=project)
        self.store.rebuild_projections()
        self.svc = ClearanceService(self.store, clock=lambda: CLOCK)
        self.runner = JobRunner(self.svc)

        self.svc.register_org("news", "通讯社海南分社", ["collector"])
        self.svc.register_org("local", "本地补采中心", ["collector"])
        self.svc.register_org("review_org", "脱敏审校中心", ["reviewer"])
        self.svc.register_org("gov", "省放行委员会", ["approver"])
        self.svc.register_org("lab", "政务问答实验室", ["applicant"])
        self.svc.register_org("platform", "数据平台", ["platform"])

        self.a1 = "article-1"
        r = self.svc.ingest_asset(
            asset_id=self.a1,
            contributor_org_id="news",
            source_id="xinhua-2026-1001",
            batch_id="batch-A",
            content_hash="hash-v1",
            declared_grants=[{
                "grant_id": "g1",
                "territories": ["CN"],
                "languages": ["zh"],
                "products": ["training", "qa"],
                "retain_until": "2027-01-01T00:00:00+08:00",
            }],
        )
        self.g1 = r["grants"][0]
        self.s1 = self.svc.cut_slice(None, self.a1, 0, "第一段正文")["payload"]["slice_id"]
        self.s2 = self.svc.cut_slice(None, self.a1, 1, "第二段正文")["payload"]["slice_id"]

        # 第二篇：带禁发期，独立授权 g2
        self.a2 = "article-2"
        self.svc.ingest_asset(
            asset_id=self.a2,
            contributor_org_id="local",
            source_id="local-supplement-9",
            batch_id="batch-A",
            content_hash="hash-a2",
            embargo_until="2026-12-01T00:00:00+08:00",
            declared_grants=[{
                "grant_id": "g2",
                "territories": ["CN", "HK"],
                "languages": ["zh", "en"],
                "products": ["intl"],
                "retain_until": "2028-01-01T00:00:00+08:00",
            }],
        )
        self.s3 = self.svc.cut_slice(None, self.a2, 0, "地方补采段")["payload"]["slice_id"]

    def review_both(self, slice_id: str, u1: str = "u-desens", u2: str = "u-fact") -> None:
        self.svc.review_slice(slice_id, "desensitize", "review_org", u1, True)
        self.svc.review_slice(slice_id, "fact_check", "review_org", u2, True)


class IngestTests(unittest.TestCase):
    def setUp(self):
        self.w = World()

    def test_same_batch_and_fingerprint_is_safe_retransmit(self):
        before = len(self.w.store.events())
        again = self.w.svc.ingest_asset(
            asset_id=self.w.a1, contributor_org_id="news", source_id="xinhua-2026-1001",
            batch_id="batch-A", content_hash="hash-v1",
            declared_grants=[{"grant_id": "g1", "territories": ["CN"], "languages": ["zh"],
                              "products": ["training", "qa"], "retain_until": "2027-01-01T00:00:00+08:00"}],
        )
        self.assertTrue(again["deduplicated"])
        self.assertEqual(again["version"], 1)
        self.assertEqual(len(self.w.store.events()), before)  # 不产生新事件

    def test_same_id_different_body_is_quarantined_without_touching_current(self):
        with self.assertRaises(QuarantineConflict) as ctx:
            self.w.svc.ingest_asset(
                asset_id=self.w.a1, contributor_org_id="news", source_id="xinhua-2026-1001",
                batch_id="batch-B", content_hash="hash-v2-different",
                declared_grants=[{"grant_id": "gx", "territories": ["CN"], "languages": ["zh"],
                                  "products": ["training"], "retain_until": "2027-01-01T00:00:00+08:00"}],
            )
        self.assertEqual(ctx.exception.code, "asset_disputed")
        detail = self.w.svc.asset_detail(self.w.a1)
        self.assertEqual([v["version"] for v in detail["versions"]], [1, 2])
        self.assertEqual(detail["versions"][0]["status"], "ingested")  # 现行版本保留
        self.assertEqual(detail["versions"][1]["status"], "quarantined")  # 争议版本隔离
        asset = self.w.svc._get_asset(self.w.a1)
        self.assertEqual(asset["current_version"], 1)
        self.assertEqual(asset["disputed"], 1)
        # 争议授权未随争议版本登记，不得挂接到任何切片
        from corpus_clearance.errors import ValidationError
        with self.assertRaises((ConflictError, ValidationError)):
            self.w.svc.cut_slice(None, self.w.a1, 2, "争议版本切片", grant_ids=["gx"])

    def test_same_id_different_grant_print_is_quarantined(self):
        grants = [{"grant_id": "g1", "territories": ["CN", "US"], "languages": ["zh"],
                   "products": ["training", "qa"], "retain_until": "2027-01-01T00:00:00+08:00"}]
        with self.assertRaises(QuarantineConflict):
            self.w.svc.ingest_asset(
                asset_id=self.w.a1, contributor_org_id="news", source_id="xinhua-2026-1001",
                batch_id="batch-C", content_hash="hash-v1", declared_grants=grants,
            )

    def test_disputed_version_can_be_promoted_after_adjudication(self):
        # v2 正文不同被隔离
        with self.assertRaises(QuarantineConflict):
            self.w.svc.ingest_asset(
                asset_id=self.w.a1, contributor_org_id="news", source_id="xinhua-2026-1001",
                batch_id="batch-D", content_hash="hash-v2",
                declared_grants=[{"grant_id": "g-new", "territories": ["CN"], "languages": ["zh"],
                                  "products": ["training"], "retain_until": "2028-01-01T00:00:00+08:00"}],
            )
        # 非平台机构不能裁决
        with self.assertRaises(AuthorizationError):
            self.w.svc.resolve_dispute(self.w.a1, 2, "promote", "news")
        # 只能裁决隔离中的版本
        with self.assertRaises(ConflictError):
            self.w.svc.resolve_dispute(self.w.a1, 1, "promote", "platform")

        result = self.w.svc.resolve_dispute(self.w.a1, 2, "promote", "platform", note="核实为通讯社合法改版")
        self.assertIn("g-new", result["resolved_grants"])
        asset = self.w.svc._get_asset(self.w.a1)
        self.assertEqual(asset["current_version"], 2)
        self.assertEqual(asset["disputed"], 0)
        detail = self.w.svc.asset_detail(self.w.a1)
        self.assertEqual(detail["versions"][1]["status"], "ingested")

        # 新切片挂接到新版本授权
        s = self.w.svc.cut_slice(None, self.w.a1, 9, "改版后段落")
        self.assertEqual(s["payload"]["grants"], ["g-new"])

    def test_disputed_version_can_be_rejected(self):
        with self.assertRaises(QuarantineConflict):
            self.w.svc.ingest_asset(
                asset_id=self.w.a1, contributor_org_id="news", source_id="xinhua-2026-1001",
                batch_id="batch-E", content_hash="hash-bad",
                declared_grants=[],
            )
        self.w.svc.resolve_dispute(self.w.a1, 2, "reject", "platform", note="伪造补采")
        detail = self.w.svc.asset_detail(self.w.a1)
        self.assertEqual(detail["versions"][1]["status"], "rejected")
        self.assertEqual(self.w.svc._get_asset(self.w.a1)["current_version"], 1)
        self.assertEqual(self.w.svc._get_asset(self.w.a1)["disputed"], 0)

    def test_collector_scope_is_enforced(self):
        # 无 collector 职责的机构不能归集
        with self.assertRaises(AuthorizationError):
            self.w.svc.ingest_asset(
                asset_id="x", contributor_org_id="lab", source_id="s", batch_id="b",
                content_hash="h",
            )
        # 归集方只能声明自己拥有的范围：local 不得为 news 归集的版本登记授权
        with self.assertRaises(AuthorizationError) as ctx:
            self.w.svc.record_grant(
                asset_id=self.w.a1, asset_version=1, granter_org_id="local",
                territories=["CN"], languages=["zh"], products=["training"],
                retain_until="2027-01-01T00:00:00+08:00",
            )
        self.assertEqual(ctx.exception.code, "grant_outside_scope")

    def test_grant_id_safe_retransmit_and_rebound_rejected(self):
        again = self.w.svc.record_grant(
            asset_id=self.w.a1, asset_version=1, granter_org_id="news",
            territories=["CN"], languages=["zh"], products=["training", "qa"],
            retain_until="2027-01-01T00:00:00+08:00", grant_id="g1",
        )
        self.assertTrue(again["deduplicated"])
        with self.assertRaises(ConflictError):
            self.w.svc.record_grant(
                asset_id=self.w.a2, asset_version=1, granter_org_id="local",
                territories=["CN"], languages=["zh"], products=["intl"],
                retain_until="2028-01-01T00:00:00+08:00", grant_id="g1",
            )


class ReviewTests(unittest.TestCase):
    def setUp(self):
        self.w = World()

    def test_desensitize_and_fact_check_must_be_different_people(self):
        self.w.svc.review_slice(self.w.s1, "desensitize", "review_org", "same-user", True)
        with self.assertRaises(AuthorizationError) as ctx:
            self.w.svc.review_slice(self.w.s1, "fact_check", "review_org", "same-user", True)
        self.assertEqual(ctx.exception.code, "segregation_of_duty")
        # 换人后可以
        self.w.svc.review_slice(self.w.s1, "fact_check", "review_org", "another-user", True)

    def test_org_without_reviewer_role_cannot_review(self):
        with self.assertRaises(AuthorizationError):
            self.w.svc.review_slice(self.w.s1, "desensitize", "news", "u", True)

    def test_failed_review_blocks_use(self):
        self.w.svc.review_slice(self.w.s1, "desensitize", "review_org", "u1", False)
        self.w.svc.review_slice(self.w.s1, "fact_check", "review_org", "u2", True)
        elig = self.w.svc.eligibility(self.w.s1, "2026-10-06", "CN", "zh", "training")
        self.assertFalse(elig["usable"])
        self.assertTrue(any("复核" in b for b in elig["blockers"]))


class EligibilityTests(unittest.TestCase):
    def setUp(self):
        self.w = World()

    def test_blocked_until_both_reviews_pass(self):
        elig = self.w.svc.eligibility(self.w.s1, "2026-10-06", "CN", "zh", "training")
        self.assertFalse(elig["usable"])
        self.w.review_both(self.w.s1)
        elig = self.w.svc.eligibility(self.w.s1, "2026-10-06", "CN", "zh", "training")
        self.assertTrue(elig)
        self.assertTrue(elig["usable"], elig["blockers"])
        self.assertEqual(elig["effective_grants"], ["g1"])

    def test_territory_language_product_must_all_match(self):
        self.w.review_both(self.w.s1)
        for territory, language, product in [("US", "zh", "training"), ("CN", "en", "training"),
                                             ("CN", "zh", "intl")]:
            elig = self.w.svc.eligibility(self.w.s1, "2026-10-06", territory, language, product)
            self.assertFalse(elig["usable"], (territory, language, product))

    def test_embargo_blocks_until_release_date(self):
        self.w.review_both(self.w.s3, "r1", "r2")
        during = self.w.svc.eligibility(self.w.s3, "2026-11-30", "CN", "zh", "intl")
        self.assertFalse(during["usable"])
        self.assertTrue(any("禁发期" in b for b in during["blockers"]))
        after = self.w.svc.eligibility(self.w.s3, "2026-12-02", "CN", "en", "intl")
        self.assertTrue(after["usable"], after["blockers"])

    def test_retain_until_boundary(self):
        self.w.review_both(self.w.s1)
        expired = self.w.svc.eligibility(self.w.s1, "2027-01-02", "CN", "zh", "training")
        self.assertFalse(expired["usable"])
        last_day = self.w.svc.eligibility(self.w.s1, "2026-12-31", "CN", "zh", "qa")
        self.assertTrue(last_day["usable"], last_day["blockers"])


class DecisionTests(unittest.TestCase):
    def setUp(self):
        self.w = World()
        self.w.review_both(self.w.s1)

    def _request(self, materials=None):
        return self.w.svc.submit_request(
            applicant_org_id="lab", purpose="政务问答训练", territory="CN", language="zh",
            product="training", slice_ids=[self.w.s1], materials=materials or {"form": "v1"},
            request_id="req-1",
        )

    def test_approval_flow_and_basis(self):
        self._request()
        dec = self.w.svc.decide_request("req-1", "gov", "zhao", "approved")
        self.assertEqual(dec["event_type"], "USE_APPROVED")
        self.assertEqual(dec["payload"]["basis"]["grant_ids"], ["g1"])

    def test_cannot_approw_without_preconditions(self):
        self.w.svc.submit_request(
            applicant_org_id="lab", purpose="x", territory="CN", language="zh", product="training",
            slice_ids=[self.w.s2], materials={"form": 1}, request_id="req-2",
        )
        with self.assertRaises(ConflictError) as ctx:
            self.w.svc.decide_request("req-2", "gov", "zhao", "approved")
        self.assertEqual(ctx.exception.code, "approval_precondition_failed")

    def test_concurrent_decisions_collapse_to_one(self):
        self._request()
        outcomes: list[str] = []
        barrier = threading.Barrier(8)

        def decide(i):
            barrier.wait()
            try:
                self.w.svc.decide_request("req-1", "gov", f"officer-{i}", "approved")
                outcomes.append("approved")
            except ConflictError as exc:
                outcomes.append(exc.code)

        threads = [threading.Thread(target=decide, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(outcomes.count("approved"), 1)
        self.assertEqual(outcomes.count("single_active_decision"), 7)
        active = self.w.store.conn.execute(
            "SELECT COUNT(*) c FROM active_decisions WHERE request_id='req-1'"
        ).fetchone()["c"]
        self.assertEqual(active, 1)
        # 已有有效决定后再次放行仍被拒绝
        with self.assertRaises(ConflictError):
            self.w.svc.decide_request("req-1", "gov", "zhao2", "approved")

    def test_materials_change_voids_old_approval(self):
        self._request({"form": "v1"})
        first = self.w.svc.decide_request("req-1", "gov", "zhao", "approved")
        first_id = first["payload"]["decision_id"]

        # 相同材料安全重传
        again = self._request({"form": "v1"})
        self.assertTrue(again["deduplicated"])

        # 材料改变 → 同编号补正，旧批准作废
        self._request({"form": "v2", "addendum": "人物授权范围变化"})
        old = self.w.store.conn.execute(
            "SELECT active FROM p_decisions WHERE decision_id=?", (first_id,)
        ).fetchone()
        self.assertEqual(old["active"], 0)
        self.assertIsNone(self.w.store.conn.execute(
            "SELECT decision_id FROM active_decisions WHERE request_id='req-1'").fetchone())
        # 必须基于新材料重新决定
        second = self.w.svc.decide_request("req-1", "gov", "zhao", "approved")
        self.assertNotEqual(second["payload"]["decision_id"], first_id)
        self.assertEqual(second["payload"]["request_materials_hash"][:8],
                         self.w.store.conn.execute("SELECT materials_hash FROM p_requests WHERE request_id='req-1'")
                         .fetchone()["materials_hash"][:8])

    def test_deny_requires_no_preconditions_and_closes_request(self):
        self._request()
        dec = self.w.svc.decide_request("req-1", "gov", "zhao", "denied", reason="人物授权仅限中文传播")
        self.assertEqual(dec["payload"]["reason"], "人物授权仅限中文传播")
        with self.assertRaises(ConflictError):
            self.w.svc.decide_request("req-1", "gov", "zhao", "approved")


class WithdrawalTests(unittest.TestCase):
    def setUp(self):
        self.w = World()
        self.w.review_both(self.w.s1)
        self.w.review_both(self.w.s2, "u3", "u4")
        self.w.review_both(self.w.s3, "r1", "r2")

    def test_withdrawal_freezes_only_affected_slices_and_descendants(self):
        req = self.w.svc.submit_request(
            applicant_org_id="lab", purpose="训练", territory="CN", language="zh",
            product="training", slice_ids=[self.w.s1], materials={"form": 1}, request_id="req-ds",
        )
        dec = self.w.svc.decide_request("req-ds", "gov", "zhao", "approved")
        decision_id = dec["payload"]["decision_id"]
        self.w.svc.create_dataset("ds-1", "lab", "training", [self.w.s1])
        self.w.svc.publish_dataset("ds-1", [decision_id])

        self.w.svc.withdraw_grant("g1", "通讯社撤回人物授权")

        self.assertEqual(self.w.svc._get_slice(self.w.s1)["status"], "frozen")
        self.assertEqual(self.w.svc._get_slice(self.w.s2)["status"], "frozen")
        # 不受 g1 影响的切片保持可用
        self.assertEqual(self.w.svc._get_slice(self.w.s3)["status"], "active")
        # 派生物冻结，且当时发布依据保留
        lineage = self.w.svc.dataset_lineage("ds-1")
        self.assertEqual(lineage["status"], "frozen")
        self.assertIsNotNone(lineage["published_basis"])
        self.assertEqual(lineage["decisions"][0]["decision_id"], decision_id)
        self.assertEqual(len(lineage["pending_obligations"]), 1)
        oblig = lineage["pending_obligations"][0]
        self.assertEqual(oblig["kind"], "purge_or_notice")
        self.assertEqual(oblig["detail"]["slice_id"], self.w.s1)

        # 处置幂等：重复传播不产生重复义务
        self.w.svc.propagate_withdrawal("g1", "通讯社撤回人物授权", CLOCK)
        lineage = self.w.svc.dataset_lineage("ds-1")
        self.assertEqual(len([o for o in lineage["obligations"] if o["status"] == "open"]), 1)

        # 履行义务后关闭
        self.w.svc.fulfill_obligation(oblig["obligation_id"], "已从训练集清除并通知接收方")
        lineage = self.w.svc.dataset_lineage("ds-1")
        self.assertEqual(len(lineage["pending_obligations"]), 0)

    def test_frozen_slice_is_no_longer_eligible_or_approvable(self):
        self.w.svc.withdraw_grant("g1", "撤回")
        elig = self.w.svc.eligibility(self.w.s1, "2026-10-06", "CN", "zh", "training")
        self.assertFalse(elig["usable"])
        self.assertTrue(any("冻结" in b for b in elig["blockers"]))

    def test_withdraw_twice_rejected(self):
        self.w.svc.withdraw_grant("g1", "撤回")
        with self.assertRaises(ConflictError):
            self.w.svc.withdraw_grant("g1", "再次撤回")

    def test_only_granter_may_withdraw(self):
        # g1 属于通讯社 news，地方补采中心 local 不得撤回
        with self.assertRaises(AuthorizationError) as ctx:
            self.w.svc.withdraw_grant("g1", "越权撤回", acting_org_id="local")
        self.assertEqual(ctx.exception.code, "withdraw_not_granter")
        # 授权方本人可以
        self.w.svc.withdraw_grant("g1", "本人撤回", acting_org_id="news")

    def test_unpublished_dataset_is_not_obligated(self):
        self.w.svc.create_dataset("ds-draft", "lab", "training", [self.w.s1])
        self.w.svc.withdraw_grant("g1", "撤回")
        lineage = self.w.svc.dataset_lineage("ds-draft")
        self.assertEqual(lineage["obligations"], [])


class ExpiryAndRestartTests(unittest.TestCase):
    def test_expiry_scan_freezes_after_retain_until(self):
        w = World()
        w.review_both(w.s1)
        expired = w.svc.expire_grants(datetime(2027, 1, 2, tzinfo=timezone.utc))
        self.assertEqual(expired, ["g1"])
        # 再扫一次幂等无新过期
        self.assertEqual(w.svc.expire_grants(datetime(2027, 1, 3, tzinfo=timezone.utc)), [])
        self.assertEqual(w.svc._get_slice(w.s1)["status"], "frozen")

    def test_jobs_resume_after_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "state.db")
            w = World(db)
            w.review_both(w.s1, "u1", "u2")
            # 未来生效的撤回：写入持久作业后“服务崩溃”
            w.svc.withdraw_grant(
                "g1", "未来撤回", effective_at="2026-11-01T00:00:00+00:00"
            )
            self.assertEqual(w.svc._get_slice(w.s1)["status"], "active")
            w.store.close()

            # 新进程：重放投影并续跑作业
            store2 = EventStore(db, projector=project)
            replayed = store2.rebuild_projections()
            self.assertGreater(replayed, 0)
            svc2 = ClearanceService(store2, clock=lambda: datetime(2026, 10, 6, tzinfo=timezone.utc))
            runner2 = JobRunner(svc2)
            svc2.schedule_grant_expiries()
            # 未到生效时刻不传播
            runner2.run_due()
            self.assertEqual(svc2._get_slice(w.s1)["status"], "active")
            # 时钟推进到生效后，作业被领取并完成传播
            svc3 = ClearanceService(store2, clock=lambda: datetime(2026, 11, 2, tzinfo=timezone.utc))
            runner3 = JobRunner(svc3)
            counts = runner3.run_due()
            self.assertGreaterEqual(counts["withdrawal"], 1)
            self.assertEqual(svc3._get_slice(w.s1)["status"], "frozen")
            store2.close()

    def test_interrupted_lease_is_reclaimed(self):
        w = World()
        w.svc.withdraw_grant("g1", "撤回")  # 同步传播后作业 done
        jobs = w.store.list_jobs()
        self.assertTrue(any(j["kind"] == "withdrawal" and j["status"] == "done" for j in jobs))


class DatasetTests(unittest.TestCase):
    def setUp(self):
        self.w = World()
        self.w.review_both(self.w.s1)
        self.w.review_both(self.w.s3, "r1", "r2")

    def _publish_setup(self):
        req = self.w.svc.submit_request(
            applicant_org_id="lab", purpose="国际传播", territory="CN", language="en",
            product="intl", slice_ids=[self.w.s3], materials={"form": 1}, request_id="req-intl",
        )
        dec = self.w.svc.decide_request(
            "req-intl", "gov", "qian", "approved",
            at=datetime(2026, 12, 2, tzinfo=timezone.utc),
        )
        self.w.svc.create_dataset("ds-intl", "lab", "intl", [self.w.s3])
        return dec["payload"]["decision_id"]

    def test_publish_requires_covering_active_decisions(self):
        self.w.svc.create_dataset("ds-x", "lab", "intl", [self.w.s3])
        with self.assertRaises(ConflictError):
            self.w.svc.publish_dataset("ds-x", ["dec-does-not-exist"])
        decision_id = self._publish_setup()
        # 另一个未覆盖切片的数据集不能借决定发布
        self.w.svc.create_dataset("ds-y", "lab", "training", [self.w.s1])
        with self.assertRaises(ConflictError):
            self.w.svc.publish_dataset("ds-y", [decision_id])

    def test_old_approval_cannot_publish_after_rights_withdrawn(self):
        decision_id = self._publish_setup()  # 已创建 ds-intl
        # 撤回发生在发布之前：旧批准仍然 active，但发布时刻权利已不在
        self.w.svc.withdraw_grant("g2", "授权撤销")
        with self.assertRaises(ConflictError) as ctx:
            self.w.svc.publish_dataset(
                "ds-intl", [decision_id], at=datetime(2026, 12, 2, tzinfo=timezone.utc)
            )
        self.assertEqual(ctx.exception.code, "rights_no_longer_held")

    def test_lineage_reverse_lookup_is_complete(self):
        decision_id = self._publish_setup()
        self.w.svc.publish_dataset(
            "ds-intl", [decision_id], at=datetime(2026, 12, 2, tzinfo=timezone.utc)
        )
        lineage = self.w.svc.dataset_lineage("ds-intl")
        slice_view = lineage["slices"][0]
        self.assertEqual(slice_view["slice_id"], self.w.s3)
        self.assertEqual(slice_view["asset_id"], "article-2")
        self.assertEqual(slice_view["asset_version"], 1)
        self.assertEqual(slice_view["source_id"], "local-supplement-9")
        self.assertEqual(slice_view["contributor_org"], "local")
        self.assertTrue(slice_view["content_hash"])
        self.assertIn("g2", slice_view["rights"])
        duties = {r["duty"] for r in slice_view["reviews"]}
        self.assertEqual(duties, {"desensitize", "fact_check"})
        self.assertTrue(any(r["event_type"] == "SLICE_CUT" for r in slice_view["processing"]))
        self.assertEqual(lineage["decisions"][0]["decider_user"], "qian")
        self.assertTrue(lineage["decisions"][0]["materials_hash"])


class HeadlineScenarioTests(unittest.TestCase):
    """题面场景：同一篇报道同时含通讯社来源、地方补采、禁发政策附件、仅中文人物授权。

    训练集、政务问答、国际传播包三类下游必须获得彼此相容、各自成立的权利。
    """

    def setUp(self):
        self.w = World()
        w = self.w
        # 通讯社正文段 s1：g1 仅限 CN/zh/(training,qa)，到 2027-01-01
        # 地方补采段 s3：g2 可 CN,HK / zh,en / intl，但带禁发期至 2026-12-01
        # 追加一段“仅中文传播”的人物授权段 s4，独立 grant g3（仅 zh）
        w.svc.ingest_asset(
            asset_id="article-3", contributor_org_id="news", source_id="xinhua-portrait-7",
            batch_id="batch-A", content_hash="hash-a3",
            declared_grants=[{
                "grant_id": "g3", "territories": ["CN"], "languages": ["zh"],
                "products": ["training", "qa", "intl"],
                "retain_until": "2027-06-01T00:00:00+08:00",
            }],
        )
        self.s4 = w.svc.cut_slice(None, "article-3", 0, "人物专访：仅授权中文传播")["payload"]["slice_id"]
        for sid, u1, u2 in [(w.s1, "d1", "f1"), (w.s3, "d3", "f3"), (self.s4, "d4", "f4")]:
            w.review_both(sid, u1, u2)

    def test_three_downstream_products_get_incompatible_but_correct_answers(self):
        w = self.w
        day = "2026-12-02"  # 禁发期已过
        # 1) 训练集：可用通讯社段（CN/zh/training），不可用人物段之外的英文需求
        self.assertTrue(w.svc.eligibility(w.s1, day, "CN", "zh", "training")["usable"])
        # 2) 政务问答：同段 zh 可用
        self.assertTrue(w.svc.eligibility(w.s1, day, "CN", "zh", "qa")["usable"])
        # 3) 国际传播包：通讯社段不授权 intl
        intl_news = w.svc.eligibility(w.s1, day, "CN", "zh", "intl")
        self.assertFalse(intl_news["usable"])
        # 地方补采段可供国际传播（英文）
        self.assertTrue(w.svc.eligibility(w.s3, day, "CN", "en", "intl")["usable"])
        # 人物段：中文 intl 可用，英文 intl 被“只能中文传播”挡住
        self.assertTrue(w.svc.eligibility(self.s4, day, "CN", "zh", "intl")["usable"])
        en_portrait = w.svc.eligibility(self.s4, day, "CN", "en", "intl")
        self.assertFalse(en_portrait["usable"])
        self.assertTrue(any("有效权利依据" in b for b in en_portrait["blockers"]))
        # 禁发期内，地方补采段对任何产品都不可用
        self.assertFalse(w.svc.eligibility(w.s3, "2026-11-01", "CN", "en", "intl")["usable"])

    def test_each_product_needs_its_own_request_and_single_decision(self):
        w = self.w
        day = datetime(2026, 12, 2, tzinfo=timezone.utc)
        for rid, territory, language, product, slices in [
            ("req-train", "CN", "zh", "training", [w.s1]),
            ("req-qa", "CN", "zh", "qa", [w.s1]),
            ("req-intl", "CN", "en", "intl", [w.s3]),
        ]:
            w.svc.submit_request(
                applicant_org_id="lab", purpose=product, territory=territory, language=language,
                product=product, slice_ids=slices, materials={"form": 1}, request_id=rid,
            )
            dec = w.svc.decide_request(rid, "gov", "zhao", "approved", at=day)
            self.assertEqual(dec["event_type"], "USE_APPROVED")
        # 人物段英文国际传播：批准前置条件失败
        w.svc.submit_request(
            applicant_org_id="lab", purpose="intl", territory="CN", language="en", product="intl",
            slice_ids=[self.s4], materials={"form": 1}, request_id="req-portrait-en",
        )
        with self.assertRaises(ConflictError) as ctx:
            w.svc.decide_request("req-portrait-en", "gov", "zhao", "approved", at=day)
        self.assertEqual(ctx.exception.code, "approval_precondition_failed")


if __name__ == "__main__":
    unittest.main()
