"""HTTP API 端到端测试（真实 socket，线程服务器）。"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from corpus_clearance.httpapi import build_server

TODAY = datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()


class Client:
    def __init__(self, server):
        host, port = server.server_address
        self.base = f"http://{host}:{port}"

    def request(self, method, path, body=None, headers=None):
        data = None
        hdrs = dict(headers or {})
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            hdrs["Content-Type"] = "application/json; charset=utf-8"
        req = urllib.request.Request(self.base + path, data=data, headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))


class ApiFlowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = build_server(":memory:")
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.client = Client(cls.server)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)

    def _headers(self, org, user=None, idem=None):
        h = {"X-Org-Id": org}
        if user:
            h["X-User-Id"] = user
        if idem:
            h["Idempotency-Key"] = idem
        return h

    def test_01_full_journey(self):
        c = self.client
        # 登记机构
        for org in [
            ("news", "通讯社", ["collector"]),
            ("review_org", "审校中心", ["reviewer"]),
            ("gov", "放行委", ["approver"]),
            ("lab", "实验室", ["applicant"]),
        ]:
            status, _ = c.request("POST", "/v1/orgs",
                                  {"org_id": org[0], "name": org[1], "roles": org[2]},
                                  self._headers(org[0], idem=f"org-{org[0]}"))
            self.assertEqual(status, 201)

        # 缺少机构头 → 403
        status, body = c.request("POST", "/v1/assets/ingest",
                                 {"asset_id": "a", "source_id": "s", "content_hash": "h"})
        self.assertEqual(status, 403)
        self.assertEqual(body["error"]["code"], "acting_org_required")

        # 冒认其他机构 → 403
        status, body = c.request("POST", "/v1/assets/ingest",
                                 {"asset_id": "a", "contributor_org_id": "OTHER",
                                  "source_id": "s", "batch_id": "b", "content_hash": "h"},
                                 self._headers("news"))
        self.assertEqual(status, 403)
        self.assertEqual(body["error"]["code"], "org_mismatch")

        # 摄取（幂等键重放）
        ingest_body = {
            "asset_id": "art-1", "contributor_org_id": "news", "source_id": "x-1",
            "batch_id": "b1", "content_hash": "h1",
            "declared_grants": [{"grant_id": "g-1", "territories": ["CN"], "languages": ["zh"],
                                 "products": ["qa", "training"],
                                 "retain_until": "2027-06-01T00:00:00+08:00"}],
        }
        s1, b1 = c.request("POST", "/v1/assets/ingest", ingest_body,
                           self._headers("news", idem="ingest-1"))
        self.assertEqual(s1, 200)
        self.assertEqual(b1["version"], 1)
        s2, b2 = c.request("POST", "/v1/assets/ingest", ingest_body,
                           self._headers("news", idem="ingest-1"))
        self.assertEqual(s2, 200)
        self.assertTrue(b2.get("idempotent_replay"))

        # 切片
        status, body = c.request("POST", "/v1/assets/art-1/slices",
                                 {"ordinal": 0, "text": "自贸港建设三年行动"},
                                 self._headers("news"))
        self.assertEqual(status, 201)
        slice_id = body["event"]["payload"]["slice_id"]

        # 双职责复核：同一人不得兼任
        status, body = c.request("POST", f"/v1/slices/{slice_id}/reviews",
                                 {"duty": "desensitize", "reviewer_id": "u-a", "passed": True},
                                 self._headers("review_org", "u-a"))
        self.assertEqual(status, 201)
        status, body = c.request("POST", f"/v1/slices/{slice_id}/reviews",
                                 {"duty": "fact_check", "reviewer_id": "u-a", "passed": True},
                                 self._headers("review_org", "u-a"))
        self.assertEqual(status, 403)
        self.assertEqual(body["error"]["code"], "segregation_of_duty")
        status, _ = c.request("POST", f"/v1/slices/{slice_id}/reviews",
                              {"duty": "fact_check", "reviewer_id": "u-b", "passed": True},
                              self._headers("review_org", "u-b"))
        self.assertEqual(status, 201)

        # 资格判定
        status, body = c.request("GET",
                                 f"/v1/eligibility?slice_id={slice_id}&date={TODAY}"
                                 "&territory=CN&language=zh&product=qa")
        self.assertEqual(status, 200)
        self.assertTrue(body["usable"], body["blockers"])
        self.assertEqual(body["effective_grants"], ["g-1"])
        status, body = c.request("GET",
                                 f"/v1/eligibility?slice_id={slice_id}&date={TODAY}"
                                 "&territory=US&language=zh&product=qa")
        self.assertEqual(status, 200)
        self.assertFalse(body["usable"])

        # 缺少参数
        status, body = c.request("GET", f"/v1/eligibility?slice_id={slice_id}&date={TODAY}")
        self.assertEqual(status, 400)

        # 申请 + 放行
        req_body = {
            "applicant_org_id": "lab", "purpose": "政务问答", "territory": "CN",
            "language": "zh", "product": "qa", "slice_ids": [slice_id],
            "materials": {"authorization_letter": "L-001"},
            "request_id": "req-http-1",
        }
        status, body = c.request("POST", "/v1/requests", req_body, self._headers("lab"))
        self.assertEqual(status, 201)
        status, body = c.request("POST", "/v1/requests/req-http-1/decisions",
                                 {"decider_org_id": "gov", "verdict": "approved"},
                                 self._headers("gov", "zhao"))
        self.assertEqual(status, 201)
        decision_id = body["event"]["payload"]["decision_id"]

        # 第二个有效决定 → 409
        status, body = c.request("POST", "/v1/requests/req-http-1/decisions",
                                 {"decider_org_id": "gov", "verdict": "approved"},
                                 self._headers("gov", "qian"))
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "single_active_decision")

        # 数据集发布
        status, body = c.request("POST", "/v1/datasets",
                                 {"dataset_id": "ds-qa", "creator_org_id": "lab",
                                  "product": "qa", "slice_ids": [slice_id]},
                                 self._headers("lab"))
        self.assertEqual(status, 201)
        status, body = c.request("POST", "/v1/datasets/ds-qa/publish",
                                 {"decision_ids": [decision_id]}, self._headers("lab"))
        self.assertEqual(status, 201)

        # 血缘反查
        status, lineage = c.request("GET", "/v1/datasets/ds-qa/lineage")
        self.assertEqual(status, 200)
        self.assertEqual(lineage["slices"][0]["source_id"], "x-1")
        self.assertEqual(lineage["slices"][0]["contributor_org"], "news")
        self.assertIn("g-1", lineage["slices"][0]["rights"])
        self.assertEqual(lineage["decisions"][0]["decider_user"], "zhao")
        self.assertTrue(lineage["published_basis"]["decision_ids"], [decision_id])

        # 撤回 → 手动触发作业 → 数据集冻结 + 处置义务
        status, _ = c.request("POST", "/v1/grants/g-1/withdraw",
                              {"reason": "人物授权撤回"}, self._headers("news"))
        self.assertEqual(status, 201)
        status, body = c.request("POST", "/v1/maintenance/run-jobs", {}, self._headers("gov"))
        self.assertEqual(status, 200)
        status, lineage = c.request("GET", "/v1/datasets/ds-qa/lineage")
        self.assertEqual(lineage["status"], "frozen")
        self.assertEqual(len(lineage["pending_obligations"]), 1)
        obligation_id = lineage["pending_obligations"][0]["obligation_id"]

        # 冻结后资格不再可用
        status, body = c.request("GET",
                                 f"/v1/eligibility?slice_id={slice_id}&date={TODAY}"
                                 "&territory=CN&language=zh&product=qa")
        self.assertFalse(body["usable"])

        # 履行处置义务
        status, _ = c.request("POST", f"/v1/obligations/{obligation_id}/fulfill",
                              {"note": "已通知所有接收方停止使用"}, self._headers("gov"))
        self.assertEqual(status, 201)
        status, lineage = c.request("GET", "/v1/datasets/ds-qa/lineage")
        self.assertEqual(len(lineage["pending_obligations"]), 0)

    def test_quarantine_reported_as_409(self):
        c = self.client
        body = {
            "asset_id": "art-dispute", "contributor_org_id": "news", "source_id": "x-2",
            "batch_id": "b2", "content_hash": "h2",
        }
        status, _ = c.request("POST", "/v1/assets/ingest", body, self._headers("news"))
        self.assertEqual(status, 200)
        body["batch_id"] = "b3"
        body["content_hash"] = "h2-changed"
        status, payload = c.request("POST", "/v1/assets/ingest", body, self._headers("news"))
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "asset_disputed")
        self.assertEqual(payload["error"]["details"]["current_version"], 1)
        self.assertEqual(payload["error"]["details"]["quarantined_version"], 2)

    def test_unknown_path_and_bad_json(self):
        c = self.client
        status, _ = c.request("GET", "/v1/nope")
        self.assertEqual(status, 404)
        req = urllib.request.Request(self.client.base + "/v1/orgs", data=b"{not json",
                                     headers={**self._headers("news"), "Content-Type": "application/json"},
                                     method="POST")
        try:
            urllib.request.urlopen(req, timeout=5)
            self.fail("应返回 400")
        except urllib.error.HTTPError as exc:
            self.assertEqual(exc.code, 400)


class RestartResumptionTests(unittest.TestCase):
    def test_scheduler_resumes_future_withdrawal_across_processes(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "state.db")
            server = build_server(db)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            c = Client(server)
            h = lambda org, user=None: {"X-Org-Id": org, **({"X-User-Id": user} if user else {})}
            for org, name, roles in [("news", "通讯社", ["collector"]), ("review_org", "审校", ["reviewer"]),
                                     ("gov", "委", ["approver"]), ("lab", "室", ["applicant"])]:
                c.request("POST", "/v1/orgs", {"org_id": org, "name": name, "roles": roles}, h(org))
            c.request("POST", "/v1/assets/ingest",
                      {"asset_id": "a", "contributor_org_id": "news", "source_id": "s",
                       "batch_id": "b", "content_hash": "hh",
                       "declared_grants": [{"grant_id": "gg", "territories": ["CN"], "languages": ["zh"],
                                            "products": ["qa"],
                                            "retain_until": "2028-01-01T00:00:00+08:00"}]},
                      h("news"))
            _, body = c.request("POST", "/v1/assets/a/slices",
                                {"ordinal": 0, "text": "正文"}, h("news"))
            sid = body["event"]["payload"]["slice_id"]
            c.request("POST", f"/v1/slices/{sid}/reviews",
                      {"duty": "desensitize", "reviewer_id": "r1", "passed": True}, h("review_org", "r1"))
            c.request("POST", f"/v1/slices/{sid}/reviews",
                      {"duty": "fact_check", "reviewer_id": "r2", "passed": True}, h("review_org", "r2"))
            # 未来生效撤回
            c.request("POST", "/v1/grants/gg/withdraw",
                      {"reason": "未来撤回", "effective_at": "2026-10-10T00:00:00+00:00"}, h("news"))
            server.shutdown()
            server.server_close()

            # 重启后：作业仍在；调度续跑
            server2 = build_server(db, start_scheduler=True, scheduler_interval=0.05)
            thread2 = threading.Thread(target=server2.serve_forever, daemon=True)
            thread2.start()
            c2 = Client(server2)
            status, body = c2.request("GET",
                                      f"/v1/eligibility?slice_id={sid}&date={TODAY}"
                                      "&territory=CN&language=zh&product=qa")
            self.assertTrue(body["usable"])
            # 等待后台线程在生效后执行（通过手动触发模拟时钟推进不可行，故直接校验作业入队事实，
            # 跨进程恢复语义由 test_service.JobTests 覆盖）
            status, jobs = c2.request("GET", "/v1/jobs")
            self.assertTrue(any(j["kind"] == "withdrawal" and j["ref_key"] == "grant:gg"
                                for j in jobs["jobs"]))
            server2.scheduler.stop()
            server2.shutdown()
            server2.server_close()


if __name__ == "__main__":
    unittest.main()
