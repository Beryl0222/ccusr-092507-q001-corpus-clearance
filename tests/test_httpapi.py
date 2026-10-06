"""HTTP API 端到端测试（进程内起服务，不起后台线程，作业走维护端点触发）。"""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from corpus_clearance.domain import sha256_hex
from corpus_clearance.httpapi import Application, build_server
from corpus_clearance.services import ClearanceService
from corpus_clearance.store import EventStore


class HttpTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "http.db")
        self.app = Application(EventStore(self.db_path))
        self.httpd, _ = build_server("127.0.0.1", 0, self.app, with_worker=False)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)
        self.tmp.cleanup()

    def call(self, method: str, path: str, body=None, *, actor=None,
             idem=None, expect=None):
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", method=method)
        req.add_header("Content-Type", "application/json")
        if actor:
            req.add_header("X-Actor-Id", actor)
        if idem:
            req.add_header("Idempotency-Key", idem)
        data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
        try:
            with urllib.request.urlopen(req, data) as resp:
                result = resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            result = exc.code, json.loads(exc.read())
        if expect is not None and result[0] != expect:
            raise AssertionError(f"{method} {path}: 期望 {expect}，实际 {result}")
        return result

    def _seed(self) -> tuple[str, str]:
        self.call("POST", "/v1/orgs", {"org_id": "hnr", "name": "报社"}, expect=201)
        self.call("POST", "/v1/assets/ingest", {
            "batch_id": "b", "collector_id": "hnr", "collector_org_id": "hnr",
            "asset_id": "a1", "content_hash": sha256_hex("正文"),
            "authorization_fingerprint": sha256_hex("授权")}, actor="hnr", expect=201)
        self.call("POST", "/v1/assets/a1/rights", {
            "right_id": "r1", "territories": ["CN"], "languages": ["zh"],
            "products": ["train"], "embargo_not_before": "2026-09-01T00:00:00+08:00",
            "not_after": "2027-01-01T00:00:00+08:00"}, actor="hnr", expect=201)
        sid = self.call("POST", "/v1/assets/a1/slices",
                        {"slices": [{"content_hash": sha256_hex("段1")}]},
                        expect=201)[1]["slice_ids"][0]
        self.call("POST", f"/v1/slices/{sid}/reviews",
                  {"review_kind": "desensitization", "passed": True},
                  actor="redactor-a", expect=201)
        self.call("POST", f"/v1/slices/{sid}/reviews",
                  {"review_kind": "fact", "passed": True},
                  actor="fact-b", expect=201)
        req = self.call("POST", "/v1/requests", {
            "purpose": "news", "slice_ids": [sid], "territories": ["CN"],
            "languages": ["zh"], "products": ["train"],
            "materials": {"plan": "v1"}}, actor="team", expect=201)[1]
        decision = self.call(
            "POST", f"/v1/requests/{req['request_id']}/decision",
            {"approved": True}, actor="officer-zhang", expect=201)[1]
        return sid, decision["decision_id"]

    def test_health_and_unknown_route(self) -> None:
        self.assertEqual("ok", self.call("GET", "/health")[1]["status"])
        self.assertEqual(404, self.call("GET", "/nope")[0])

    def test_idempotency_header_replays_first_response(self) -> None:
        self.call("POST", "/v1/orgs", {"org_id": "o1", "name": "机构"}, expect=201)
        body = {"batch_id": "b", "collector_id": "o1", "collector_org_id": "o1",
                "asset_id": "x1", "content_hash": sha256_hex("c")}
        first = self.call("POST", "/v1/assets/ingest", body, idem="k1", expect=201)
        second = self.call("POST", "/v1/assets/ingest", body, idem="k1", expect=201)
        self.assertEqual(first, second)

    def test_clearance_and_manifest_flow(self) -> None:
        sid, decision_id = self._seed()
        self.call("POST", "/v1/datasets", {
            "dataset_id": "ds1", "name": "训练集", "product": "train",
            "slice_ids": [sid]}, actor="data", expect=201)

        ok = self.call(
            "GET", f"/v1/clearance?slice_id={sid}&date=2026-10-07"
                  "&territory=CN&language=zh&product=train")[1]
        self.assertTrue(ok["usable"])
        self.assertEqual("officer-zhang", ok["effective_decision"]["decider_id"])

        bad = self.call(
            "GET", f"/v1/clearance?slice_id={sid}&date=2026-10-07"
                  "&territory=CN&language=en&product=train")[1]
        self.assertFalse(bad["usable"])

        missing = self.call(
            "GET", f"/v1/clearance?slice_id={sid}&date=2026-10-07&territory=CN")[0]
        self.assertEqual(400, missing)

        manifest = self.call("GET", "/v1/datasets/ds1/manifest")[1]
        self.assertEqual("a1", manifest["slices"][0]["source_asset"]["asset_id"])
        self.assertEqual(decision_id, manifest["slices"][0]["approvals"][0]["decision_id"])

    def test_withdraw_propagation_triggered_by_maintenance_endpoint(self) -> None:
        sid, _ = self._seed()
        self.call("POST", "/v1/datasets", {
            "dataset_id": "ds1", "name": "训练集", "product": "train",
            "slice_ids": [sid]}, expect=201)
        self.call("POST", f"/v1/slices/{sid}/publications", {
            "published_at": "2026-10-03T09:00:00+08:00", "basis": {"channel": "终端"}},
            expect=201)
        self.call("POST", "/v1/rights/r1/withdraw", {
            "effective_at": "2026-10-05T00:00:00+08:00", "reason": "授权撤回"},
            actor="legal", expect=202)
        # 作业尚未传播：切片此刻仍可能可用（发布传播未执行）
        result = self.call("POST", "/v1/maintenance/run-jobs", {}, expect=200)[1]
        self.assertGreaterEqual(result["processed"], 1)
        q = self.call(
            "GET", f"/v1/clearance?slice_id={sid}&date=2026-10-07"
                  "&territory=CN&language=zh&product=train")[1]
        self.assertFalse(q["usable"])
        self.assertIn("slice_frozen", q["blockers"])
        self.assertEqual(1, len(q["open_dispositions"]))
        manifest = self.call("GET", "/v1/datasets/ds1/manifest")[1]
        self.assertTrue(manifest["frozen"])

    def test_restart_continues_pending_withdraw_job(self) -> None:
        sid, _ = self._seed()
        self.call("POST", "/v1/rights/r1/withdraw", {
            "effective_at": "2026-10-05T00:00:00+08:00", "reason": "授权撤回"},
            expect=202)
        # 重启：新的 Application 指向同一数据库
        self.httpd.shutdown()
        app2 = Application(EventStore(self.db_path))
        httpd2, _ = build_server("127.0.0.1", 0, app2, with_worker=False)
        port2 = httpd2.server_address[1]
        thread2 = threading.Thread(target=httpd2.serve_forever, daemon=True)
        thread2.start()
        try:
            req = urllib.request.Request(
                f"http://127.0.0.1:{port2}/v1/maintenance/run-jobs",
                data=b"{}", method="POST")
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req) as resp:
                payload = json.loads(resp.read())
            self.assertGreaterEqual(payload["processed"], 1)
            qreq = urllib.request.Request(
                f"http://127.0.0.1:{port2}/v1/clearance?slice_id={sid}"
                "&date=2026-10-07&territory=CN&language=zh&product=train")
            with urllib.request.urlopen(qreq) as resp:
                clearance = json.loads(resp.read())
            self.assertIn("slice_frozen", clearance["blockers"])
        finally:
            httpd2.shutdown()
            httpd2.server_close()
            thread2.join(timeout=5)

    def test_events_of_aggregate_are_listable(self) -> None:
        sid, _ = self._seed()
        events = self.call("GET", "/v1/aggregates/corpus_slice/" + sid + "/events",
                           expect=200)[1]["events"]
        kinds = [e["event_type"] for e in events]
        self.assertEqual("SLICE_CUT", kinds[0])
        self.assertIn("SLICE_REVIEWED", kinds)
        # 版本从 1 连续递增
        self.assertEqual(list(range(1, len(kinds) + 1)), [e["version"] for e in events])


if __name__ == "__main__":
    unittest.main()
