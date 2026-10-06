"""HTTP API（标准库实现，无第三方依赖）。

写接口要求请求头 ``X-Org-Id`` 标明执行机构，人员用 ``X-User-Id``；
机构只能以自己的名义归集、授权、复核、申请或放行，请求体中的机构号
必须与请求头一致，跨机构冒认返回 403。

所有写接口支持 ``Idempotency-Key``：相同键的重放返回首次响应。
"""

from __future__ import annotations

import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from .errors import DomainError
from .jobs import BackgroundScheduler, JobRunner
from .projector import project
from .service import ClearanceService
from .store import EventStore

log = logging.getLogger("corpus_clearance.api")


class ApiContext:
    def __init__(self, service: ClearanceService, runner: JobRunner):
        self.service = service
        self.runner = runner


def _json_default(value):
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


class ClearanceHandler(BaseHTTPRequestHandler):
    server_version = "CorpusClearance/1.0"

    # ── 基础收发 ───────────────────────────────────────────────
    def log_message(self, fmt, *args):  # 静默默认访问日志，统一走 logging
        log.debug("%s - %s", self.address_string(), fmt % args)

    def _send_json(self, status: int, body: dict) -> None:
        raw = json.dumps(body, ensure_ascii=False, default=_json_default).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)
        # 写命令的 2xx 成功结果按 Idempotency-Key 留存（错误不缓存，便于更正后重试）
        key = getattr(self, "_idem_key", None)
        if key and self.command == "POST" and 200 <= status < 300:
            self.server.ctx.service.store.put_idempotent(key, status, body)
            self._idem_key = None

    def _error(self, exc: Exception) -> None:
        if isinstance(exc, DomainError):
            self._send_json(exc.http_status, {"error": {"code": exc.code, "message": exc.message,
                                                        "details": exc.details}})
        elif isinstance(exc, (ValueError, KeyError, TypeError)):
            self._send_json(400, {"error": {"code": "invalid_request", "message": str(exc)}})
        else:
            log.exception("未处理异常")
            self._send_json(500, {"error": {"code": "internal_error", "message": "服务内部错误"}})

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        try:
            body = json.loads(self.rfile.read(length).decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"请求体不是合法 JSON：{exc}") from exc
        if not isinstance(body, dict):
            raise ValueError("请求体必须是 JSON 对象")
        return body

    def _acting_org(self, body: dict, *fields: str) -> str:
        org = self.headers.get("X-Org-Id", "").strip()
        if not org:
            from .errors import AuthorizationError

            raise AuthorizationError("缺少 X-Org-Id 请求头", "acting_org_required")
        for field in fields:
            if field in body and body[field] != org:
                from .errors import AuthorizationError

                raise AuthorizationError(
                    f"请求头机构 {org} 不得冒用其他机构名义（{field}={body[field]}）", "org_mismatch"
                )
        return org

    def _idempotent(self, path: str, body: dict) -> bool:
        """返回 False 表示命中缓存已直接回包；True 表示继续正常处理。"""
        self._idem_key = None
        key = self.headers.get("Idempotency-Key")
        if not key:
            return True
        store = self.server.ctx.service.store
        full_key = f"{self.command} {path} {key}"
        row = store.get_idempotent(full_key)
        if row is not None:
            cached = json.loads(row["body"])
            cached["idempotent_replay"] = True
            self._send_json(row["status_code"], cached)
            return False
        self._idem_key = full_key
        return True

    # ── 路由 ───────────────────────────────────────────────────
    def do_GET(self) -> None:
        try:
            parts = urlsplit(self.path)
            path = parts.path.rstrip("/") or "/"
            q = {k: v[0] for k, v in parse_qs(parts.query).items()}
            self._route_get(path, q)
        except Exception as exc:  # noqa: BLE001 - 统一错误出口
            self._error(exc)

    def do_POST(self) -> None:
        try:
            parts = urlsplit(self.path)
            path = parts.path.rstrip("/") or "/"
            body = self._read_json()
            self._route_post(path, body)
        except Exception as exc:  # noqa: BLE001
            self._error(exc)

    # ── GET ────────────────────────────────────────────────────
    def _route_get(self, path: str, q: dict) -> None:
        svc = self.server.ctx.service
        if path == "/healthz":
            self._send_json(200, {"status": "ok"})
        elif path == "/v1/eligibility":
            required = ("slice_id", "date", "territory", "language", "product")
            missing = [f for f in required if not q.get(f)]
            if missing:
                raise ValueError(f"缺少查询参数: {missing}")
            self._send_json(200, svc.eligibility(q["slice_id"], q["date"], q["territory"],
                                                 q["language"], q["product"]))
        elif path == "/v1/assets":
            self._send_json(200, {"assets": svc.list_assets()})
        elif path.startswith("/v1/assets/"):
            asset_id = path.split("/")[3]
            self._send_json(200, svc.asset_detail(asset_id))
        elif path.startswith("/v1/slices/"):
            slice_id = path.split("/")[3]
            svc._get_slice(slice_id)
            rows = svc._rows(
                "SELECT g.* FROM p_slice_grants sg JOIN p_grants g ON g.grant_id=sg.grant_id WHERE sg.slice_id=?",
                (slice_id,),
            )
            self._send_json(200, {
                "slice": {k: svc._row("SELECT * FROM p_slices WHERE slice_id=?", (slice_id,))[k]
                          for k in ("slice_id", "asset_id", "asset_version", "ordinal", "status")},
                "grants": [dict(r) for r in rows],
                "reviews": [dict(r) for r in svc._rows(
                    "SELECT duty,reviewer_id,passed,note FROM p_reviews WHERE slice_id=?", (slice_id,))],
                "processing": svc._processing_records(slice_id),
            })
        elif path.startswith("/v1/grants/"):
            grant_id = path.split("/")[3]
            row = svc._row("SELECT * FROM p_grants WHERE grant_id=?", (grant_id,))
            if row is None:
                from .errors import NotFoundError

                raise NotFoundError(f"权利依据 {grant_id} 不存在", "grant_not_found")
            self._send_json(200, dict(row) | {"territories": json.loads(row["territories"]),
                                              "languages": json.loads(row["languages"]),
                                              "products": json.loads(row["products"])})
        elif path == "/v1/datasets":
            self._send_json(200, {"datasets": svc.list_datasets()})
        elif path.endswith("/lineage") and path.startswith("/v1/datasets/"):
            dataset_id = path.split("/")[3]
            self._send_json(200, svc.dataset_lineage(dataset_id))
        elif path.startswith("/v1/datasets/"):
            dataset_id = path.split("/")[3]
            self._send_json(200, svc.dataset_lineage(dataset_id))
        elif path == "/v1/requests":
            self._send_json(200, {"requests": svc.list_requests()})
        elif path == "/v1/obligations":
            self._send_json(200, {"obligations": svc.list_obligations(q.get("status"))})
        elif path == "/v1/jobs":
            self._send_json(200, {"jobs": [dict(r) for r in svc.store.list_jobs()]})
        else:
            self._send_json(404, {"error": {"code": "not_found", "message": f"未知路径 {path}"}})

    # ── POST ───────────────────────────────────────────────────
    def _route_post(self, path: str, body: dict) -> None:
        svc = self.server.ctx.service
        segments = [s for s in path.split("/") if s]

        if not self._idempotent(path, body):
            return

        if path == "/v1/orgs":
            org = self._acting_org(body, "org_id")
            event = svc.register_org(body["org_id"], body["name"], body.get("roles", []))
            self._send_json(201, {"event": _event_view(event)})
            return

        if path == "/v1/assets/ingest":
            org = self._acting_org(body, "contributor_org_id")
            result = svc.ingest_asset(
                asset_id=body["asset_id"],
                contributor_org_id=org,
                source_id=body["source_id"],
                batch_id=body.get("batch_id", "default"),
                content_hash=body["content_hash"],
                body_ref=body.get("body_ref"),
                body_text=body.get("body_text"),
                declared_grants=body.get("declared_grants", []),
                flags=body.get("flags", {}),
                embargo_until=body.get("embargo_until"),
            )
            self._send_json(200, result)
            return

        if len(segments) == 5 and segments[:2] == ["v1", "assets"] and segments[3] == "disputes" \
                and segments[4] == "resolve":
            org = self._acting_org(body, "resolver_org_id")
            result = svc.resolve_dispute(
                asset_id=segments[2], version=int(body["version"]),
                resolution=body["resolution"], resolver_org_id=org, note=body.get("note"),
            )
            self._send_json(201, {"event": _event_view(result),
                                  "resolved_grants": result.get("resolved_grants", [])})
            return

        if len(segments) == 4 and segments[:2] == ["v1", "assets"] and segments[3] == "slices":
            org = self._acting_org(body)
            result = svc.cut_slice(
                slice_id=body.get("slice_id"),
                asset_id=segments[2],
                ordinal=int(body.get("ordinal", 0)),
                text=body["text"],
                flags=body.get("flags"),
                grant_ids=body.get("grant_ids"),
            )
            self._send_json(201, {"event": _event_view(result)})
            return

        if len(segments) == 4 and segments[:2] == ["v1", "slices"] and segments[3] == "reviews":
            org = self._acting_org(body, "reviewer_org_id")
            user = self.headers.get("X-User-Id", "").strip()
            if not user:
                from .errors import AuthorizationError

                raise AuthorizationError("缺少 X-User-Id 请求头", "user_required")
            result = svc.review_slice(
                slice_id=segments[2],
                duty=body["duty"],
                reviewer_org_id=org,
                reviewer_id=body.get("reviewer_id", user),
                passed=bool(body["passed"]),
                note=body.get("note"),
            )
            self._send_json(201, {"event": _event_view(result)})
            return

        if path == "/v1/grants":
            org = self._acting_org(body, "granter_org_id")
            result = svc.record_grant(
                asset_id=body["asset_id"],
                asset_version=int(body["asset_version"]),
                granter_org_id=org,
                territories=body["territories"],
                languages=body["languages"],
                products=body["products"],
                retain_until=body["retain_until"],
                valid_from=body.get("valid_from"),
                grant_id=body.get("grant_id"),
            )
            status = 200 if result.get("deduplicated") else 201
            self._send_json(status, result if result.get("deduplicated") else {"event": _event_view(result)})
            return

        if len(segments) == 4 and segments[:2] == ["v1", "grants"] and segments[3] == "withdraw":
            org = self._acting_org(body)
            result = svc.withdraw_grant(segments[2], body["reason"], body.get("effective_at"),
                                        acting_org_id=org)
            self._send_json(201, {"event": _event_view(result)})
            return

        if path == "/v1/requests":
            org = self._acting_org(body, "applicant_org_id")
            result = svc.submit_request(
                applicant_org_id=org,
                purpose=body["purpose"],
                territory=body["territory"],
                language=body["language"],
                product=body["product"],
                slice_ids=body["slice_ids"],
                materials=body["materials"],
                request_id=body.get("request_id"),
            )
            status = 200 if result.get("deduplicated") else 201
            self._send_json(status, result if result.get("deduplicated") else {"event": _event_view(result)})
            return

        if len(segments) == 4 and segments[:2] == ["v1", "requests"] and segments[3] == "decisions":
            org = self._acting_org(body, "decider_org_id")
            user = self.headers.get("X-User-Id", "").strip()
            if not user:
                from .errors import AuthorizationError

                raise AuthorizationError("缺少 X-User-Id 请求头", "user_required")
            result = svc.decide_request(
                request_id=segments[2],
                decider_org_id=org,
                decider_user=body.get("decider_user", user),
                verdict=body["verdict"],
                reason=body.get("reason"),
            )
            self._send_json(201, {"event": _event_view(result)})
            return

        if path == "/v1/datasets":
            org = self._acting_org(body, "creator_org_id")
            result = svc.create_dataset(
                dataset_id=body.get("dataset_id"),
                creator_org_id=org,
                product=body["product"],
                slice_ids=body["slice_ids"],
            )
            self._send_json(201, {"event": _event_view(result)})
            return

        if len(segments) == 4 and segments[:2] == ["v1", "datasets"] and segments[3] == "publish":
            self._acting_org(body)
            result = svc.publish_dataset(segments[2], body["decision_ids"])
            self._send_json(201, {"event": _event_view(result)})
            return

        if len(segments) == 4 and segments[:2] == ["v1", "obligations"] and segments[3] == "fulfill":
            self._acting_org(body)
            result = svc.fulfill_obligation(segments[2], body.get("note"))
            self._send_json(201, {"event": _event_view(result)})
            return

        if path == "/v1/maintenance/run-jobs":
            self._acting_org(body)
            counts = self.server.ctx.runner.run_due()
            self._send_json(200, {"ran": counts})
            return

        self._send_json(404, {"error": {"code": "not_found", "message": f"未知路径 {path}"}})


def _event_view(event: dict) -> dict:
    return {k: event[k] for k in ("event_id", "event_type", "aggregate_type", "aggregate_id",
                                  "version", "occurred_at", "payload") if k in event}


def build_server(db_path: str = ":memory:", *, host: str = "127.0.0.1", port: int = 0,
                 start_scheduler: bool = False, scheduler_interval: float = 1.0) -> ThreadingHTTPServer:
    store = EventStore(db_path, projector=project)
    store.rebuild_projections()
    service = ClearanceService(store)
    runner = JobRunner(service)
    httpd = ThreadingHTTPServer((host, port), ClearanceHandler)
    httpd.ctx = ApiContext(service, runner)
    httpd.store = store
    # 启动时登记缺失的到期作业并继续未完成的传播
    service.schedule_grant_expiries()
    runner.run_due()
    httpd.scheduler = None
    if start_scheduler:
        httpd.scheduler = BackgroundScheduler(runner, interval_seconds=scheduler_interval)
        httpd.scheduler.start()
    return httpd
