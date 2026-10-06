"""HTTP API：基于标准库的无依赖实现。

启动时打开 SQLite 事件库并启动后台 worker 线程；worker 周期执行到期扫描与
撤回传播作业，认领带租约，因此服务重启后会继续未完成的作业。

写接口支持 ``Idempotency-Key`` 请求头：同一键的重复提交直接回放首次响应。
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .domain import DomainError
from .services import ClearanceService
from .store import EventStore

DEFAULT_DB = os.environ.get("CORPUS_DB", "data/clearance.db")
WORKER_INTERVAL = float(os.environ.get("CORPUS_WORKER_INTERVAL", "5"))


# ---------------------------------------------------------------------------
# HTTP 层幂等重放（独立于业务自然幂等，保证客户端安全重试）
# ---------------------------------------------------------------------------


def ensure_idempotency_table(store: EventStore) -> None:
    with store.tx() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS http_idempotency ("
            "idempotency_key TEXT PRIMARY KEY, response_status INTEGER NOT NULL, "
            "response_body TEXT NOT NULL, created_at TEXT NOT NULL)"
        )


# ---------------------------------------------------------------------------
# 应用装配
# ---------------------------------------------------------------------------


class Application:
    def __init__(self, store: EventStore) -> None:
        self.store = store
        store.init()
        ensure_idempotency_table(store)
        self.service = ClearanceService(store)
        # 重启后立即恢复周期到期扫描；未完成的 pending/running 作业由 worker 认领
        self.service.schedule_expiry_sweep()

    # -- 作业触发 -------------------------------------------------------------

    def run_jobs_once(self, worker_id: str | None = None) -> int:
        return self.store.run_due_jobs(
            self.service.job_handlers(),
            worker_id=worker_id or f"manual-{uuid.uuid4().hex[:8]}",
        )


# ---------------------------------------------------------------------------
# 请求处理
# ---------------------------------------------------------------------------


class _Handler(BaseHTTPRequestHandler):
    app: Application  # 由工厂注入到类属性

    server_version = "CorpusClearance/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:  # 安静日志
        if os.environ.get("CORPUS_HTTP_LOG"):
            super().log_message(fmt, *args)

    # -- 基础工具 --------------------------------------------------------------

    def _send_json(self, status: int, body: Any) -> None:
        raw = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        try:
            body = json.loads(self.rfile.read(length).decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise DomainError(f"请求体不是合法 JSON: {exc}") from exc
        if not isinstance(body, dict):
            raise DomainError("请求体必须是 JSON 对象")
        return body

    def _actor(self, body: dict) -> str | None:
        return self.headers.get("X-Actor-Id") or body.pop("_actor_id", None)

    # -- 路由 ------------------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        try:
            self._route_get()
        except DomainError as exc:
            self._send_json(exc.http_status, exc.to_dict())
        except Exception as exc:  # noqa: BLE001
            self._send_json(500, {"code": "internal_error", "message": str(exc)})

    def do_POST(self) -> None:  # noqa: N802
        idem_key = self.headers.get("Idempotency-Key")
        if idem_key:
            replayed = self._replay_idempotent(idem_key)
            if replayed is not None:
                self._send_json(replayed[0], replayed[1])
                return
        try:
            status, body = self._route_post()
            if idem_key:
                self._store_idempotent(idem_key, status, body)
            self._send_json(status, body)
        except DomainError as exc:
            self._send_json(exc.http_status, exc.to_dict())
        except (KeyError, TypeError) as exc:
            self._send_json(422, {"code": "validation_failed",
                                  "message": f"请求字段缺失或类型不符: {exc}"})
        except Exception as exc:  # noqa: BLE001
            self._send_json(500, {"code": "internal_error", "message": str(exc)})

    def _replay_idempotent(self, key: str) -> tuple[int, Any] | None:
        with self.app.store.ro() as conn:
            row = conn.execute(
                "SELECT response_status, response_body FROM http_idempotency "
                "WHERE idempotency_key = ?", (key,)
            ).fetchone()
        if row is None:
            return None
        return row["response_status"], json.loads(row["response_body"])

    def _store_idempotent(self, key: str, status: int, body: Any) -> None:
        from datetime import datetime, timezone

        with self.app.store.tx() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO http_idempotency(idempotency_key, response_status, "
                "response_body, created_at) VALUES (?, ?, ?, ?)",
                (key, status, json.dumps(body, ensure_ascii=False),
                 datetime.now(timezone.utc).isoformat()),
            )

    # -- GET 路由 ---------------------------------------------------------------

    def _route_get(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        qs = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        svc = self.app.service

        if path == "/health":
            self._send_json(200, {"status": "ok"})
        elif path == "/v1/assets":
            self._send_json(200, {"assets": svc.list_assets()})
        elif path == "/v1/requests":
            self._send_json(200, {"requests": svc.list_requests()})
        elif path == "/v1/datasets":
            self._send_json(200, {"datasets": svc.list_datasets()})
        elif path == "/v1/dispositions":
            self._send_json(200, {"open_dispositions": svc.list_open_dispositions()})
        elif path == "/v1/jobs":
            self._send_json(200, {"jobs": svc.job_stats()})
        elif path == "/v1/clearance":
            required = ("slice_id", "date", "territory", "language", "product")
            missing = [f for f in required if not qs.get(f)]
            if missing:
                raise DomainError(f"缺少查询参数: {', '.join(missing)}")
            result = svc.slice_clearance(
                slice_id=qs["slice_id"], date=qs["date"], territory=qs["territory"],
                language=qs["language"], product=qs["product"], purpose=qs.get("purpose"),
            )
            self._send_json(200, result)
        elif path.startswith("/v1/datasets/") and path.endswith("/manifest"):
            dataset_id = path[len("/v1/datasets/"):-len("/manifest")]
            self._send_json(200, svc.dataset_manifest(dataset_id))
        elif path.startswith("/v1/assets/") and path.endswith("/trace"):
            asset_id = path[len("/v1/assets/"):-len("/trace")]
            self._send_json(200, svc.trace_asset(asset_id))
        elif path.startswith("/v1/aggregates/"):
            rest = path[len("/v1/aggregates/"):].split("/")
            if len(rest) != 3 or rest[2] != "events":
                raise DomainError("路径应为 /v1/aggregates/{type}/{id}/events", )
            agg_type, agg_id = rest[0], rest[1]
            self._send_json(200, {"events": svc.store.events_for(agg_type, agg_id)})
        else:
            self._send_json(404, {"code": "not_found", "message": f"无此路径: {path}"})

    # -- POST 路由 --------------------------------------------------------------

    def _route_post(self) -> tuple[int, Any]:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        body = self._read_json()
        svc = self.app.service
        actor = self._actor(body)

        if path == "/v1/orgs":
            return 201, svc.register_org(body["org_id"], body.get("name"))

        if path == "/v1/assets/ingest":
            result = svc.ingest_asset(
                batch_id=body["batch_id"], collector_id=body["collector_id"],
                content_hash=body["content_hash"],
                collector_org_id=body["collector_org_id"],
                asset_id=body.get("asset_id"), title=body.get("title"),
                authorization_fingerprint=body.get("authorization_fingerprint"),
            )
            # 新建 201；安全重传/争议隔离是既有事实的回应，200
            return (201 if result["status"] == "ingested" else 200), result

        if path.startswith("/v1/assets/") and path.endswith("/rights"):
            asset_id = path[len("/v1/assets/"):-len("/rights")]
            return 201, svc.declare_right(
                right_id=body["right_id"], asset_id=asset_id,
                declarer_id=body.get("declarer_id", actor),
                territories=body["territories"], languages=body["languages"],
                licensor_id=body.get("licensor_id"), products=body.get("products"),
                purpose_tags=body.get("purpose_tags"), slice_ids=body.get("slice_ids"),
                embargo_not_before=body.get("embargo_not_before"),
                not_after=body.get("not_after"), basis_type=body.get("basis_type", "declared"),
                notes=body.get("notes"),
            )

        if path.startswith("/v1/assets/") and path.endswith("/slices"):
            asset_id = path[len("/v1/assets/"):-len("/slices")]
            ids = svc.cut_slices(asset_id, body["slices"], actor_id=actor)
            return 201, {"slice_ids": ids}

        if path.startswith("/v1/slices/") and path.endswith("/reviews"):
            slice_id = path[len("/v1/slices/"):-len("/reviews")]
            return 201, svc.review_slice(
                slice_id=slice_id, review_kind=body["review_kind"],
                reviewer_id=body.get("reviewer_id", actor),
                passed=body["passed"], notes=body.get("notes"),
            )

        if path == "/v1/requests":
            result = svc.create_request(
                applicant_id=body.get("applicant_id", actor), purpose=body["purpose"],
                slice_ids=body["slice_ids"], territories=body["territories"],
                languages=body["languages"], products=body["products"],
                materials=body["materials"], request_id=body.get("request_id"),
            )
            return 201, result

        if path.startswith("/v1/requests/") and path.endswith("/supersede"):
            request_id = path[len("/v1/requests/"):-len("/supersede")]
            return 200, svc.supersede_request(
                old_request_id=request_id,
                new_request_id=body.get("new_request_id"),
                applicant_id=body.get("applicant_id", actor),
                materials=body["materials"], purpose=body.get("purpose"),
                slice_ids=body.get("slice_ids"), territories=body.get("territories"),
                languages=body.get("languages"), products=body.get("products"),
            )

        if path.startswith("/v1/requests/") and path.endswith("/decision"):
            request_id = path[len("/v1/requests/"):-len("/decision")]
            return 201, svc.decide(
                request_id=request_id, decider_id=body.get("decider_id", actor),
                approved=body["approved"], reason=body.get("reason"),
            )

        if path.startswith("/v1/rights/") and path.endswith("/withdraw"):
            right_id = path[len("/v1/rights/"):-len("/withdraw")]
            return 202, svc.withdraw_right(
                right_id=right_id, effective_at=body["effective_at"],
                reason=body["reason"], actor_id=actor,
            )

        if path == "/v1/datasets":
            return 201, svc.register_dataset(
                dataset_id=body["dataset_id"], name=body["name"], product=body["product"],
                slice_ids=body["slice_ids"],
                registered_by=body.get("registered_by", actor),
            )

        if path.startswith("/v1/datasets/") and path.endswith("/derivations"):
            parent = path[len("/v1/datasets/"):-len("/derivations")]
            return 201, svc.record_derivation(
                parent_dataset_id=parent, child_dataset_id=body["child_dataset_id"],
                derived_by=actor,
            )

        if path.startswith("/v1/slices/") and path.endswith("/publications"):
            slice_id = path[len("/v1/slices/"):-len("/publications")]
            return 201, svc.record_publication(
                slice_id=slice_id, published_at=body["published_at"], basis=body["basis"],
                publication_id=body.get("publication_id"),
            )

        if path.startswith("/v1/dispositions/") and path.endswith("/fulfill"):
            disposition_id = path[len("/v1/dispositions/"):-len("/fulfill")]
            return 200, svc.fulfill_disposition(
                disposition_id=disposition_id,
                fulfilled_by=body.get("fulfilled_by", actor), note=body.get("note"),
            )

        if path == "/v1/maintenance/run-jobs":
            count = self.app.run_jobs_once(worker_id=actor)
            return 200, {"processed": count, "jobs": svc.job_stats()}

        if path == "/v1/maintenance/rebuild-projections":
            return 200, {"replayed_events": self.app.store.rebuild_projections()}

        return 404, {"code": "not_found", "message": f"无此路径: {path}"}


# ---------------------------------------------------------------------------
# 后台 worker：周期处理到期作业
# ---------------------------------------------------------------------------


class JobWorker:
    def __init__(self, app: Application, interval: float = WORKER_INTERVAL,
                 worker_id: str | None = None) -> None:
        self.app = app
        self.interval = interval
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name="job-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.app.run_jobs_once(worker_id=self.worker_id)
            except Exception:  # noqa: BLE001 - worker 不能因单次异常退出
                pass
            self._stop.wait(self.interval)


# ---------------------------------------------------------------------------
# 启动
# ---------------------------------------------------------------------------


def build_server(host: str, port: int, app: Application, *, with_worker: bool = True
                 ) -> tuple[ThreadingHTTPServer, JobWorker | None]:
    handler = type("Handler", (_Handler,), {"app": app})
    httpd = ThreadingHTTPServer((host, port), handler)
    worker = JobWorker(app) if with_worker else None
    if worker is not None:
        worker.start()
    return httpd, worker


def serve(host: str = "127.0.0.1", port: int = 8080, db_path: str = DEFAULT_DB,
          *, with_worker: bool = True) -> None:
    app = Application(EventStore(db_path))
    httpd, worker = build_server(host, port, app, with_worker=with_worker)
    try:
        httpd.serve_forever()
    finally:
        if worker is not None:
            worker.stop()
        httpd.server_close()


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="语料用途放行 HTTP 服务")
    parser.add_argument("--host", default=os.environ.get("CORPUS_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("CORPUS_PORT", "8080")))
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--no-worker", action="store_true", help="不在本进程运行后台作业线程")
    args = parser.parse_args()
    serve(args.host, args.port, args.db, with_worker=not args.no_worker)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
