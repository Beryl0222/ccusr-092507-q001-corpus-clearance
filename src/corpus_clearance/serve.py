"""启动 HTTP 服务：python -m corpus_clearance.serve --db corpus.db --port 8080。"""

from __future__ import annotations

import argparse
import logging

from .httpapi import build_server


def main() -> int:
    parser = argparse.ArgumentParser(description="自贸港语料用途放行服务")
    parser.add_argument("--db", default="corpus_clearance.db", help="SQLite 数据库路径（默认 corpus_clearance.db）")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--no-scheduler", action="store_true", help="不启动后台到期扫描线程（仅手动触发）")
    parser.add_argument("--interval", type=float, default=1.0, help="后台扫描间隔秒数")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")

    httpd = build_server(
        args.db,
        host=args.host,
        port=args.port,
        start_scheduler=not args.no_scheduler,
        scheduler_interval=args.interval,
    )
    logging.getLogger("corpus_clearance").info(
        "语料用途放行服务已启动: http://%s:%s （数据库 %s）", args.host, args.port, args.db
    )
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if httpd.scheduler:
            httpd.scheduler.stop()
        httpd.shutdown()
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
