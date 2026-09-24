"""从命令行校验一个领域事件。"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from .contracts import validate_event


def main() -> int:
    if len(sys.argv) != 3:
        print("用法: python -m corpus_clearance.cli <schema.json> <event.json>", file=sys.stderr)
        return 2
    schema = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    event = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
    issues = validate_event(event, schema)
    if not issues:
        print("valid")
        return 0
    for issue in issues:
        print(f"{issue.field}	{issue.code}	{issue.message}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
