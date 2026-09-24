"""领域事件交换契约校验。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping


@dataclass(frozen=True)
class ContractIssue:
    field: str
    code: str
    message: str


def _timezone_is_explicit(value: str) -> bool:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def validate_event(payload: Any, schema: Mapping[str, Any]) -> list[ContractIssue]:
    """返回稳定排序的问题列表，且不修改输入。"""
    if not isinstance(payload, Mapping):
        return [ContractIssue("$", "object_required", "事件必须是 JSON 对象")]

    issues: list[ContractIssue] = []
    for field in schema.get("required", []):
        if field not in payload:
            issues.append(ContractIssue(str(field), "required", "缺少必填字段"))

    for field in ("event_id", "event_type", "aggregate_type", "aggregate_id"):
        if field in payload and (not isinstance(payload[field], str) or not payload[field].strip()):
            issues.append(ContractIssue(field, "non_empty_string", "字段必须是非空字符串"))

    version = payload.get("version")
    if "version" in payload and (isinstance(version, bool) or not isinstance(version, int) or version < 1):
        issues.append(ContractIssue("version", "positive_integer", "版本必须是正整数"))

    occurred_at = payload.get("occurred_at")
    if "occurred_at" in payload and (not isinstance(occurred_at, str) or not _timezone_is_explicit(occurred_at)):
        issues.append(ContractIssue("occurred_at", "timezone_required", "发生时间必须包含时区"))

    properties = schema.get("properties", {})
    for field in ("event_type", "aggregate_type"):
        allowed = properties.get(field, {}).get("enum", [])
        value = payload.get(field)
        if isinstance(value, str) and allowed and value not in allowed:
            issues.append(ContractIssue(field, "unsupported_value", "字段值未在契约中登记"))

    event_type = payload.get("event_type")
    event_payload = payload.get("payload")
    if "payload" in payload and not isinstance(event_payload, Mapping):
        issues.append(ContractIssue("payload", "object_required", "事件载荷必须是 JSON 对象"))
    elif isinstance(event_type, str) and isinstance(event_payload, Mapping):
        required = schema.get("payload_required_by_event", {}).get(event_type, [])
        for field in required:
            if field not in event_payload:
                issues.append(ContractIssue(f"payload.{field}", "required", "事件载荷缺少必填字段"))

    return sorted(issues, key=lambda issue: (issue.field, issue.code))
