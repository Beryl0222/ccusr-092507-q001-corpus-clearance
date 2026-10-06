"""领域内核：枚举、错误、时间与标识工具。

本模块不含任何存储或框架依赖，所有业务规则中可复用的常量与判定集中于此，
服务层（``services``）与 HTTP 层共享同一套语义。
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from enum import StrEnum


# ---------------------------------------------------------------------------
# 事件与聚合目录（须与 contracts/domain.schema.json 保持一致）
# ---------------------------------------------------------------------------


class EventType(StrEnum):
    ORG_REGISTERED = "ORG_REGISTERED"
    ASSET_INGESTED = "ASSET_INGESTED"
    ASSET_DISPUTED = "ASSET_DISPUTED"
    SLICE_CUT = "SLICE_CUT"
    SLICE_REVIEWED = "SLICE_REVIEWED"
    RIGHT_DECLARED = "RIGHT_DECLARED"
    USE_REQUESTED = "USE_REQUESTED"
    REVIEW_OPENED = "REVIEW_OPENED"
    USE_APPROVED = "USE_APPROVED"
    USE_REJECTED = "USE_REJECTED"
    REQUEST_SUPERSEDED = "REQUEST_SUPERSEDED"
    RIGHT_WITHDRAWN = "RIGHT_WITHDRAWN"
    RIGHT_EXPIRED = "RIGHT_EXPIRED"
    SLICE_FROZEN = "SLICE_FROZEN"
    DATASET_REGISTERED = "DATASET_REGISTERED"
    DATASET_DERIVATION_RECORDED = "DATASET_DERIVATION_RECORDED"
    DATASET_FROZEN = "DATASET_FROZEN"
    PUBLICATION_RECORDED = "PUBLICATION_RECORDED"
    DISPOSITION_RAISED = "DISPOSITION_RAISED"
    DISPOSITION_FULFILLED = "DISPOSITION_FULFILLED"
    DECISION_EFFECTIVENESS_CHANGED = "DECISION_EFFECTIVENESS_CHANGED"


class AggregateType(StrEnum):
    ORGANIZATION = "organization"
    SOURCE_ASSET = "source_asset"
    CORPUS_SLICE = "corpus_slice"
    RIGHT_BASIS = "right_basis"
    USE_REQUEST = "use_request"
    RELEASE_DECISION = "release_decision"
    DERIVED_DATASET = "derived_dataset"
    DISPOSITION = "disposition"


class ReviewKind(StrEnum):
    """审校职责类型。脱敏与事实审校必须由不同人完成。"""

    DESENSITIZATION = "desensitization"  # 脱敏复核
    FACT = "fact"                        # 事实审校


class AssetStatus(StrEnum):
    ACTIVE = "active"
    DISPUTED = "disputed"
    QUARANTINED = "quarantined"


class RequestStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    SUPERSEDED = "superseded"  # 申请人材料改变后旧申请作废，不得沿用


class DatasetStatus(StrEnum):
    ACTIVE = "active"
    FROZEN = "frozen"  # 其任一组成切片被撤回传播时冻结


# ---------------------------------------------------------------------------
# 错误
# ---------------------------------------------------------------------------


class DomainError(Exception):
    """所有可预期业务拒绝的基类。"""

    code = "domain_error"
    http_status = 400

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict:
        body = {"code": self.code, "message": self.message}
        if self.details:
            body["details"] = self.details
        return body


class NotFoundError(DomainError):
    code = "not_found"
    http_status = 404


class ConflictError(DomainError):
    code = "conflict"
    http_status = 409


class ValidationFailure(DomainError):
    code = "validation_failed"
    http_status = 422


class AuthorizationError(DomainError):
    code = "forbidden"
    http_status = 403


# ---------------------------------------------------------------------------
# 时间 / 哈希 / 标识
# ---------------------------------------------------------------------------

_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.\-:]{1,128}$")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: str | datetime, field: str = "时间") -> datetime:
    """把输入解析为带时区的时间；裸时间视为错误（契约要求显式时区）。"""
    if isinstance(value, datetime):
        dt = value
    else:
        if not isinstance(value, str):
            raise ValidationFailure(f"{field}必须是 ISO 8601 字符串")
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValidationFailure(f"{field}不是合法的 ISO 8601 时间: {value}") from exc
    if dt.tzinfo is None:
        raise ValidationFailure(f"{field}必须显式携带时区")
    return dt


def require_id(value: str, field: str) -> str:
    if not isinstance(value, str) or not _ID_PATTERN.match(value):
        raise ValidationFailure(
            f"{field}必须是 1-128 位非空标识（允许字母、数字、_ . - :）",
            details={"field": field, "value": value},
        )
    return value


def require_text(value: str, field: str, *, max_length: int = 500) -> str:
    """非空自由文本（允许中文），用于名称、用途、产品、理由等。"""
    if not isinstance(value, str) or not value.strip():
        raise ValidationFailure(f"{field}必须是非空字符串")
    if len(value) > max_length:
        raise ValidationFailure(f"{field}长度不得超过 {max_length}")
    return value.strip()


def canonical_json(value: object) -> str:
    """稳定序列化：键排序、空白紧凑，供指纹使用。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_hex(value: bytes | str) -> str:
    if isinstance(value, str):
        value = value.encode("utf-8")
    return hashlib.sha256(value).hexdigest()


def materials_fingerprint(materials: object) -> str:
    """申请材料指纹：材料任一字段变化都会得到不同指纹。"""
    return sha256_hex(canonical_json(materials))


def normalize_list(values: object, field: str) -> list[str]:
    if not isinstance(values, list) or not values:
        raise ValidationFailure(f"{field}必须是非空数组")
    result: list[str] = []
    for item in values:
        if not isinstance(item, str) or not item.strip():
            raise ValidationFailure(f"{field}中的每一项都必须是非空字符串")
        result.append(item.strip())
    return result
