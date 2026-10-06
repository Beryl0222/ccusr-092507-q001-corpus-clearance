"""领域错误：带稳定错误码与 HTTP 状态，消息使用中文。"""

from __future__ import annotations


class DomainError(Exception):
    http_status = 400
    code = "domain_error"

    def __init__(self, message: str, code: str | None = None, details: dict | None = None):
        super().__init__(message)
        self.message = message
        if code:
            self.code = code
        self.details = details or {}


class ValidationError(DomainError):
    http_status = 400
    code = "invalid_request"


class NotFoundError(DomainError):
    http_status = 404
    code = "not_found"


class ConflictError(DomainError):
    http_status = 409
    code = "conflict"


class AuthorizationError(DomainError):
    http_status = 403
    code = "forbidden"


class QuarantineConflict(ConflictError):
    """编号相同而正文或授权不一致：隔离争议，不覆盖现行版本。"""

    http_status = 409
    code = "asset_disputed"
