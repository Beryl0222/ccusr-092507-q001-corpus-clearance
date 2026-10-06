"""应用服务层：语料用途放行的全部业务规则。

规则索引（对应需求）：

* 归集方只能声明自己拥有的范围——``declare_right`` 校验声明人即归集人；
  授权维度（地域/语言/产品/用途/期限）构成下游使用的硬边界。
* 同批次同内容指纹安全重传；编号相同而正文或授权指纹不同→隔离争议
  （``ingest_asset``）。
* 脱敏与事实审校是两种职责，批准时两类最新记录均须通过且复核人不同。
* 申请材料带指纹；材料改变必须作废旧申请（``supersede_request``），
  旧批准随即失效，不能沿用。
* 同一申请的批准决定由部分唯一索引兜底，并发放行只产生一个有效决定。
* 撤回只冻结受影响切片及其派生数据集；已发布版本保留当时依据并生成处置义务
  （由可续跑的撤回传播作业执行）。
* 授权到期由持久化到期作业/周期扫描处理，服务重启后继续。
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timedelta
from typing import Any, Iterable

from . import jobs as jobs_mod
from . import projections
from .domain import (
    AggregateType,
    AssetStatus,
    AuthorizationError,
    ConflictError,
    EventType,
    NotFoundError,
    ReviewKind,
    ValidationFailure,
    materials_fingerprint,
    normalize_list,
    now_utc,
    parse_dt,
    require_id,
    require_text,
)
from .store import EventStore, append_within_tx

WILDCARD = "*"
SWEEP_JOB_ID = "sweep-right-expiry"
SWEEP_INTERVAL = timedelta(hours=1)


def _event(
    agg_type: AggregateType | str,
    agg_id: str,
    version: int,
    event_type: EventType | str,
    payload: dict[str, Any],
    *,
    occurred_at: datetime | None = None,
    event_id: str | None = None,
) -> dict[str, Any]:
    return {
        "event_id": event_id or f"ev-{uuid.uuid4().hex}",
        "event_type": str(event_type),
        "aggregate_type": str(agg_type),
        "aggregate_id": agg_id,
        "occurred_at": (occurred_at or now_utc()).isoformat(),
        "version": version,
        "payload": payload,
    }


def _next_version(conn: sqlite3.Connection, agg_id: str) -> int:
    row = conn.execute(
        "SELECT version FROM aggregates WHERE aggregate_id = ?", (agg_id,)
    ).fetchone()
    return 1 if row is None else int(row["version"]) + 1


def _jload(raw: str | None, default: Any) -> Any:
    return json.loads(raw) if raw is not None else default


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    return value if isinstance(value, list) else json.loads(value)


class ClearanceService:
    def __init__(self, store: EventStore) -> None:
        self.store = store

    # ======================================================================
    # 机构
    # ======================================================================

    def register_org(self, org_id: str, name: str | None = None) -> dict:
        require_id(org_id, "org_id")
        with self.store.tx() as conn:
            if projections.get_org(conn, org_id) is not None:
                return dict(projections.get_org(conn, org_id))  # 幂等
            event = _event(
                AggregateType.ORGANIZATION, org_id, 1,
                EventType.ORG_REGISTERED, {"org_id": org_id, "name": name or org_id},
            )
            append_within_tx(conn, [event], idempotency_key=f"org:{org_id}")
            return {"org_id": org_id, "name": name or org_id}

    # ======================================================================
    # 来源资产入库：幂等重传 / 争议隔离
    # ======================================================================

    def ingest_asset(
        self,
        *,
        batch_id: str,
        collector_id: str,
        content_hash: str,
        collector_org_id: str,
        asset_id: str | None = None,
        title: str | None = None,
        authorization_fingerprint: str | None = None,
        occurred_at: datetime | None = None,
    ) -> dict:
        """归集一批报道资产。

        返回 ``status``：``ingested`` 新建 / ``retransmitted`` 安全重传 /
        ``disputed`` 编号相同但正文或授权不同（资产被隔离）。
        """
        require_id(batch_id, "batch_id")
        require_id(collector_id, "collector_id")
        require_id(content_hash, "content_hash")
        require_id(collector_org_id, "collector_org_id")
        if asset_id is not None:
            require_id(asset_id, "asset_id")

        with self.store.tx() as conn:
            org = projections.get_org(conn, collector_org_id)
            if org is None:
                raise ValidationFailure(
                    "归集机构尚未登记，不能归集语料",
                    details={"collector_org_id": collector_org_id},
                )

            existing = None
            if asset_id is not None:
                existing = projections.get_asset(conn, asset_id)
            if existing is None:
                existing = conn.execute(
                    "SELECT * FROM p_assets WHERE batch_id = ? AND content_hash = ? "
                    "AND status != 'quarantined' ORDER BY created_at LIMIT 1",
                    (batch_id, content_hash),
                ).fetchone()

            if existing is not None:
                same_auth = (existing["authorization_fingerprint"] or None) == (
                    authorization_fingerprint or None
                )
                if existing["content_hash"] == content_hash and same_auth:
                    return {
                        "status": "retransmitted",
                        "asset_id": existing["asset_id"],
                        "batch_id": batch_id,
                        "content_hash": content_hash,
                    }
                # 编号相同（或同批次同指纹的授权改变）→ 隔离争议
                target_id = existing["asset_id"]
                version = _next_version(conn, target_id)
                event = _event(
                    AggregateType.SOURCE_ASSET, target_id, version,
                    EventType.ASSET_DISPUTED,
                    {
                        "batch_id": batch_id,
                        "content_hash": content_hash,
                        "existing_content_hash": existing["content_hash"],
                        "authorization_fingerprint": authorization_fingerprint,
                        "existing_authorization_fingerprint":
                            existing["authorization_fingerprint"],
                        "reason": "同编号资产正文或授权依据不一致，隔离待裁",
                        "reporter_id": collector_id,
                    },
                    occurred_at=occurred_at,
                )
                append_within_tx(conn, [event])
                return {
                    "status": "disputed",
                    "asset_id": target_id,
                    "batch_id": batch_id,
                    "existing_content_hash": existing["content_hash"],
                    "incoming_content_hash": content_hash,
                    "quarantined": True,
                }

            asset_id = asset_id or f"asset-{uuid.uuid4().hex[:16]}"
            event = _event(
                AggregateType.SOURCE_ASSET, asset_id, 1,
                EventType.ASSET_INGESTED,
                {
                    "batch_id": batch_id,
                    "collector_id": collector_id,
                    "collector_org_id": collector_org_id,
                    "title": title,
                    "content_hash": content_hash,
                    "authorization_fingerprint": authorization_fingerprint,
                },
                occurred_at=occurred_at,
            )
            try:
                append_within_tx(
                    conn, [event], idempotency_key=f"ingest:{batch_id}:{content_hash}"
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError("并发入库冲突，请读取最新状态后重试") from exc
            return {
                "status": "ingested",
                "asset_id": asset_id,
                "batch_id": batch_id,
                "content_hash": content_hash,
            }

    # ======================================================================
    # 权利依据：归集方只能声明自己拥有的范围
    # ======================================================================

    def declare_right(
        self,
        *,
        right_id: str,
        asset_id: str,
        declarer_id: str,
        territories: list[str],
        languages: list[str],
        licensor_id: str | None = None,
        products: list[str] | None = None,
        purpose_tags: list[str] | None = None,
        slice_ids: list[str] | None = None,
        embargo_not_before: datetime | str | None = None,
        not_after: datetime | str | None = None,
        basis_type: str = "declared",
        notes: str | None = None,
    ) -> dict:
        require_id(right_id, "right_id")
        require_id(asset_id, "asset_id")
        require_id(declarer_id, "declarer_id")
        territories = normalize_list(territories, "territories")
        languages = normalize_list(languages, "languages")
        products = normalize_list(products or [WILDCARD], "products")
        purpose_tags = normalize_list(purpose_tags or [WILDCARD], "purpose_tags")

        embargo = parse_dt(embargo_not_before, "embargo_not_before") if embargo_not_before else None
        expiry = parse_dt(not_after, "not_after") if not_after else None
        if embargo and expiry and embargo > expiry:
            raise ValidationFailure("禁发起点不得晚于保留期限终点")

        with self.store.tx() as conn:
            asset = projections.get_asset(conn, asset_id)
            if asset is None:
                raise NotFoundError("资产不存在", details={"asset_id": asset_id})
            if asset["status"] == AssetStatus.QUARANTINED:
                raise ConflictError("资产处于争议隔离状态，不得声明授权")
            if asset["collector_id"] != declarer_id:
                raise AuthorizationError(
                    "归集方只能声明自己归集范围内的权利依据",
                    details={
                        "asset_id": asset_id,
                        "asset_collector_id": asset["collector_id"],
                        "declarer_id": declarer_id,
                    },
                )
            if projections.get_right(conn, right_id) is not None:
                raise ConflictError("权利依据编号已存在", details={"right_id": right_id})

            if slice_ids is not None:
                slice_ids = normalize_list(slice_ids, "slice_ids")
                asset_slice_ids = {
                    row["slice_id"] for row in projections.slices_for_asset(conn, asset_id)
                }
                unknown = sorted(set(slice_ids) - asset_slice_ids)
                if unknown:
                    raise ValidationFailure(
                        "权利依据引用了不属于该资产的切片", details={"unknown_slice_ids": unknown}
                    )

            event = _event(
                AggregateType.RIGHT_BASIS, right_id, 1,
                EventType.RIGHT_DECLARED,
                {
                    "right_id": right_id,
                    "asset_id": asset_id,
                    "declarer_id": declarer_id,
                    "licensor_id": licensor_id or declarer_id,
                    "territories": territories,
                    "languages": languages,
                    "products": products,
                    "purpose_tags": purpose_tags,
                    "slice_ids": slice_ids,
                    "embargo_not_before": embargo.isoformat() if embargo else None,
                    "not_after": expiry.isoformat() if expiry else None,
                    "basis_type": basis_type,
                    "notes": notes,
                },
            )
            append_within_tx(conn, [event])
            if expiry is not None:
                jobs_mod.schedule(
                    conn, "RIGHT_EXPIRY", {"right_id": right_id},
                    due_at=expiry, job_id=f"expire-{right_id}",
                )
            return {"right_id": right_id, "asset_id": asset_id, "not_after": expiry.isoformat() if expiry else None}

    # ======================================================================
    # 切片与双职责审校
    # ======================================================================

    def cut_slices(
        self,
        asset_id: str,
        slices: list[dict[str, Any]],
        *,
        actor_id: str | None = None,
    ) -> list[str]:
        require_id(asset_id, "asset_id")
        if not isinstance(slices, list) or not slices:
            raise ValidationFailure("slices 必须是非空数组")
        with self.store.tx() as conn:
            asset = projections.get_asset(conn, asset_id)
            if asset is None:
                raise NotFoundError("资产不存在", details={"asset_id": asset_id})
            if asset["status"] == AssetStatus.QUARANTINED:
                raise ConflictError("资产处于争议隔离状态，不得切片")
            used_ordinals = {row["ordinal"] for row in projections.slices_for_asset(conn, asset_id)}
            next_ordinal = (max(used_ordinals) + 1) if used_ordinals else 1
            events: list[dict] = []
            slice_ids: list[str] = []
            for item in slices:
                content_hash = item.get("content_hash")
                require_id(content_hash, "content_hash")
                ordinal = item.get("ordinal")
                if ordinal is None:
                    while next_ordinal in used_ordinals:
                        next_ordinal += 1
                    ordinal = next_ordinal
                if not isinstance(ordinal, int) or ordinal < 1 or ordinal in used_ordinals:
                    raise ValidationFailure(f"切片序号冲突: {ordinal}")
                used_ordinals.add(ordinal)
                next_ordinal += 1
                slice_id = item.get("slice_id") or f"{asset_id}:s{ordinal}"
                require_id(slice_id, "slice_id")
                if projections.get_slice(conn, slice_id) is not None:
                    raise ConflictError("切片编号已存在", details={"slice_id": slice_id})
                sensitivity = item.get("sensitivity", {})
                if not isinstance(sensitivity, dict):
                    raise ValidationFailure("sensitivity 必须是对象")
                events.append(
                    _event(
                        AggregateType.CORPUS_SLICE, slice_id, 1,
                        EventType.SLICE_CUT,
                        {
                            "asset_id": asset_id,
                            "ordinal": ordinal,
                            "content_hash": content_hash,
                            "sensitivity": sensitivity,
                            "cut_by": actor_id,
                        },
                    )
                )
                slice_ids.append(slice_id)
            append_within_tx(conn, events)
            return slice_ids

    def review_slice(
        self,
        *,
        slice_id: str,
        review_kind: str,
        reviewer_id: str,
        passed: bool,
        notes: str | None = None,
    ) -> dict:
        require_id(slice_id, "slice_id")
        require_id(reviewer_id, "reviewer_id")
        if review_kind not in {ReviewKind.DESENSITIZATION, ReviewKind.FACT}:
            raise ValidationFailure(
                "review_kind 必须是 desensitization（脱敏复核）或 fact（事实审校）"
            )
        if not isinstance(passed, bool):
            raise ValidationFailure("passed 必须是布尔值")
        with self.store.tx() as conn:
            slice_row = projections.get_slice(conn, slice_id)
            if slice_row is None:
                raise NotFoundError("切片不存在", details={"slice_id": slice_id})
            version = _next_version(conn, slice_id)
            event = _event(
                AggregateType.CORPUS_SLICE, slice_id, version,
                EventType.SLICE_REVIEWED,
                {
                    "review_kind": review_kind,
                    "reviewer_id": reviewer_id,
                    "passed": passed,
                    "notes": notes,
                },
            )
            append_within_tx(conn, [event])
            return {"slice_id": slice_id, "review_kind": review_kind,
                    "reviewer_id": reviewer_id, "passed": passed}

    @staticmethod
    def _review_gate(conn: sqlite3.Connection, slice_id: str) -> tuple[bool, str | None]:
        """脱敏与事实审校：两类都要通过，且必须是不同的人。"""
        reviews = projections.latest_reviews(conn, slice_id)
        desens = reviews.get(str(ReviewKind.DESENSITIZATION))
        fact = reviews.get(str(ReviewKind.FACT))
        if desens is None or fact is None:
            return False, "review_incomplete"
        if not desens["passed"] or not fact["passed"]:
            return False, "review_not_passed"
        if desens["reviewer_id"] == fact["reviewer_id"]:
            return False, "reviewer_must_differ"
        return True, None

    # ======================================================================
    # 使用申请：材料指纹 + 改变后旧批准作废
    # ======================================================================

    def create_request(
        self,
        *,
        applicant_id: str,
        purpose: str,
        slice_ids: list[str],
        territories: list[str],
        languages: list[str],
        products: list[str],
        materials: dict[str, Any],
        request_id: str | None = None,
        occurred_at: datetime | None = None,
    ) -> dict:
        require_id(applicant_id, "applicant_id")
        require_text(purpose, "purpose")
        slice_ids = normalize_list(slice_ids, "slice_ids")
        territories = normalize_list(territories, "territories")
        languages = normalize_list(languages, "languages")
        products = normalize_list(products, "products")
        if not isinstance(materials, dict) or not materials:
            raise ValidationFailure("materials 必须是非空对象，用于生成材料指纹")
        request_id = request_id or f"req-{uuid.uuid4().hex[:16]}"
        require_id(request_id, "request_id")
        fingerprint = materials_fingerprint(materials)

        with self.store.tx() as conn:
            if projections.get_request(conn, request_id) is not None:
                raise ConflictError("申请编号已存在", details={"request_id": request_id})
            for slice_id in slice_ids:
                if projections.get_slice(conn, slice_id) is None:
                    raise NotFoundError("切片不存在", details={"slice_id": slice_id})
            event = _event(
                AggregateType.USE_REQUEST, request_id, 1,
                EventType.USE_REQUESTED,
                {
                    "applicant_id": applicant_id,
                    "purpose": purpose,
                    "slice_ids": slice_ids,
                    "territories": territories,
                    "languages": languages,
                    "products": products,
                    "materials": materials,
                    "materials_fingerprint": fingerprint,
                },
                occurred_at=occurred_at,
            )
            append_within_tx(conn, [event], idempotency_key=f"request:{request_id}")
            return {"request_id": request_id, "materials_fingerprint": fingerprint,
                    "status": "pending"}

    def supersede_request(
        self,
        *,
        old_request_id: str,
        new_request_id: str | None = None,
        applicant_id: str,
        materials: dict[str, Any],
        purpose: str | None = None,
        slice_ids: list[str] | None = None,
        territories: list[str] | None = None,
        languages: list[str] | None = None,
        products: list[str] | None = None,
    ) -> dict:
        """申请人材料改变：旧申请及其批准立即失效，新申请进入待决。"""
        require_id(old_request_id, "old_request_id")
        with self.store.tx() as conn:
            old = projections.get_request(conn, old_request_id)
            if old is None:
                raise NotFoundError("旧申请不存在", details={"request_id": old_request_id})
            if old["applicant_id"] != applicant_id:
                raise AuthorizationError("只有原申请人可以声明材料变更并作废旧申请")
            if old["status"] == "superseded":
                raise ConflictError("旧申请已被作废", details={"request_id": old_request_id})

            new_fp = materials_fingerprint(materials)
            if new_fp == old["materials_fingerprint"]:
                raise ConflictError("材料指纹未发生变化，无需作废重提")

            new_request_id = new_request_id or f"req-{uuid.uuid4().hex[:16]}"
            require_id(new_request_id, "new_request_id")
            if projections.get_request(conn, new_request_id) is not None:
                raise ConflictError("新申请编号已存在", details={"request_id": new_request_id})

            events = [
                _event(
                    AggregateType.USE_REQUEST, new_request_id, 1,
                    EventType.USE_REQUESTED,
                    {
                        "applicant_id": applicant_id,
                        "purpose": purpose or old["purpose"],
                        "slice_ids": slice_ids or _jload(old["slice_ids"], []),
                        "territories": territories or _jload(old["territories"], []),
                        "languages": languages or _jload(old["languages"], []),
                        "products": products or _jload(old["products"], []),
                        "materials": materials,
                        "materials_fingerprint": new_fp,
                        "supersedes": old_request_id,
                    },
                ),
                _event(
                    AggregateType.USE_REQUEST, old_request_id,
                    _next_version(conn, old_request_id),
                    EventType.REQUEST_SUPERSEDED,
                    {"superseded_by": new_request_id, "reason": "applicant_materials_changed"},
                ),
            ]
            # 每个仍有效决定在其自身聚合上留下失效事件，记录原因与替代申请
            for dec in conn.execute(
                "SELECT decision_id FROM p_decisions WHERE request_id = ? AND effective = 1",
                (old_request_id,),
            ).fetchall():
                events.append(_event(
                    AggregateType.RELEASE_DECISION, dec["decision_id"],
                    _next_version(conn, dec["decision_id"]),
                    EventType.DECISION_EFFECTIVENESS_CHANGED,
                    {"effective": False, "reason": "applicant_materials_changed",
                     "superseded_by": new_request_id},
                ))
            append_within_tx(conn, events)
            return {"old_request_id": old_request_id, "new_request_id": new_request_id,
                    "old_status": "superseded", "new_status": "pending",
                    "materials_fingerprint": new_fp}

    # ======================================================================
    # 放行决定：唯一有效决定 + 双审 + 权利交集
    # ======================================================================

    @staticmethod
    def _right_covers_dimensions(
        right: sqlite3.Row,
        *,
        date: datetime,
        territories: Iterable[str],
        languages: Iterable[str],
        products: Iterable[str],
        purpose: str | None,
    ) -> bool:
        if right["withdrawn"] or not right["active"]:
            return False
        if right["embargo_not_before"]:
            if date < parse_dt(right["embargo_not_before"], "embargo_not_before"):
                return False
        if right["not_after"]:
            if date > parse_dt(right["not_after"], "not_after"):
                return False
        allowed = {
            "territories": set(_jload(right["territories"], [])),
            "languages": set(_jload(right["languages"], [])),
            "products": set(_jload(right["products"], [])),
        }
        requested = {
            "territories": list(territories),
            "languages": list(languages),
            "products": list(products),
        }
        for dim, values in requested.items():
            for value in values:
                if WILDCARD not in allowed[dim] and value not in allowed[dim]:
                    return False
        purpose_tags = set(_jload(right["purpose_tags"], []))
        if purpose is not None and WILDCARD not in purpose_tags and purpose not in purpose_tags:
            return False
        return True

    def _covering_rights(
        self, conn: sqlite3.Connection, *, asset_id: str, slice_id: str,
        date: datetime, territories, languages, products, purpose,
    ) -> list[sqlite3.Row]:
        result = []
        for right in projections.rights_for_asset(conn, asset_id):
            scoped = _as_list(right["slice_ids"])
            if scoped and slice_id not in scoped:
                continue
            if self._right_covers_dimensions(
                right, date=date, territories=territories, languages=languages,
                products=products, purpose=purpose,
            ):
                result.append(right)
        return result

    def decide(
        self,
        *,
        request_id: str,
        decider_id: str,
        approved: bool,
        reason: str | None = None,
        occurred_at: datetime | None = None,
    ) -> dict:
        require_id(request_id, "request_id")
        require_id(decider_id, "decider_id")
        if not isinstance(approved, bool):
            raise ValidationFailure("approved 必须是布尔值")
        decided_at = occurred_at or now_utc()

        with self.store.tx() as conn:
            req = projections.get_request(conn, request_id)
            if req is None:
                raise NotFoundError("申请不存在", details={"request_id": request_id})
            if req["status"] != "pending":
                raise ConflictError(
                    "申请已有决定或已作废", details={"request_id": request_id,
                                              "status": req["status"]}
                )

            decision_id = f"dec-{uuid.uuid4().hex[:16]}"
            if not approved:
                if not reason:
                    raise ValidationFailure("驳回必须给出 reason")
                event = _event(
                    AggregateType.RELEASE_DECISION, decision_id, 1,
                    EventType.USE_REJECTED,
                    {"request_id": request_id, "decider_id": decider_id, "reason": reason},
                    occurred_at=decided_at,
                )
                append_within_tx(conn, [event])
                return {"decision_id": decision_id, "request_id": request_id,
                        "approved": False, "reason": reason}

            req_slices = _jload(req["slice_ids"], [])
            req_terr = _jload(req["territories"], [])
            req_lang = _jload(req["languages"], [])
            req_prod = _jload(req["products"], [])

            grants: list[dict] = []
            problems: dict[str, list[str]] = {}
            earliest_expiry: datetime | None = None
            for slice_id in req_slices:
                slice_row = projections.get_slice(conn, slice_id)
                if slice_row is None:
                    problems.setdefault(slice_id, []).append("slice_missing")
                    continue
                if slice_row["frozen"]:
                    problems.setdefault(slice_id, []).append("slice_frozen")
                asset = projections.get_asset(conn, slice_row["asset_id"])
                if asset["status"] != AssetStatus.ACTIVE:
                    problems.setdefault(slice_id, []).append(f"asset_{asset['status']}")
                gate_ok, gate_reason = self._review_gate(conn, slice_id)
                if not gate_ok:
                    problems.setdefault(slice_id, []).append(gate_reason)

                covering = self._covering_rights(
                    conn, asset_id=slice_row["asset_id"], slice_id=slice_id,
                    date=decided_at, territories=req_terr, languages=req_lang,
                    products=req_prod, purpose=req["purpose"],
                )
                if not covering:
                    problems.setdefault(slice_id, []).append("no_covering_right")
                    continue

                allowed = {"territories": set(), "languages": set(), "products": set()}
                for right in covering:
                    for dim in allowed:
                        values = _jload(right[dim], [])
                        if WILDCARD in values:
                            allowed[dim].update(req_terr if dim == "territories"
                                                else req_lang if dim == "languages"
                                                else req_prod)
                        else:
                            allowed[dim].update(
                                set(values) & set(req_terr if dim == "territories"
                                                  else req_lang if dim == "languages"
                                                  else req_prod)
                            )
                    if right["not_after"]:
                        exp = parse_dt(right["not_after"], "not_after")
                        earliest_expiry = exp if earliest_expiry is None else min(earliest_expiry, exp)
                missing = []
                for dim, requested in (("territories", req_terr), ("languages", req_lang),
                                       ("products", req_prod)):
                    if set(requested) - allowed[dim]:
                        missing.append(f"{dim}_uncovered")
                if missing:
                    problems.setdefault(slice_id, []).extend(missing)
                    continue
                grants.append({
                    "slice_id": slice_id,
                    "territories": sorted(allowed["territories"]),
                    "languages": sorted(allowed["languages"]),
                    "products": sorted(allowed["products"]),
                    "right_ids": [r["right_id"] for r in covering],
                })

            if problems:
                raise ValidationFailure(
                    "存在不满足放行条件的切片，不能批准（可驳回）",
                    details={"blockers": problems},
                )

            event = _event(
                AggregateType.RELEASE_DECISION, decision_id, 1,
                EventType.USE_APPROVED,
                {
                    "request_id": request_id,
                    "decider_id": decider_id,
                    "grants": grants,
                    "valid_from": decided_at.isoformat(),
                    "valid_until": earliest_expiry.isoformat() if earliest_expiry else None,
                },
                occurred_at=decided_at,
            )
            try:
                append_within_tx(conn, [event])
            except sqlite3.IntegrityError as exc:
                raise ConflictError(
                    "该申请已存在有效放行决定；并发放行以先提交者为准"
                ) from exc
            return {"decision_id": decision_id, "request_id": request_id,
                    "approved": True, "grants": grants,
                    "valid_until": earliest_expiry.isoformat() if earliest_expiry else None}

    # ======================================================================
    # 撤回：事件落库 + 可续跑传播作业
    # ======================================================================

    def withdraw_right(
        self,
        *,
        right_id: str,
        effective_at: datetime | str,
        reason: str,
        actor_id: str | None = None,
    ) -> dict:
        require_id(right_id, "right_id")
        effective = parse_dt(effective_at, "effective_at")
        if not reason or not reason.strip():
            raise ValidationFailure("撤回必须给出 reason")

        with self.store.tx() as conn:
            right = projections.get_right(conn, right_id)
            if right is None:
                raise NotFoundError("权利依据不存在", details={"right_id": right_id})
            if right["withdrawn"]:
                return {"right_id": right_id, "status": "already_withdrawn",
                        "propagation_job_id": f"withdraw-{right_id}"}

            affected = _as_list(right["slice_ids"]) or [
                row["slice_id"] for row in projections.slices_for_asset(conn, right["asset_id"])
            ]
            version = _next_version(conn, right_id)
            append_within_tx(conn, [_event(
                AggregateType.RIGHT_BASIS, right_id, version,
                EventType.RIGHT_WITHDRAWN,
                {"right_id": right_id, "effective_at": effective.isoformat(),
                 "reason": reason, "actor_id": actor_id,
                 "affected_slice_ids": affected},
            )])
            jobs_mod.schedule(
                conn, "WITHDRAW_PROPAGATION",
                {"right_id": right_id, "asset_id": right["asset_id"],
                 "slice_ids": affected, "effective_at": effective.isoformat(),
                 "reason": reason},
                due_at=now_utc(), job_id=f"withdraw-{right_id}",
            )
            return {"right_id": right_id, "status": "withdrawn",
                    "affected_slice_ids": affected,
                    "propagation_job_id": f"withdraw-{right_id}"}

    # ======================================================================
    # 派生数据集
    # ======================================================================

    def register_dataset(
        self,
        *,
        dataset_id: str,
        name: str,
        product: str,
        slice_ids: list[str],
        registered_by: str | None = None,
    ) -> dict:
        require_id(dataset_id, "dataset_id")
        require_text(name, "name")
        require_text(product, "product")
        if not isinstance(slice_ids, list) or any(not isinstance(s, str) or not s.strip()
                                                 for s in slice_ids):
            raise ValidationFailure("slice_ids 必须是字符串数组（允许为空，成员可经派生流入）")
        with self.store.tx() as conn:
            if projections.get_dataset(conn, dataset_id) is not None:
                raise ConflictError("数据集编号已存在", details={"dataset_id": dataset_id})
            frozen_members: list[str] = []
            for slice_id in slice_ids:
                row = projections.get_slice(conn, slice_id)
                if row is None:
                    raise NotFoundError("切片不存在", details={"slice_id": slice_id})
                if row["frozen"]:
                    frozen_members.append(slice_id)
            events = [_event(
                AggregateType.DERIVED_DATASET, dataset_id, 1,
                EventType.DATASET_REGISTERED,
                {"dataset_id": dataset_id, "name": name, "product": product,
                 "slice_ids": slice_ids, "registered_by": registered_by},
            )]
            if frozen_members:
                events.append(_event(
                    AggregateType.DERIVED_DATASET, dataset_id, 2,
                    EventType.DATASET_FROZEN,
                    {"dataset_id": dataset_id,
                     "reason": f"contains_frozen_slices:{','.join(sorted(frozen_members))}"},
                ))
            append_within_tx(conn, events, idempotency_key=f"dataset:{dataset_id}")
            return {"dataset_id": dataset_id, "frozen": bool(frozen_members),
                    "frozen_slice_ids": frozen_members}

    def record_derivation(
        self, *, parent_dataset_id: str, child_dataset_id: str,
        derived_by: str | None = None,
    ) -> dict:
        require_id(parent_dataset_id, "parent_dataset_id")
        require_id(child_dataset_id, "child_dataset_id")
        if parent_dataset_id == child_dataset_id:
            raise ValidationFailure("数据集不能派生自自身")
        with self.store.tx() as conn:
            parent = projections.get_dataset(conn, parent_dataset_id)
            child = projections.get_dataset(conn, child_dataset_id)
            if parent is None or child is None:
                raise NotFoundError("父/子数据集必须均已登记")
            if child_dataset_id in projections.dataset_parents(conn, parent_dataset_id):
                raise ConflictError("不允许形成派生环")
            version = _next_version(conn, child_dataset_id)
            events = [_event(
                AggregateType.DERIVED_DATASET, child_dataset_id, version,
                EventType.DATASET_DERIVATION_RECORDED,
                {"parent_dataset_id": parent_dataset_id,
                 "child_dataset_id": child_dataset_id, "derived_by": derived_by},
            )]
            if parent["frozen"] and not child["frozen"]:
                events.append(_event(
                    AggregateType.DERIVED_DATASET, child_dataset_id, version + 1,
                    EventType.DATASET_FROZEN,
                    {"dataset_id": child_dataset_id,
                     "reason": f"derived_from_frozen:{parent_dataset_id}"},
                ))
            append_within_tx(
                conn, events,
                idempotency_key=f"derivation:{parent_dataset_id}:{child_dataset_id}",
            )
            return {"parent_dataset_id": parent_dataset_id,
                    "child_dataset_id": child_dataset_id,
                    "child_frozen": bool(parent["frozen"])}

    # ======================================================================
    # 发布事实（保留当时依据）与处置义务
    # ======================================================================

    def record_publication(
        self,
        *,
        slice_id: str,
        published_at: datetime | str,
        basis: dict[str, Any],
        publication_id: str | None = None,
    ) -> dict:
        require_id(slice_id, "slice_id")
        published = parse_dt(published_at, "published_at")
        if not isinstance(basis, dict) or not basis:
            raise ValidationFailure("发布必须记录当时依据 basis（非空对象）")
        publication_id = publication_id or f"pub-{uuid.uuid4().hex[:16]}"
        require_id(publication_id, "publication_id")
        with self.store.tx() as conn:
            slice_row = projections.get_slice(conn, slice_id)
            if slice_row is None:
                raise NotFoundError("切片不存在", details={"slice_id": slice_id})
            # 快照发布时点仍然有效的权利依据，确保"当时依据"永久可查
            snapshot = []
            for right in projections.rights_for_asset(conn, slice_row["asset_id"]):
                scoped = _as_list(right["slice_ids"])
                if scoped and slice_id not in scoped:
                    continue
                if not right["withdrawn"] and right["active"]:
                    snapshot.append({
                        "right_id": right["right_id"],
                        "territories": _jload(right["territories"], []),
                        "languages": _jload(right["languages"], []),
                        "products": _jload(right["products"], []),
                        "not_after": right["not_after"],
                    })
            version = _next_version(conn, slice_id)
            event = _event(
                AggregateType.CORPUS_SLICE, slice_id, version,
                EventType.PUBLICATION_RECORDED,
                {"publication_id": publication_id, "published_at": published.isoformat(),
                 "basis": {"declared": basis, "rights_snapshot": snapshot}},
            )
            append_within_tx(conn, [event], idempotency_key=f"pub:{publication_id}")
            return {"publication_id": publication_id, "slice_id": slice_id,
                    "published_at": published.isoformat()}

    def fulfill_disposition(
        self, *, disposition_id: str, fulfilled_by: str, note: str | None = None
    ) -> dict:
        require_id(disposition_id, "disposition_id")
        require_id(fulfilled_by, "fulfilled_by")
        with self.store.tx() as conn:
            row = conn.execute(
                "SELECT * FROM p_dispositions WHERE disposition_id = ?", (disposition_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError("处置义务不存在", details={"disposition_id": disposition_id})
            if row["status"] == "fulfilled":
                return {"disposition_id": disposition_id, "status": "fulfilled"}
            version = _next_version(conn, disposition_id)
            append_within_tx(conn, [_event(
                AggregateType.DISPOSITION, disposition_id, version,
                EventType.DISPOSITION_FULFILLED,
                {"disposition_id": disposition_id, "fulfilled_by": fulfilled_by, "note": note},
            )])
            return {"disposition_id": disposition_id, "status": "fulfilled"}

    # ======================================================================
    # 资格判定：指定日期 × 地区 × 语言 × 产品
    # ======================================================================

    def slice_clearance(
        self, *, slice_id: str, date: datetime | str,
        territory: str, language: str, product: str, purpose: str | None = None,
    ) -> dict:
        require_id(slice_id, "slice_id")
        at = self._parse_query_date(date)
        require_text(territory, "territory", max_length=64)
        require_text(language, "language", max_length=64)
        require_text(product, "product", max_length=128)

        with self.store.ro() as conn:
            slice_row = projections.get_slice(conn, slice_id)
            if slice_row is None:
                raise NotFoundError("切片不存在", details={"slice_id": slice_id})
            asset = projections.get_asset(conn, slice_row["asset_id"])
            blockers: list[str] = []

            if asset["status"] == AssetStatus.QUARANTINED:
                blockers.append("asset_disputed")
            if slice_row["frozen"]:
                blockers.append("slice_frozen")

            gate_ok, gate_reason = self._review_gate(conn, slice_id)
            if not gate_ok:
                blockers.append(gate_reason)

            purpose = purpose  # None 时资格判定不按用途标签收窄
            covering = self._covering_rights(
                conn, asset_id=asset["asset_id"], slice_id=slice_id, date=at,
                territories=[territory], languages=[language], products=[product],
                purpose=purpose,
            )
            if not covering:
                blockers.append("no_covering_right")

            effective_decision = None
            decision_rows = projections.decisions_for_slice(conn, slice_id)
            for dec in decision_rows:
                if not (dec["effective"] and dec["approved"]):
                    continue
                if dec["valid_from"] and at < parse_dt(dec["valid_from"], "valid_from"):
                    continue
                if dec["valid_until"] and at > parse_dt(dec["valid_until"], "valid_until"):
                    continue
                grant_ok = False
                for grant in _jload(dec["grants"], []):
                    if grant["slice_id"] != slice_id:
                        continue
                    if (territory in grant["territories"]
                            and language in grant["languages"]
                            and product in grant["products"]):
                        grant_ok = True
                        break
                if not grant_ok:
                    continue
                req = projections.get_request(conn, dec["request_id"])
                effective_decision = {
                    "decision_id": dec["decision_id"],
                    "request_id": dec["request_id"],
                    "decider_id": dec["decider_id"],
                    "decided_at": dec["decided_at"],
                    "valid_from": dec["valid_from"],
                    "valid_until": dec["valid_until"],
                    "materials_fingerprint": req["materials_fingerprint"] if req else None,
                }
                break
            if effective_decision is None:
                blockers.append("no_effective_decision")

            frozen_datasets = [
                dict(row) for row in conn.execute(
                    "SELECT dataset_id, freeze_reason FROM p_datasets WHERE frozen = 1 "
                    "AND dataset_id IN (SELECT dataset_id FROM p_dataset_slices WHERE slice_id = ?)",
                    (slice_id,),
                ).fetchall()
            ]
            open_dispositions = [
                dict(row) for row in conn.execute(
                    "SELECT disposition_id, kind, description FROM p_dispositions "
                    "WHERE slice_id = ? AND status = 'open'", (slice_id,),
                ).fetchall()
            ]

            return {
                "slice_id": slice_id,
                "asset_id": asset["asset_id"],
                "at": at.isoformat(),
                "territory": territory,
                "language": language,
                "product": product,
                "usable": not blockers,
                "blockers": blockers,
                "active_right_ids": [r["right_id"] for r in covering],
                "effective_decision": effective_decision,
                "frozen_datasets": frozen_datasets,
                "open_dispositions": open_dispositions,
                "sensitivity": _jload(slice_row["sensitivity"], {}),
            }

    @staticmethod
    def _parse_query_date(value: datetime | str) -> datetime:
        if isinstance(value, datetime):
            dt = value
            if dt.tzinfo is None:
                raise ValidationFailure("时间必须显式携带时区")
            return dt
        if not isinstance(value, str):
            raise ValidationFailure("date 必须是 ISO 日期或带时区时间")
        if len(value) == 10:  # YYYY-MM-DD → 当日 00:00Z，故解除禁发当日即放行
            value = value + "T00:00:00+00:00"
        return parse_dt(value, "date")

    # ======================================================================
    # 血缘反查
    # ======================================================================

    def dataset_manifest(self, dataset_id: str) -> dict:
        with self.store.ro() as conn:
            dataset = projections.get_dataset(conn, dataset_id)
            if dataset is None:
                raise NotFoundError("数据集不存在", details={"dataset_id": dataset_id})
            slices = []
            for srow in projections.slices_for_dataset(conn, dataset_id):
                asset = projections.get_asset(conn, srow["asset_id"])
                reviews = projections.latest_reviews(conn, srow["slice_id"])
                rights = []
                for right in projections.rights_for_asset(conn, asset["asset_id"]):
                    scoped = _as_list(right["slice_ids"])
                    if scoped and srow["slice_id"] not in scoped:
                        continue
                    rights.append({
                        "right_id": right["right_id"],
                        "declarer_id": right["declarer_id"],
                        "territories": _jload(right["territories"], []),
                        "languages": _jload(right["languages"], []),
                        "products": _jload(right["products"], []),
                        "embargo_not_before": right["embargo_not_before"],
                        "not_after": right["not_after"],
                        "withdrawn": bool(right["withdrawn"]),
                        "active": bool(right["active"]),
                    })
                deciders = []
                for dec in projections.decisions_for_slice(conn, srow["slice_id"]):
                    deciders.append({
                        "decision_id": dec["decision_id"],
                        "request_id": dec["request_id"],
                        "decider_id": dec["decider_id"],
                        "approved": bool(dec["approved"]),
                        "effective": bool(dec["effective"]),
                        "decided_at": dec["decided_at"],
                        "ineffective_reason": dec["ineffective_reason"],
                    })
                publications = []
                for pub in projections.publications_for_slice(conn, srow["slice_id"]):
                    publications.append({
                        "publication_id": pub["publication_id"],
                        "published_at": pub["published_at"],
                        "basis": _jload(pub["basis"], {}),
                    })
                pending = conn.execute(
                    "SELECT disposition_id, kind, description, created_at FROM p_dispositions "
                    "WHERE slice_id = ? AND status = 'open' ORDER BY created_at",
                    (srow["slice_id"],),
                ).fetchall()
                slices.append({
                    "slice_id": srow["slice_id"],
                    "ordinal": srow["ordinal"],
                    "content_hash": srow["content_hash"],
                    "frozen": bool(srow["frozen"]),
                    "freeze_reason": srow["freeze_reason"],
                    "sensitivity": _jload(srow["sensitivity"], {}),
                    "source_asset": {
                        "asset_id": asset["asset_id"],
                        "batch_id": asset["batch_id"],
                        "collector_id": asset["collector_id"],
                        "content_hash": asset["content_hash"],
                        "authorization_fingerprint": asset["authorization_fingerprint"],
                        "status": asset["status"],
                    },
                    "reviews": reviews,
                    "rights": rights,
                    "approvals": deciders,
                    "publications": publications,
                    "pending_dispositions": [dict(r) for r in pending],
                })
            return {
                "dataset_id": dataset_id,
                "name": dataset["name"],
                "product": dataset["product"],
                "frozen": bool(dataset["frozen"]),
                "freeze_reason": dataset["freeze_reason"],
                "parents": projections.dataset_parents(conn, dataset_id),
                "children": projections.dataset_descendants(conn, dataset_id),
                "slice_count": len(slices),
                "slices": slices,
            }

    def trace_asset(self, asset_id: str) -> dict:
        with self.store.ro() as conn:
            asset = projections.get_asset(conn, asset_id)
            if asset is None:
                raise NotFoundError("资产不存在", details={"asset_id": asset_id})
            slices = []
            for srow in projections.slices_for_asset(conn, asset_id):
                slices.append({
                    "slice_id": srow["slice_id"],
                    "ordinal": srow["ordinal"],
                    "frozen": bool(srow["frozen"]),
                    "datasets": projections.datasets_containing_slice(conn, srow["slice_id"]),
                })
            rights = [dict(r) for r in projections.rights_for_asset(conn, asset_id)]
            for right in rights:
                for key in ("territories", "languages", "products", "purpose_tags", "slice_ids"):
                    right[key] = _jload(right[key], [])
            return {"asset": dict(asset), "slices": slices, "rights": rights,
                    "events": self.store.events_for(str(AggregateType.SOURCE_ASSET), asset_id)}

    # ======================================================================
    # 后台作业：到期扫描 + 撤回传播（处理器，由 worker 驱动）
    # ======================================================================

    def job_handlers(self) -> dict:
        return {
            "RIGHT_EXPIRY": self._handle_right_expiry,
            "EXPIRY_SWEEP": self._handle_expiry_sweep,
            "WITHDRAW_PROPAGATION": self._handle_withdraw_propagation,
        }

    def schedule_expiry_sweep(self) -> None:
        """启动时调用：登记/恢复周期到期扫描作业（重启后续跑）。"""
        with self.store.tx() as conn:
            jobs_mod.ensure_recurring(
                conn, SWEEP_JOB_ID, "EXPIRY_SWEEP", {}, due_at=now_utc()
            )

    def _handle_right_expiry(self, conn: sqlite3.Connection, payload: dict, job_id: str) -> None:
        right = projections.get_right(conn, payload["right_id"])
        if right is None:
            return
        if not right["active"] or right["withdrawn"]:
            return
        version = _next_version(conn, right["right_id"])
        append_within_tx(conn, [_event(
            AggregateType.RIGHT_BASIS, right["right_id"], version,
            EventType.RIGHT_EXPIRED, {"right_id": right["right_id"], "reason": "not_after_reached"},
        )])

    def _handle_expiry_sweep(self, conn: sqlite3.Connection, payload: dict, job_id: str) -> None:
        now = now_utc()
        rows = conn.execute(
            "SELECT right_id, not_after FROM p_rights "
            "WHERE active = 1 AND withdrawn = 0 AND not_after IS NOT NULL AND not_after <= ?",
            (now.isoformat(),),
        ).fetchall()
        events = []
        for row in rows:
            events.append(_event(
                AggregateType.RIGHT_BASIS, row["right_id"], _next_version(conn, row["right_id"]),
                EventType.RIGHT_EXPIRED,
                {"right_id": row["right_id"], "reason": "expiry_sweep"},
                event_id=f"ev-expiry-sweep-{row['right_id']}-{row['not_after']}",
            ))
        append_within_tx(conn, events)
        # 下一轮扫描时刻随本次事务一起提交（见 jobs.run_due 的周期作业约定）
        return {"reschedule_at": now + SWEEP_INTERVAL}

    def _handle_withdraw_propagation(
        self, conn: sqlite3.Connection, payload: dict, job_id: str
    ) -> None:
        right_id = payload["right_id"]
        reason = f"right_withdrawn:{right_id}"
        frozen_at = parse_dt(payload["effective_at"], "effective_at")
        events: list[dict] = []

        # 撤回只冻结失去全部有效权利依据的切片；仍有其他有效授权覆盖的切片不动
        frozen_slice_ids: list[str] = []
        for slice_id in payload["slice_ids"]:
            slice_row = projections.get_slice(conn, slice_id)
            if slice_row is None:
                continue
            if slice_row["frozen"]:
                frozen_slice_ids.append(slice_id)
                continue
            remaining = []
            for r in projections.rights_for_asset(conn, slice_row["asset_id"]):
                scoped = _as_list(r["slice_ids"])
                if scoped and slice_id not in scoped:
                    continue
                if r["withdrawn"] or not r["active"]:
                    continue
                if r["embargo_not_before"] and frozen_at < parse_dt(
                        r["embargo_not_before"], "embargo_not_before"):
                    continue
                if r["not_after"] and frozen_at > parse_dt(r["not_after"], "not_after"):
                    continue
                remaining.append(r["right_id"])
            if remaining:
                continue
            frozen_slice_ids.append(slice_id)
            events.append(_event(
                AggregateType.CORPUS_SLICE, slice_id,
                _next_version(conn, slice_id),
                EventType.SLICE_FROZEN,
                {"reason": reason, "right_id": right_id,
                 "frozen_at": frozen_at.isoformat(),
                 "withdraw_reason": payload.get("reason")},
                event_id=f"ev-freeze-{right_id}-{slice_id}",
            ))

        # 处置义务只针对被冻结切片的已发布版本（当时依据保留在发布事件中）
        for slice_id in frozen_slice_ids:
            for pub in projections.publications_for_slice(conn, slice_id):
                if parse_dt(pub["published_at"], "published_at") > frozen_at:
                    continue
                disposition_id = f"disp-{pub['publication_id']}-{right_id}"
                if projections.disposition_exists(conn, disposition_id):
                    continue
                events.append(_event(
                    AggregateType.DISPOSITION, disposition_id, 1,
                    EventType.DISPOSITION_RAISED,
                    {"disposition_id": disposition_id, "slice_id": slice_id,
                     "kind": "post_publication_action",
                     "origin": {"type": "right_withdrawn", "right_id": right_id,
                                "publication_id": pub["publication_id"]},
                     "description": f"权利依据 {right_id} 撤回，须对已发布版本履行处置义务: "
                                    f"{payload.get('reason', '')}",
                     "raised_by": "system"},
                    event_id=f"ev-disp-{pub['publication_id']}-{right_id}",
                ))

        # 冻结仅波及含有受影响切片的数据集，并沿派生边向下传播
        affected_datasets: set[str] = set()
        for slice_id in frozen_slice_ids:
            affected_datasets.update(projections.datasets_containing_slice(conn, slice_id))
        for dataset_id in sorted(affected_datasets):
            drow = projections.get_dataset(conn, dataset_id)
            if drow is not None and not drow["frozen"]:
                events.append(_event(
                    AggregateType.DERIVED_DATASET, dataset_id,
                    _next_version(conn, dataset_id),
                    EventType.DATASET_FROZEN,
                    {"dataset_id": dataset_id, "reason": reason},
                    event_id=f"ev-dsfrozen-{right_id}-{dataset_id}",
                ))

        append_within_tx(conn, events)

    # ======================================================================
    # 列表/详情（供 API）
    # ======================================================================

    def list_assets(self) -> list[dict]:
        with self.store.ro() as conn:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM p_assets ORDER BY created_at").fetchall()]

    def list_requests(self) -> list[dict]:
        with self.store.ro() as conn:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM p_requests ORDER BY created_at").fetchall()]

    def list_datasets(self) -> list[dict]:
        with self.store.ro() as conn:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM p_datasets ORDER BY created_at").fetchall()]

    def list_open_dispositions(self) -> list[dict]:
        with self.store.ro() as conn:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM p_dispositions WHERE status = 'open' ORDER BY created_at"
            ).fetchall()]

    def job_stats(self) -> dict:
        with self.store.ro() as conn:
            return jobs_mod.stats(conn)
