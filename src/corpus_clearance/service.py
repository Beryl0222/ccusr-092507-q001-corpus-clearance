"""语料用途放行领域服务。

职责边界：
- 归集方只能声明自己拥有的来源与权利范围；
- 脱敏（desensitize）与事实审校（fact_check）必须由不同人员复核；
- 申请按材料指纹定版，材料改变后旧批准自动失效，必须重新申请；
- 同一申请跨机构并发放行只产生一个有效决定；
- 撤回只冻结受影响切片及其派生数据集，已发布版本保留当时依据并生成处置义务；
- 编号相同而正文或授权指纹不同则隔离争议；同批次同指纹安全重传。
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from .errors import (
    AuthorizationError,
    ConflictError,
    NotFoundError,
    QuarantineConflict,
    ValidationError,
)
from .store import EventStore, to_utc

HAINAN_TZ = ZoneInfo("Asia/Shanghai")
REVIEW_DUTIES = ("desensitize", "fact_check")
SLICE_BLOCKED_STATUSES = {"frozen"}


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def canonical_hash(value: object) -> str:
    return _sha256_text(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def _deterministic_id(prefix: str, seed: str) -> str:
    return f"{prefix}_{_sha256_text(seed)[:24]}"


class ClearanceService:
    def __init__(self, store: EventStore, *, clock=None):
        self.store = store
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None:
            raise ValueError("时钟必须返回带时区时间")
        return value.astimezone(timezone.utc)

    # ── 查询辅助 ────────────────────────────────────────────────
    def _row(self, sql: str, params: tuple = ()):
        return self.store.fetchone(sql, params)

    def _rows(self, sql: str, params: tuple = ()):
        return self.store.fetchall(sql, params)

    def _get_org(self, org_id: str):
        row = self._row("SELECT * FROM p_orgs WHERE org_id=?", (org_id,))
        if row is None:
            raise NotFoundError(f"机构 {org_id} 未登记", "org_not_found")
        return row

    def _require_org_role(self, org_id: str, role: str):
        org = self._get_org(org_id)
        roles = set(json.loads(org["roles"]))
        if role not in roles:
            raise AuthorizationError(f"机构 {org_id} 不具备 {role} 职责", "org_role_required")
        return org

    def _get_asset(self, asset_id: str):
        row = self._row("SELECT * FROM p_assets WHERE asset_id=?", (asset_id,))
        if row is None:
            raise NotFoundError(f"语料 {asset_id} 不存在", "asset_not_found")
        return row

    def _get_slice(self, slice_id: str):
        row = self._row("SELECT * FROM p_slices WHERE slice_id=?", (slice_id,))
        if row is None:
            raise NotFoundError(f"切片 {slice_id} 不存在", "slice_not_found")
        return row

    def _get_dataset(self, dataset_id: str):
        row = self._row("SELECT * FROM p_datasets WHERE dataset_id=?", (dataset_id,))
        if row is None:
            raise NotFoundError(f"数据集 {dataset_id} 不存在", "dataset_not_found")
        return row

    # ── 机构 ────────────────────────────────────────────────────
    def register_org(self, org_id: str, name: str, roles: list[str]) -> dict:
        if not org_id or not name:
            raise ValidationError("机构编号与名称不能为空")
        known = {"collector", "reviewer", "approver", "applicant", "platform"}
        unknown = set(roles) - known
        if unknown:
            raise ValidationError(f"未知机构职责: {sorted(unknown)}", "unknown_role")
        return self.store.append(
            "ORG_REGISTERED", "source_org", org_id, {"org_id": org_id, "name": name, "roles": roles}
        )

    # ── 语料摄取 ────────────────────────────────────────────────
    def ingest_asset(
        self,
        asset_id: str,
        contributor_org_id: str,
        source_id: str,
        batch_id: str,
        content_hash: str,
        body_ref: str | None = None,
        body_text: str | None = None,
        declared_grants: list[dict] | None = None,
        flags: dict | None = None,
        embargo_until: str | None = None,
    ) -> dict:
        """归集方上传语料版本。

        - 相同批次+指纹：安全重传，返回首次结果；
        - 编号相同但正文指纹或授权指纹不同：隔离争议，不覆盖现行版本。
        """
        self._require_org_role(contributor_org_id, "collector")
        declared_grants = declared_grants or []
        grant_print = canonical_hash(declared_grants)
        flags = dict(flags or {})
        if embargo_until:
            flags["embargo_until"] = to_utc(embargo_until).isoformat()

        dedup = self.store.find_ingest(batch_id, content_hash)
        if dedup is not None:
            existing = self._row(
                "SELECT * FROM p_asset_versions WHERE asset_id=? AND version=?",
                (dedup["asset_id"], dedup["version"]),
            )
            return {
                "deduplicated": True,
                "asset_id": dedup["asset_id"],
                "version": dedup["version"],
                "status": existing["status"] if existing else "ingested",
            }

        current = self._row(
            "SELECT av.* FROM p_asset_versions av JOIN p_assets a ON a.asset_id=av.asset_id"
            " WHERE av.asset_id=? AND av.version=a.current_version",
            (asset_id,),
        )
        quarantined = bool(
            current is not None
            and (current["content_hash"] != content_hash or current["grant_print"] != grant_print)
        )

        quarantined_version = None
        grant_ids: list[str] = []
        with self.store.transaction():
            event = self.store.append(
                "ASSET_INGESTED",
                "source_asset",
                asset_id,
                {
                    "contributor_org_id": contributor_org_id,
                    "source_id": source_id,
                    "batch_id": batch_id,
                    "content_hash": content_hash,
                    "grant_print": grant_print,
                    "declared_grants": declared_grants,
                    "body_ref": body_ref,
                    "body_excerpt": (body_text or "")[:200],
                    "flags": flags,
                    "quarantined": quarantined,
                },
            )
            self.store.remember_ingest(
                batch_id, content_hash, asset_id, event["version"], quarantined, event["seq"]
            )

            if quarantined:
                # 争议版本只隔离，不覆盖现行版本（投影保持 current_version 不变）
                self.store.append(
                    "ASSET_QUARANTINED",
                    "source_asset",
                    asset_id,
                    {
                        "reason": "同一编号的正文指纹或授权声明与现行版本不一致",
                        "incoming_version": event["version"],
                        "current_version": current["version"],
                        "incoming_hash": content_hash,
                        "current_hash": current["content_hash"],
                        "grant_print_changed": current["grant_print"] != grant_print,
                    },
                )
                quarantined_version = event["version"]
            else:
                # 新摄取时把随附授权登记为权利依据（归集方声明自有范围）
                for g in declared_grants:
                    recorded = self.record_grant(
                        asset_id=asset_id,
                        asset_version=event["version"],
                        granter_org_id=contributor_org_id,
                        territories=g["territories"],
                        languages=g["languages"],
                        products=g["products"],
                        valid_from=g.get("valid_from"),
                        retain_until=g["retain_until"],
                        grant_id=g.get("grant_id"),
                    )
                    grant_ids.append(recorded["payload"]["grant_id"])

        if quarantined:
            raise QuarantineConflict(
                f"语料 {asset_id} 第 {quarantined_version} 版与现行第 {current['version']} 版正文或授权不一致，已隔离争议",
                details={
                    "asset_id": asset_id,
                    "quarantined_version": quarantined_version,
                    "current_version": current["version"],
                },
            )
        return {"deduplicated": False, "asset_id": asset_id, "version": event["version"], "status": "ingested",
                "grants": grant_ids}

    # ── 争议裁决 ────────────────────────────────────────────────
    def resolve_dispute(
        self, asset_id: str, version: int, resolution: str, resolver_org_id: str, note: str | None = None
    ) -> dict:
        """对隔离的争议版本作裁决：promote 升为现行版本（补登记其授权），reject 作废。"""
        self._require_org_role(resolver_org_id, "platform")
        if resolution not in ("promote", "reject"):
            raise ValidationError("resolution 必须是 promote 或 reject")
        v = self._row(
            "SELECT * FROM p_asset_versions WHERE asset_id=? AND version=?", (asset_id, version)
        )
        if v is None:
            raise NotFoundError(f"语料版本 {asset_id}@{version} 不存在", "asset_version_not_found")
        if v["status"] != "quarantined":
            raise ConflictError(f"版本 {version} 当前状态 {v['status']}，无可裁决争议", "not_quarantined")

        with self.store.transaction():
            event = self.store.append(
                "ASSET_DISPUTE_RESOLVED",
                "source_asset",
                asset_id,
                {"resolved_version": version, "resolution": resolution,
                 "resolver_org_id": resolver_org_id, "note": note},
            )
            grant_ids: list[str] = []
            if resolution == "promote":
                # 升为现行版本时，把随该版上报的授权正式登记（隔离期间未生效）。
                # 按该版本的内容指纹+授权指纹回溯其摄取事件。
                declared: list[dict] = []
                for r in self._rows(
                    "SELECT payload FROM events WHERE aggregate_type='source_asset' AND aggregate_id=?"
                    " AND event_type='ASSET_INGESTED' ORDER BY seq",
                    (asset_id,),
                ):
                    candidate = json.loads(r["payload"])
                    if candidate.get("content_hash") == v["content_hash"] and \
                            candidate.get("grant_print") == v["grant_print"]:
                        declared = candidate.get("declared_grants", [])
                        break
                for g in declared:
                    rec = self.record_grant(
                        asset_id=asset_id, asset_version=version,
                        granter_org_id=v["contributor"],
                        territories=g["territories"], languages=g["languages"],
                        products=g["products"], valid_from=g.get("valid_from"),
                        retain_until=g["retain_until"], grant_id=g.get("grant_id"),
                    )
                    if not rec.get("deduplicated"):
                        grant_ids.append(rec["payload"]["grant_id"])
            event["resolved_grants"] = grant_ids
            return event

    # ── 段落切片 ────────────────────────────────────────────────
    def cut_slice(
        self,
        slice_id: str | None,
        asset_id: str,
        ordinal: int,
        text: str,
        flags: dict | None = None,
        grant_ids: list[str] | None = None,
    ) -> dict:
        asset = self._get_asset(asset_id)
        version = asset["current_version"]
        version_row = self._row(
            "SELECT * FROM p_asset_versions WHERE asset_id=? AND version=?", (asset_id, version)
        )
        if version_row is not None and version_row["status"] == "quarantined":
            raise ConflictError(f"语料 {asset_id} 现行版本处于隔离状态，不得切片", "asset_quarantined")
        merged_flags = json.loads(version_row["flags"]) if version_row is not None else {}
        merged_flags.update(flags or {})
        grant_ids = grant_ids or []
        valid_grant_ids = {
            r["grant_id"]
            for r in self._rows(
                "SELECT grant_id FROM p_grants WHERE asset_id=? AND asset_version=? AND status='active'",
                (asset_id, version),
            )
        }
        # 未显式给出时，默认挂接该版本全部有效权利依据
        if not grant_ids:
            grant_ids = sorted(valid_grant_ids)
        unknown = [g for g in grant_ids if g not in valid_grant_ids]
        if unknown:
            raise ValidationError(f"权利依据 {unknown} 不属于该语料现行版本或已失效", "grant_unavailable")
        slice_id = slice_id or _new_id("slice")
        content_hash = _sha256_text(text)
        return self.store.append(
            "SLICE_CUT",
            "corpus_slice",
            slice_id,
            {
                "slice_id": slice_id,
                "asset_id": asset_id,
                "asset_version": version,
                "ordinal": ordinal,
                "content_hash": content_hash,
                "text": text,
                "flags": merged_flags,
                "grants": grant_ids,
            },
        )

    # ── 复核：脱敏与事实审校职责分离 ───────────────────────────
    def review_slice(
        self, slice_id: str, duty: str, reviewer_org_id: str, reviewer_id: str, passed: bool, note: str | None = None
    ) -> dict:
        self._get_slice(slice_id)
        if duty not in REVIEW_DUTIES:
            raise ValidationError(f"复核职责必须是 {REVIEW_DUTIES} 之一", "unknown_duty")
        self._require_org_role(reviewer_org_id, "reviewer")
        other = self._row(
            "SELECT reviewer_id FROM p_reviews WHERE slice_id=? AND duty=?", (slice_id, _other_duty(duty))
        )
        if other is not None and other["reviewer_id"] == reviewer_id:
            raise AuthorizationError(
                "脱敏与事实审校必须由不同人员完成，同一复核人不得承担两项职责", "segregation_of_duty"
            )
        return self.store.append(
            "SLICE_REVIEWED",
            "corpus_slice",
            slice_id,
            {
                "duty": duty,
                "reviewer_id": reviewer_id,
                "reviewer_org_id": reviewer_org_id,
                "passed": bool(passed),
                "note": note,
            },
        )

    def _reviews_pass(self, slice_id: str) -> dict[str, str]:
        result = {}
        for duty in REVIEW_DUTIES:
            row = self._row("SELECT * FROM p_reviews WHERE slice_id=? AND duty=?", (slice_id, duty))
            if row is None or not row["passed"]:
                result[duty] = "missing_or_failed"
        return result

    # ── 权利依据 ────────────────────────────────────────────────
    def record_grant(
        self,
        asset_id: str,
        asset_version: int,
        granter_org_id: str,
        territories: list[str],
        languages: list[str],
        products: list[str],
        retain_until: str,
        valid_from: str | None = None,
        grant_id: str | None = None,
    ) -> dict:
        """归集方只能为自己归集的语料版本登记自有授权。"""
        org = self._get_org(granter_org_id)
        version_row = self._row(
            "SELECT * FROM p_asset_versions WHERE asset_id=? AND version=?", (asset_id, asset_version)
        )
        if version_row is None:
            raise NotFoundError(f"语料版本 {asset_id}@{asset_version} 不存在", "asset_version_not_found")
        if version_row["contributor"] != granter_org_id:
            raise AuthorizationError(
                f"机构 {granter_org_id} 只能声明自己归集的语料，不得为 {version_row['contributor']} 归集的版本登记权利",
                "grant_outside_scope",
            )
        for field, value in (("territories", territories), ("languages", languages), ("products", products)):
            if not isinstance(value, list) or not value or not all(isinstance(v, str) and v for v in value):
                raise ValidationError(f"{field} 必须是非空字符串数组")
        until = to_utc(retain_until)
        if valid_from:
            start = to_utc(valid_from)
        else:
            # 未显式指定时，自声明日海南时区零点起生效（该日历日即视为可用）
            start = self.now().astimezone(HAINAN_TZ).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
        if until <= start:
            raise ValidationError("保留期限必须晚于生效起点", "retain_window_invalid")
        grant_id = grant_id or _new_id("grant")

        existing = self._row("SELECT * FROM p_grants WHERE grant_id=?", (grant_id,))
        if existing is not None:
            # 相同授权编号安全重传；关键范围字段被改写则拒绝
            if (existing["asset_id"], existing["asset_version"], existing["granter_org"]) != (
                asset_id, asset_version, granter_org_id
            ):
                raise ConflictError(f"权利依据 {grant_id} 已绑定其他语料版本或归集方", "grant_rebound")
            return {"deduplicated": True, "payload": {"grant_id": grant_id}}

        with self.store.transaction():
            event = self.store.append(
                "GRANT_RECORDED",
                "rights_grant",
                grant_id,
                {
                    "grant_id": grant_id,
                    "asset_id": asset_id,
                    "asset_version": asset_version,
                    "granter_org_id": granter_org_id,
                    "granter_name": org["name"],
                    "territories": sorted(territories),
                    "languages": sorted(languages),
                    "products": sorted(products),
                    "valid_from": start.isoformat(),
                    "retain_until": until.isoformat(),
                },
            )
            # 保留期限扫描作业（重启后由调度器续跑）
            self.store.enqueue_job(
                _new_id("job"), "grant_expiry", f"grant:{grant_id}",
                {"grant_id": grant_id}, until,
            )
        return event

    def withdraw_grant(self, grant_id: str, reason: str, effective_at: str | None = None,
                       acting_org_id: str | None = None) -> dict:
        row = self._row("SELECT * FROM p_grants WHERE grant_id=?", (grant_id,))
        if row is None:
            raise NotFoundError(f"权利依据 {grant_id} 不存在", "grant_not_found")
        if acting_org_id is not None and row["granter_org"] != acting_org_id:
            raise AuthorizationError(
                f"只有授权方 {row['granter_org']} 可以撤回 {grant_id}，{acting_org_id} 无权撤回",
                "withdraw_not_granter",
            )
        effective = to_utc(effective_at) if effective_at else self.now()

        with self.store.transaction():
            if row["status"] != "active":
                raise ConflictError(f"权利依据 {grant_id} 已处于 {row['status']} 状态", "grant_not_active")
            w = self.store.append(
                "GRANT_WITHDRAWN",
                "rights_grant",
                grant_id,
                {"grant_id": grant_id, "reason": reason, "effective_at": effective.isoformat()},
            )
            # 同步登记领域契约中的 RIGHT_WITHDRAWN（对外交换形态）
            self.store.append(
                "RIGHT_WITHDRAWN",
                "rights_grant",
                grant_id,
                {"right_id": grant_id, "effective_at": effective.isoformat(), "reason": reason},
            )
            # 撤回传播登记为持久作业：即时生效随请求执行；未来生效或服务中断则由扫描续跑
            run_at = effective if effective > self.now() else self.now()
            self.store.enqueue_job(
                _new_id("job"), "withdrawal", f"grant:{grant_id}",
                {"grant_id": grant_id, "reason": reason, "effective_at": effective.isoformat()}, run_at,
            )

        if effective <= self.now():
            self.propagate_withdrawal(grant_id, reason, effective)
            job = self._row(
                "SELECT id FROM jobs WHERE kind='withdrawal' AND ref_key=?", (f"grant:{grant_id}",)
            )
            if job is not None:
                self.store.finish_job(job["id"])
        return w

    def propagate_withdrawal(self, grant_id: str, reason: str, effective: datetime | str) -> int:
        """冻结挂接该权利依据的全部切片及其派生数据集。

        幂等：已冻结切片不重复产生事件，处置义务按 (数据集, 切片, 授权) 去重。
        服务重启后可由作业队安全重放。返回新冻结切片数。
        """
        effective_dt = to_utc(effective)
        frozen = 0
        with self.store.transaction():
            grant = self._row("SELECT * FROM p_grants WHERE grant_id=?", (grant_id,))
            if grant is None:
                raise NotFoundError(f"权利依据 {grant_id} 不存在", "grant_not_found")
            slices = self._rows(
                "SELECT s.* FROM p_slices s JOIN p_slice_grants sg ON sg.slice_id=s.slice_id"
                " WHERE sg.grant_id=? AND s.status='active'",
                (grant_id,),
            )
            for s in slices:
                self.store.append(
                    "SLICE_FROZEN",
                    "corpus_slice",
                    s["slice_id"],
                    {"reason": f"权利依据 {grant_id} 撤回：{reason}", "grant_id": grant_id,
                     "effective_at": effective_dt.isoformat()},
                )
                self.store.append(
                    "RIGHT_WITHDRAWN",
                    "corpus_slice",
                    s["slice_id"],
                    {"right_id": grant_id, "effective_at": effective_dt.isoformat()},
                )
                frozen += 1
                self._freeze_derived_datasets(s["slice_id"], grant_id, reason)
        return frozen

    def _freeze_derived_datasets(self, slice_id: str, grant_id: str, reason: str) -> None:
        datasets = self._rows(
            "SELECT d.* FROM p_datasets d JOIN p_dataset_slices ds ON ds.dataset_id=d.dataset_id"
            " WHERE ds.slice_id=? AND d.status='published'",
            (slice_id,),
        )
        for d in datasets:
            self.store.append(
                "DATASET_FROZEN",
                "derived_dataset",
                d["dataset_id"],
                {"dataset_id": d["dataset_id"], "reason": f"上游切片 {slice_id} 权利撤回：{reason}",
                 "slice_id": slice_id, "grant_id": grant_id},
            )
            # 已发布版本保留当时依据，并生成处置义务（同一数据集+同一来源只生成一次）
            basis = json.loads(d["published_basis"] or "{}")
            ref_key = f"purge:{slice_id}:{grant_id}"
            obligation_id = _deterministic_id("obl", f"{d['dataset_id']}:{ref_key}")
            self.store.append(
                "OBLIGATION_RAISED",
                "obligation",
                obligation_id,
                {
                    "obligation_id": obligation_id,
                    "dataset_id": d["dataset_id"],
                    "ref_key": ref_key,
                    "kind": "purge_or_notice",
                    "detail": {
                        "slice_id": slice_id,
                        "grant_id": grant_id,
                        "reason": reason,
                        "published_basis": basis,
                        "action": "从已发布数据集中清除受影响切片，无法清除时向接收方发出停止使用通知",
                    },
                },
            )

    # ── 到期扫描 ────────────────────────────────────────────────
    def expire_grants(self, now: datetime | None = None) -> list[str]:
        """把保留期届满的权利依据置为过期并冻结其切片（重启后可重复安全执行）。"""
        now = to_utc(now) if now else self.now()
        expired_ids = []
        rows = self._rows("SELECT * FROM p_grants WHERE status='active'", ())
        for g in rows:
            if to_utc(g["retain_until"]) <= now:
                with self.store.transaction():
                    # 双重检查，避免与并发放行交错
                    fresh = self._row("SELECT status FROM p_grants WHERE grant_id=?", (g["grant_id"],))
                    if fresh["status"] != "active":
                        continue
                    self.store.append(
                        "GRANT_EXPIRED",
                        "rights_grant",
                        g["grant_id"],
                        {"grant_id": g["grant_id"], "retain_until": g["retain_until"]},
                    )
                    expired_ids.append(g["grant_id"])
                self.propagate_withdrawal(g["grant_id"], "保留期届满", now)
                job = self._row(
                    "SELECT id FROM jobs WHERE kind='grant_expiry' AND ref_key=?", (f"grant:{g['grant_id']}",)
                )
                if job is not None:
                    self.store.finish_job(job["id"])
        return expired_ids

    def schedule_grant_expiries(self) -> int:
        """为全部活跃权利依据登记到期扫描作业（启动时补齐，重启续跑）。"""
        count = 0
        for g in self._rows("SELECT grant_id, retain_until FROM p_grants WHERE status='active'"):
            self.store.enqueue_job(
                _new_id("job"), "grant_expiry", f"grant:{g['grant_id']}",
                {"grant_id": g["grant_id"]}, to_utc(g["retain_until"]),
            )
            count += 1
        return count

    # ── 使用申请 ────────────────────────────────────────────────
    def submit_request(
        self,
        applicant_org_id: str,
        purpose: str,
        territory: str,
        language: str,
        product: str,
        slice_ids: list[str],
        materials: dict,
        request_id: str | None = None,
    ) -> dict:
        self._require_org_role(applicant_org_id, "applicant")
        if not slice_ids:
            raise ValidationError("申请至少包含一个切片")
        for sid in dict.fromkeys(slice_ids):
            self._get_slice(sid)
        if not isinstance(materials, dict) or not materials:
            raise ValidationError("申请材料不能为空")
        request_id = request_id or _new_id("req")
        materials_hash = canonical_hash(
            {"purpose": purpose, "territory": territory, "language": language, "product": product,
             "slice_ids": sorted(slice_ids), "materials": materials}
        )

        existing = self._row("SELECT * FROM p_requests WHERE request_id=?", (request_id,))
        if existing is not None and existing["materials_hash"] == materials_hash:
            # 同编号同材料：安全重传，不产生新事件
            return {"deduplicated": True, "request_id": request_id, "version_hint": existing["created_seq"],
                    "materials_hash": materials_hash}

        with self.store.transaction():
            if existing is not None and existing["materials_hash"] != materials_hash:
                # 材料改变：旧批准/旧拒绝一律不得沿用，先作废旧有效决定，申请随新事件重开
                active = self._row(
                    "SELECT decision_id FROM active_decisions WHERE request_id=?", (request_id,)
                )
                if active is not None:
                    self.store.append(
                        "DECISION_SUPERSEDED",
                        "release_decision",
                        active["decision_id"],
                        {"decision_id": active["decision_id"], "request_id": request_id,
                         "reason": "申请材料改变，旧决定不得沿用"},
                    )
            event = self.store.append(
                "USE_REQUESTED",
                "use_request",
                request_id,
                {
                    "applicant_org_id": applicant_org_id,
                    "purpose": purpose,
                    "territory": territory,
                    "language": language,
                    "product": product,
                    "slice_ids": sorted(slice_ids),
                    "materials": materials,
                    "materials_hash": materials_hash,
                    "amends": existing["materials_hash"] if existing is not None else None,
                },
            )
        return event

    # ── 放行决定 ────────────────────────────────────────────────
    def decide_request(
        self,
        request_id: str,
        decider_org_id: str,
        decider_user: str,
        verdict: str,
        reason: str | None = None,
        at: datetime | None = None,
    ) -> dict:
        """对申请作最终放行/拒绝。跨机构并发放行只产生一个有效决定。"""
        self._require_org_role(decider_org_id, "approver")
        req = self._row("SELECT * FROM p_requests WHERE request_id=?", (request_id,))
        if req is None:
            raise NotFoundError(f"申请 {request_id} 不存在", "request_not_found")
        if verdict not in ("approved", "denied"):
            raise ValidationError("决定必须是 approved 或 denied")

        # 关键并发守卫：检查-评估-写入在同一立即事务内，单写者锁使并发放行收敛
        with self.store.transaction():
            active = self.store.fetchone(
                "SELECT decision_id FROM active_decisions WHERE request_id=?", (request_id,)
            )
            if active is not None:
                raise ConflictError(
                    f"申请 {request_id} 已存在有效决定 {active['decision_id']}，跨机构并发放行只能产生一个有效决定",
                    "single_active_decision",
                )
            now = to_utc(at) if at else self.now()
            slice_ids = json.loads(req["slice_ids"])
            evaluation = self._evaluate_slices(slice_ids, req["territory"], req["language"], req["product"], now)

            if verdict == "approved" and not evaluation["usable"]:
                raise ConflictError(
                    "存在不满足放行条件的切片，不能批准：" + "；".join(evaluation["blockers"]),
                    "approval_precondition_failed",
                    details={"blockers": evaluation["blockers"]},
                )

            decision_id = _new_id("dec")
            valid_until = evaluation.get("earliest_retain_until")
            payload = {
                "decision_id": decision_id,
                "request_id": request_id,
                "request_materials_hash": req["materials_hash"],
                "territory": req["territory"],
                "language": req["language"],
                "product": req["product"],
                "valid_from": now.isoformat(),
                "valid_until": valid_until,
                "decider_user": decider_user,
                "decider_org": decider_org_id,
                "basis": {
                    "grant_ids": sorted(evaluation["grant_ids"]),
                    "reviews": {sid: ["desensitize", "fact_check"] for sid in slice_ids},
                    "slice_versions": evaluation["slice_versions"],
                    "evaluated_at": now.isoformat(),
                },
            }
            if verdict == "denied":
                payload["reason"] = reason or "未通过人工放行审查"
                event_type = "USE_DENIED"
            else:
                event_type = "USE_APPROVED"
            return self.store.append(event_type, "release_decision", decision_id, payload)

    def _evaluate_slices(self, slice_ids, territory, language, product, now):
        blockers: list[str] = []
        grant_ids: set[str] = set()
        earliest = None
        slice_versions: dict[str, dict] = {}
        for sid in slice_ids:
            s = self._get_slice(sid)
            slice_versions[sid] = {"asset_id": s["asset_id"], "asset_version": s["asset_version"],
                                   "content_hash": s["content_hash"]}
            if s["status"] in SLICE_BLOCKED_STATUSES:
                blockers.append(f"切片 {sid} 已冻结")
                continue
            flags = json.loads(s["flags"])
            embargo = flags.get("embargo_until")
            if embargo and to_utc(embargo) > now:
                blockers.append(f"切片 {sid} 尚在禁发期（至 {embargo}）")
            missing = self._reviews_pass(sid)
            if missing:
                blockers.append(f"切片 {sid} 未完成双职责复核: {sorted(missing)}")
            covering = self._rows(
                "SELECT g.* FROM p_slice_grants sg JOIN p_grants g ON g.grant_id=sg.grant_id"
                " WHERE sg.slice_id=?",
                (sid,),
            )
            slice_covered = False
            for g in covering:
                # 撤回已登记但尚未到生效时刻：该时刻之前权利仍有效
                if g["status"] == "withdrawn":
                    if not g["withdrawn_at"] or to_utc(g["withdrawn_at"]) <= now:
                        continue
                elif g["status"] != "active":
                    continue
                if to_utc(g["valid_from"]) > now:
                    continue
                if to_utc(g["retain_until"]) <= now:
                    continue
                if territory not in json.loads(g["territories"]):
                    continue
                if language not in json.loads(g["languages"]):
                    continue
                if product not in json.loads(g["products"]):
                    continue
                slice_covered = True
                grant_ids.add(g["grant_id"])
                # 有效期同时受保留期与未来撤回生效时刻约束
                until = to_utc(g["retain_until"])
                if g["status"] == "withdrawn" and g["withdrawn_at"]:
                    w_at = to_utc(g["withdrawn_at"])
                    until = w_at if w_at < until else until
                earliest = until if earliest is None or until < earliest else earliest
            if not slice_covered:
                blockers.append(f"切片 {sid} 缺少覆盖地区={territory}、语言={language}、产品={product} 的有效权利依据")
        return {
            "usable": not blockers,
            "blockers": blockers,
            "grant_ids": grant_ids,
            "earliest_retain_until": earliest.isoformat() if earliest else None,
            "slice_versions": slice_versions,
        }

    # ── 派生数据集 ──────────────────────────────────────────────
    def create_dataset(self, dataset_id: str | None, creator_org_id: str, product: str, slice_ids: list[str]) -> dict:
        self._require_org_role(creator_org_id, "applicant")
        if not slice_ids:
            raise ValidationError("数据集至少包含一个切片")
        for sid in dict.fromkeys(slice_ids):
            self._get_slice(sid)
        dataset_id = dataset_id or _new_id("ds")
        return self.store.append(
            "DATASET_CREATED",
            "derived_dataset",
            dataset_id,
            {"dataset_id": dataset_id, "creator_org_id": creator_org_id, "product": product,
             "slice_ids": sorted(dict.fromkeys(slice_ids))},
        )

    def publish_dataset(self, dataset_id: str, decision_ids: list[str], at: datetime | None = None) -> dict:
        dataset = self._get_dataset(dataset_id)
        if not decision_ids:
            raise ValidationError("发布必须携带当时生效的放行决定清单")
        if dataset["status"] == "frozen":
            raise ConflictError(f"数据集 {dataset_id} 已冻结，不得发布", "dataset_frozen")
        now = to_utc(at) if at else self.now()
        slice_ids = {
            r["slice_id"] for r in self._rows(
                "SELECT slice_id FROM p_dataset_slices WHERE dataset_id=?", (dataset_id,))
        }

        with self.store.transaction():
            basis_decisions: list[str] = []
            covered: set[str] = set()
            for did in dict.fromkeys(decision_ids):
                d = self._row("SELECT * FROM p_decisions WHERE decision_id=? AND active=1", (did,))
                if d is None:
                    raise ConflictError(f"决定 {did} 不存在或已失效，不能作为发布依据", "decision_inactive")
                req = self._row("SELECT * FROM p_requests WHERE request_id=?", (d["request_id"],))
                # 材料改变后旧批准不得沿用：决定指纹必须与申请最新指纹一致
                if d["materials_hash"] != req["materials_hash"]:
                    raise ConflictError(
                        f"决定 {did} 依据的申请材料已改变，旧批准不得用于发布", "decision_materials_stale"
                    )
                req_slices = set(json.loads(req["slice_ids"]))
                if not req_slices & slice_ids:
                    raise ConflictError(f"决定 {did} 与数据集切片无关", "decision_unrelated")
                if d["product"] != dataset["product"]:
                    raise ConflictError(f"决定 {did} 的产品与数据集产品不一致", "decision_product_mismatch")
                # 发布时刻实时复核：旧批准有效不等于发布时权利仍在（撤回/到期/冻结立即失效）
                live = self._evaluate_slices(
                    sorted(req_slices & slice_ids),
                    req["territory"], req["language"], req["product"], now,
                )
                if not live["usable"]:
                    raise ConflictError(
                        f"决定 {did} 覆盖的切片在发布时刻已不满足地区/语言/产品的权利条件："
                        + "；".join(live["blockers"]),
                        "rights_no_longer_held",
                    )
                covered |= req_slices & slice_ids
                basis_decisions.append(did)
            missing = slice_ids - covered
            if missing:
                raise ConflictError(f"切片缺少发布时的有效放行决定: {sorted(missing)}", "decision_coverage_incomplete")
            slice_versions = {
                sid: {"asset_id": r["asset_id"], "asset_version": r["asset_version"],
                      "content_hash": r["content_hash"]}
                for sid in sorted(slice_ids)
                for r in [self._row(
                    "SELECT asset_id, asset_version, content_hash FROM p_slices WHERE slice_id=?", (sid,)
                )]
            }
            basis = {
                "decision_ids": sorted(basis_decisions),
                "published_at": self.now().isoformat(),
                "material_prints": {
                    did: self._row(
                        "SELECT materials_hash FROM p_decisions WHERE decision_id=?", (did,)
                    )["materials_hash"]
                    for did in basis_decisions
                },
                "slice_versions": slice_versions,
            }
            return self.store.append(
                "DATASET_PUBLISHED",
                "derived_dataset",
                dataset_id,
                {"dataset_id": dataset_id, "basis": basis},
            )

    def fulfill_obligation(self, obligation_id: str, note: str | None = None) -> dict:
        row = self._row("SELECT * FROM p_obligations WHERE obligation_id=?", (obligation_id,))
        if row is None:
            raise NotFoundError(f"处置义务 {obligation_id} 不存在", "obligation_not_found")
        return self.store.append(
            "OBLIGATION_FULFILLED",
            "obligation",
            obligation_id,
            {"obligation_id": obligation_id, "note": note or "已履行"},
        )

    @staticmethod
    def _parse_date(value: str) -> datetime:
        """纯日期按海南时区当日 23:59:59 判定（该日内可否使用）；完整时间戳按其时区换算 UTC。"""
        if "T" in value or " " in value:
            return to_utc(value)
        parsed = datetime.strptime(value, "%Y-%m-%d")
        return parsed.replace(hour=23, minute=59, second=59, tzinfo=HAINAN_TZ).astimezone(timezone.utc)

    # ── 资格判定（HTTP 核心查询）────────────────────────────────
    def eligibility(self, slice_id: str, date: str, territory: str, language: str, product: str) -> dict:
        """判断一段语料在指定日期能否用于某地区、语言和产品。"""
        self._get_slice(slice_id)
        when = self._parse_date(date)
        ev = self._evaluate_slices([slice_id], territory, language, product, when)
        grants_detail = []
        for r in self._rows(
            "SELECT g.* FROM p_slice_grants sg JOIN p_grants g ON g.grant_id=sg.grant_id WHERE sg.slice_id=?",
            (slice_id,),
        ):
            grants_detail.append(
                {
                    "grant_id": r["grant_id"],
                    "status": r["status"],
                    "granter_org": r["granter_org"],
                    "territories": json.loads(r["territories"]),
                    "languages": json.loads(r["languages"]),
                    "products": json.loads(r["products"]),
                    "valid_from": r["valid_from"],
                    "retain_until": r["retain_until"],
                    "reason": r["reason"],
                }
            )
        return {
            "slice_id": slice_id,
            "date": when.astimezone(HAINAN_TZ).date().isoformat(),
            "territory": territory,
            "language": language,
            "product": product,
            "usable": ev["usable"],
            "blockers": ev["blockers"],
            "effective_grants": sorted(ev["grant_ids"]),
            "rights": grants_detail,
        }

    # ── 血缘反查 ────────────────────────────────────────────────
    def dataset_lineage(self, dataset_id: str) -> dict:
        dataset = self._get_dataset(dataset_id)
        slice_rows = self._rows(
            "SELECT s.* FROM p_dataset_slices ds JOIN p_slices s ON s.slice_id=ds.slice_id"
            " WHERE ds.dataset_id=? ORDER BY s.ordinal",
            (dataset_id,),
        )
        slices = []
        for s in slice_rows:
            grants = [
                r["grant_id"]
                for r in self._rows("SELECT grant_id FROM p_slice_grants WHERE slice_id=?", (s["slice_id"],))
            ]
            reviews = [
                {"duty": r["duty"], "reviewer_id": r["reviewer_id"], "passed": bool(r["passed"]), "note": r["note"]}
                for r in self._rows("SELECT * FROM p_reviews WHERE slice_id=?", (s["slice_id"],))
            ]
            av = self._row(
                "SELECT * FROM p_asset_versions WHERE asset_id=? AND version=?",
                (s["asset_id"], s["asset_version"]),
            )
            slices.append(
                {
                    "slice_id": s["slice_id"],
                    "status": s["status"],
                    "ordinal": s["ordinal"],
                    "content_hash": s["content_hash"],
                    "asset_id": s["asset_id"],
                    "asset_version": s["asset_version"],
                    "source_id": av["source_id"] if av else None,
                    "contributor_org": av["contributor"] if av else None,
                    "batch_id": av["batch_id"] if av else None,
                    "rights": grants,
                    "reviews": reviews,
                    "processing": self._processing_records(s["slice_id"]),
                }
            )
        decisions = []
        for r in self._rows(
            "SELECT d.* FROM p_dataset_decisions dd JOIN p_decisions d ON d.decision_id=dd.decision_id"
            " WHERE dd.dataset_id=? ORDER BY d.seq",
            (dataset_id,),
        ):
            decisions.append(
                {
                    "decision_id": r["decision_id"],
                    "request_id": r["request_id"],
                    "verdict": r["verdict"],
                    "active": bool(r["active"]),
                    "decider_user": r["decider_user"],
                    "decider_org": r["decider_org"],
                    "valid_from": r["valid_from"],
                    "valid_until": r["valid_until"],
                    "materials_hash": r["materials_hash"],
                    "basis": json.loads(r["basis"]),
                    "reason": r["reason"],
                }
            )
        obligations = [
            {
                "obligation_id": r["obligation_id"],
                "kind": r["kind"],
                "status": r["status"],
                "detail": json.loads(r["detail"]),
            }
            for r in self._rows("SELECT * FROM p_obligations WHERE dataset_id=? ORDER BY created_seq", (dataset_id,))
        ]
        return {
            "dataset_id": dataset_id,
            "product": dataset["product"],
            "creator_org": dataset["creator_org"],
            "status": dataset["status"],
            "published_basis": json.loads(dataset["published_basis"]) if dataset["published_basis"] else None,
            "slices": slices,
            "decisions": decisions,
            "pending_obligations": [o for o in obligations if o["status"] == "open"],
            "obligations": obligations,
        }

    def list_obligations(self, status: str | None = None) -> list[dict]:
        sql = "SELECT * FROM p_obligations"
        params: tuple = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY created_seq"
        return [
            {"obligation_id": r["obligation_id"], "dataset_id": r["dataset_id"], "kind": r["kind"],
             "status": r["status"], "detail": json.loads(r["detail"]), "ref_key": r["ref_key"]}
            for r in self._rows(sql, params)
        ]

    def _processing_records(self, slice_id: str) -> list[dict]:
        rows = self._rows(
            "SELECT event_type, occurred_at, payload FROM events WHERE aggregate_id=? ORDER BY seq",
            (slice_id,),
        )
        return [
            {"event_type": r["event_type"], "occurred_at": r["occurred_at"], "payload": json.loads(r["payload"])}
            for r in rows
        ]

    # ── 管理面 ──────────────────────────────────────────────────
    def list_assets(self) -> list[dict]:
        return [dict(r) for r in self._rows("SELECT * FROM p_assets ORDER BY asset_id")]

    def list_datasets(self) -> list[dict]:
        return [dict(r) for r in self._rows("SELECT dataset_id,product,creator_org,status FROM p_datasets ORDER BY dataset_id")]

    def list_requests(self) -> list[dict]:
        return [dict(r) for r in self._rows("SELECT * FROM p_requests ORDER BY created_seq")]

    def asset_detail(self, asset_id: str) -> dict:
        self._get_asset(asset_id)
        versions = [dict(r) for r in self._rows(
            "SELECT asset_id,version,contributor,source_id,batch_id,content_hash,grant_print,flags,status,seq"
            " FROM p_asset_versions WHERE asset_id=? ORDER BY version", (asset_id,))]
        for v in versions:
            v["flags"] = json.loads(v["flags"])
        return {"asset_id": asset_id, "versions": versions}


def _other_duty(duty: str) -> str:
    return "fact_check" if duty == "desensitize" else "desensitize"
