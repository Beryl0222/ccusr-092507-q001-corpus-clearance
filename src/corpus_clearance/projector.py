"""读模型投影：事件 → 查询表。

每个事件在写入的同一事务内投影一次；重启时按事件顺序全量重放。
投影只做机械更新，业务判断在服务层。
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any


def _jloads(raw: Any, default: Any) -> Any:
    if raw is None:
        return default
    if isinstance(raw, (dict, list)):
        return raw
    return json.loads(raw)


def project(conn: sqlite3.Connection, event: dict[str, Any]) -> None:
    et = event["event_type"]
    seq = event["seq"]
    p = event["payload"]
    handler = _HANDLERS.get(et)
    if handler is not None:
        handler(conn, event, p, seq)


def _on_org_registered(conn, e, p, seq):
    conn.execute(
        "INSERT INTO p_orgs(org_id,name,roles,seq) VALUES(?,?,?,?)"
        " ON CONFLICT(org_id) DO UPDATE SET name=excluded.name, roles=excluded.roles, seq=excluded.seq",
        (p["org_id"], p["name"], json.dumps(p.get("roles", []), ensure_ascii=False), seq),
    )


def _on_asset_ingested(conn, e, p, seq):
    asset_id = e["aggregate_id"]
    version = e["version"]
    quarantined = bool(p.get("quarantined"))
    conn.execute(
        "INSERT INTO p_asset_versions(asset_id,version,contributor,source_id,batch_id,content_hash,"
        "grant_print,body_ref,flags,status,seq) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (
            asset_id,
            version,
            p.get("contributor_org_id", ""),
            p["source_id"],
            p.get("batch_id", ""),
            p["content_hash"],
            p.get("grant_print", ""),
            p.get("body_ref"),
            json.dumps(p.get("flags", {}), ensure_ascii=False, sort_keys=True),
            "quarantined" if quarantined else "ingested",
            seq,
        ),
    )
    row = conn.execute("SELECT current_version,status FROM p_assets WHERE asset_id=?", (asset_id,)).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO p_assets(asset_id,current_version,status,disputed,updated_seq) VALUES(?,?,?,0,?)",
            (asset_id, version, "ingested", seq),
        )
    elif not quarantined:
        # 新版正式接收，争议隔离标记随之清除
        conn.execute(
            "UPDATE p_assets SET current_version=?, status='ingested', disputed=0, updated_seq=? WHERE asset_id=?",
            (version, seq, asset_id),
        )
    # 重建摄取幂等索引
    conn.execute(
        "INSERT OR IGNORE INTO ingest_dedup(batch_id,content_hash,asset_id,version,quarantined,seq)"
        " VALUES(?,?,?,?,?,?)",
        (
            p.get("batch_id", ""),
            p["content_hash"],
            asset_id,
            version,
            1 if quarantined else 0,
            seq,
        ),
    )


def _on_asset_quarantined(conn, e, p, seq):
    asset_id = e["aggregate_id"]
    conn.execute(
        "UPDATE p_asset_versions SET status='quarantined', seq=? WHERE asset_id=? AND version=?",
        (seq, asset_id, p["incoming_version"]),
    )
    # 只置争议标记：现行版本继续可用，争议版本单独隔离
    conn.execute("UPDATE p_assets SET disputed=1, updated_seq=? WHERE asset_id=?", (seq, asset_id))


def _on_asset_dispute_resolved(conn, e, p, seq):
    asset_id = e["aggregate_id"]
    version = p["resolved_version"]
    if p["resolution"] == "promote":
        conn.execute(
            "UPDATE p_asset_versions SET status='ingested', seq=? WHERE asset_id=? AND version=?",
            (seq, asset_id, version),
        )
        conn.execute(
            "UPDATE p_assets SET current_version=?, status='ingested', disputed=0, updated_seq=? WHERE asset_id=?",
            (version, seq, asset_id),
        )
    else:  # reject
        conn.execute(
            "UPDATE p_asset_versions SET status='rejected', seq=? WHERE asset_id=? AND version=?",
            (seq, asset_id, version),
        )
        remaining = conn.execute(
            "SELECT COUNT(*) c FROM p_asset_versions WHERE asset_id=? AND status='quarantined'",
            (asset_id,),
        ).fetchone()["c"]
        if remaining == 0:
            conn.execute(
                "UPDATE p_assets SET disputed=0, updated_seq=? WHERE asset_id=?", (seq, asset_id)
            )


def _on_slice_cut(conn, e, p, seq):
    conn.execute(
        "INSERT INTO p_slices(slice_id,asset_id,asset_version,ordinal,content_hash,text,flags,status,updated_seq)"
        " VALUES(?,?,?,?,?,?,?, 'active',?)",
        (
            p["slice_id"],
            e["aggregate_id"] if p.get("asset_id") is None else p["asset_id"],
            p.get("asset_version", 1),
            p.get("ordinal", 0),
            p["content_hash"],
            p.get("text"),
            json.dumps(p.get("flags", {}), ensure_ascii=False, sort_keys=True),
            seq,
        ),
    )
    for grant_id in p.get("grants", []):
        conn.execute("INSERT OR IGNORE INTO p_slice_grants(slice_id,grant_id) VALUES(?,?)", (p["slice_id"], grant_id))


def _on_slice_reviewed(conn, e, p, seq):
    conn.execute(
        "INSERT INTO p_reviews(slice_id,duty,reviewer_id,passed,note,seq) VALUES(?,?,?,?,?,?)"
        " ON CONFLICT(slice_id,duty) DO UPDATE SET reviewer_id=excluded.reviewer_id, passed=excluded.passed,"
        " note=excluded.note, seq=excluded.seq",
        (e["aggregate_id"], p["duty"], p["reviewer_id"], 1 if p["passed"] else 0, p.get("note"), seq),
    )
    conn.execute("UPDATE p_slices SET updated_seq=? WHERE slice_id=?", (seq, e["aggregate_id"]))


def _on_grant_recorded(conn, e, p, seq):
    conn.execute(
        "INSERT INTO p_grants(grant_id,asset_id,asset_version,granter_org,territories,languages,products,"
        "valid_from,retain_until,status,updated_seq) VALUES(?,?,?,?,?,?,?,?,?, 'active',?)"
        " ON CONFLICT(grant_id) DO UPDATE SET territories=excluded.territories, languages=excluded.languages,"
        " products=excluded.products, retain_until=excluded.retain_until, updated_seq=excluded.updated_seq",
        (
            p["grant_id"],
            p.get("asset_id", e["aggregate_id"]),
            p.get("asset_version", 1),
            p["granter_org_id"],
            json.dumps(p["territories"], ensure_ascii=False, sort_keys=True),
            json.dumps(p["languages"], ensure_ascii=False, sort_keys=True),
            json.dumps(p["products"], ensure_ascii=False, sort_keys=True),
            p.get("valid_from", e["occurred_at"]),
            p["retain_until"],
            seq,
        ),
    )


def _on_grant_expired(conn, e, p, seq):
    conn.execute(
        "UPDATE p_grants SET status='expired', reason=?, updated_seq=? WHERE grant_id=?",
        (p.get("reason", "保留期届满"), seq, p["grant_id"]),
    )


def _on_grant_withdrawn(conn, e, p, seq):
    conn.execute(
        "UPDATE p_grants SET status='withdrawn', withdrawn_at=?, reason=?, updated_seq=? WHERE grant_id=?",
        (p["effective_at"], p.get("reason", "权利撤回"), seq, p["grant_id"]),
    )


def _on_slice_frozen(conn, e, p, seq):
    conn.execute(
        "UPDATE p_slices SET status='frozen', updated_seq=? WHERE slice_id=?",
        (seq, e["aggregate_id"]),
    )


def _on_use_requested(conn, e, p, seq):
    conn.execute(
        "INSERT INTO p_requests(request_id,applicant_org,purpose,territory,language,product,slice_ids,"
        "materials_hash,status,created_seq) VALUES(?,?,?,?,?,?,?,?,'open',?)"
        " ON CONFLICT(request_id) DO UPDATE SET applicant_org=excluded.applicant_org, purpose=excluded.purpose,"
        " territory=excluded.territory, language=excluded.language, product=excluded.product,"
        " slice_ids=excluded.slice_ids, materials_hash=excluded.materials_hash, status='open',"
        " created_seq=excluded.created_seq",
        (
            e["aggregate_id"],
            p.get("applicant_org_id", ""),
            p["purpose"],
            p["territory"],
            p.get("language", ""),
            p.get("product", ""),
            json.dumps(p.get("slice_ids", []), ensure_ascii=False, sort_keys=True),
            p["materials_hash"],
            seq,
        ),
    )


def _on_use_decided(conn, e, p, seq, verdict):
    conn.execute(
        "INSERT INTO p_decisions(decision_id,request_id,verdict,active,territory,language,product,valid_from,"
        "valid_until,basis,materials_hash,decider_user,decider_org,reason,seq)"
        " VALUES(?,?,?,1,?,?,?,?,?,?,?,?,?,?,?)",
        (
            p["decision_id"],
            p["request_id"],
            verdict,
            p["territory"],
            p["language"],
            p["product"],
            p["valid_from"],
            p.get("valid_until"),
            json.dumps(p.get("basis", {}), ensure_ascii=False, sort_keys=True),
            p["request_materials_hash"],
            p["decider_user"],
            p["decider_org"],
            p.get("reason"),
            seq,
        ),
    )
    conn.execute(
        "UPDATE p_requests SET status=? WHERE request_id=?",
        ("approved" if verdict == "approved" else "denied", p["request_id"]),
    )
    # 同一申请始终只指向最新决定（同事务内服务层已拒绝重复决定）
    conn.execute("DELETE FROM active_decisions WHERE request_id=?", (p["request_id"],))
    conn.execute(
        "INSERT INTO active_decisions(request_id,decision_id,seq) VALUES(?,?,?)",
        (p["request_id"], p["decision_id"], seq),
    )


def _on_decision_superseded(conn, e, p, seq):
    conn.execute("UPDATE p_decisions SET active=0 WHERE decision_id=?", (p["decision_id"],))
    conn.execute(
        "UPDATE p_requests SET status='superseded' WHERE request_id=?", (p["request_id"],)
    )
    conn.execute("DELETE FROM active_decisions WHERE request_id=?", (p["request_id"],))


def _on_dataset_created(conn, e, p, seq):
    conn.execute(
        "INSERT INTO p_datasets(dataset_id,product,creator_org,status,created_seq,updated_seq)"
        " VALUES(?,?,?, 'created',?,?)",
        (p["dataset_id"], p["product"], p.get("creator_org_id", ""), seq, seq),
    )
    for slice_id in p["slice_ids"]:
        conn.execute(
            "INSERT OR IGNORE INTO p_dataset_slices(dataset_id,slice_id) VALUES(?,?)",
            (p["dataset_id"], slice_id),
        )


def _on_dataset_published(conn, e, p, seq):
    conn.execute(
        "UPDATE p_datasets SET status='published', published_basis=?, updated_seq=? WHERE dataset_id=?",
        (json.dumps(p["basis"], ensure_ascii=False, sort_keys=True), seq, p["dataset_id"]),
    )
    for decision_id in p["basis"].get("decision_ids", []):
        conn.execute(
            "INSERT OR IGNORE INTO p_dataset_decisions(dataset_id,decision_id) VALUES(?,?)",
            (p["dataset_id"], decision_id),
        )


def _on_dataset_frozen(conn, e, p, seq):
    row = conn.execute("SELECT status FROM p_datasets WHERE dataset_id=?", (p["dataset_id"],)).fetchone()
    if row is not None and row["status"] != "frozen":
        conn.execute(
            "UPDATE p_datasets SET status='frozen', updated_seq=? WHERE dataset_id=?", (seq, p["dataset_id"])
        )


def _on_obligation_raised(conn, e, p, seq):
    conn.execute(
        "INSERT OR IGNORE INTO p_obligations(obligation_id,dataset_id,ref_key,kind,detail,status,created_seq)"
        " VALUES(?,?,?,?,?, 'open',?)",
        (p["obligation_id"], p["dataset_id"], p["ref_key"], p["kind"], json.dumps(p.get("detail", {}), ensure_ascii=False, sort_keys=True), seq),
    )


def _on_obligation_fulfilled(conn, e, p, seq):
    conn.execute(
        "UPDATE p_obligations SET status='fulfilled', fulfilled_seq=? WHERE obligation_id=? AND status='open'",
        (seq, p["obligation_id"]),
    )


_HANDLERS = {
    "ORG_REGISTERED": _on_org_registered,
    "ASSET_INGESTED": _on_asset_ingested,
    "ASSET_QUARANTINED": _on_asset_quarantined,
    "ASSET_DISPUTE_RESOLVED": _on_asset_dispute_resolved,
    "SLICE_CUT": _on_slice_cut,
    "SLICE_REVIEWED": _on_slice_reviewed,
    "GRANT_RECORDED": _on_grant_recorded,
    "GRANT_EXPIRED": _on_grant_expired,
    "GRANT_WITHDRAWN": _on_grant_withdrawn,
    "SLICE_FROZEN": _on_slice_frozen,
    "USE_REQUESTED": lambda c, e, p, s: _on_use_requested(c, e, p, s),
    "USE_APPROVED": lambda c, e, p, s: _on_use_decided(c, e, p, s, "approved"),
    "USE_DENIED": lambda c, e, p, s: _on_use_decided(c, e, p, s, "denied"),
    "DECISION_SUPERSEDED": _on_decision_superseded,
    "DATASET_CREATED": _on_dataset_created,
    "DATASET_PUBLISHED": _on_dataset_published,
    "DATASET_FROZEN": _on_dataset_frozen,
    "OBLIGATION_RAISED": _on_obligation_raised,
    "OBLIGATION_FULFILLED": _on_obligation_fulfilled,
}
