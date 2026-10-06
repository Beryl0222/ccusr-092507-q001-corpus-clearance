"""事件流上的读模型投影。

每个 ``apply`` 函数处理一个事件类型，只做状态演进，不含业务判定——
业务规则集中在 ``services``。所有表均可由事件流 ``reset`` 后重建。
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

_DDL = """
CREATE TABLE IF NOT EXISTS p_orgs (
    org_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS p_assets (
    asset_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    collector_id TEXT NOT NULL,
    title TEXT,
    content_hash TEXT NOT NULL,
    authorization_fingerprint TEXT,
    status TEXT NOT NULL,
    dispute_reason TEXT,
    stored_hash TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_p_assets_batch_hash ON p_assets (batch_id, content_hash);

CREATE TABLE IF NOT EXISTS p_rights (
    right_id TEXT PRIMARY KEY,
    asset_id TEXT NOT NULL,
    declarer_id TEXT NOT NULL,
    licensor_id TEXT NOT NULL,
    slice_ids TEXT,
    basis_type TEXT NOT NULL,
    territories TEXT NOT NULL,
    languages TEXT NOT NULL,
    purpose_tags TEXT NOT NULL,
    products TEXT NOT NULL,
    embargo_not_before TEXT,
    not_after TEXT,
    notes TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    inactive_reason TEXT,
    withdrawn INTEGER NOT NULL DEFAULT 0,
    withdrawn_at TEXT,
    withdraw_reason TEXT,
    expiry_job_scheduled INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_p_rights_asset ON p_rights (asset_id);

CREATE TABLE IF NOT EXISTS p_slices (
    slice_id TEXT PRIMARY KEY,
    asset_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    sensitivity TEXT NOT NULL,
    frozen INTEGER NOT NULL DEFAULT 0,
    freeze_reason TEXT,
    frozen_at TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (asset_id, ordinal)
);

CREATE TABLE IF NOT EXISTS p_slice_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slice_id TEXT NOT NULL,
    review_kind TEXT NOT NULL,
    reviewer_id TEXT NOT NULL,
    passed INTEGER NOT NULL,
    notes TEXT,
    reviewed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_p_slice_reviews_lookup
    ON p_slice_reviews (slice_id, review_kind, id);

CREATE TABLE IF NOT EXISTS p_requests (
    request_id TEXT PRIMARY KEY,
    applicant_id TEXT NOT NULL,
    purpose TEXT NOT NULL,
    slice_ids TEXT NOT NULL,
    territories TEXT NOT NULL,
    languages TEXT NOT NULL,
    products TEXT NOT NULL,
    materials TEXT NOT NULL,
    materials_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    decision_id TEXT,
    reject_reason TEXT,
    superseded_by TEXT,
    created_at TEXT NOT NULL,
    decided_at TEXT
);

CREATE TABLE IF NOT EXISTS p_request_slices (
    request_id TEXT NOT NULL,
    slice_id TEXT NOT NULL,
    PRIMARY KEY (request_id, slice_id)
);
CREATE INDEX IF NOT EXISTS idx_p_request_slices_slice ON p_request_slices (slice_id);

CREATE TABLE IF NOT EXISTS p_decisions (
    decision_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    decider_id TEXT NOT NULL,
    approved INTEGER NOT NULL,
    effective INTEGER NOT NULL,
    ineffective_reason TEXT,
    grants TEXT NOT NULL,
    valid_from TEXT,
    valid_until TEXT,
    notes TEXT,
    decided_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_p_decisions_request ON p_decisions (request_id);
-- 同一申请在数据库层面至多存在一个当前有效（批准）决定：
CREATE UNIQUE INDEX IF NOT EXISTS ux_p_decisions_one_effective
    ON p_decisions (request_id) WHERE effective = 1 AND approved = 1;

CREATE TABLE IF NOT EXISTS p_datasets (
    dataset_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    product TEXT NOT NULL,
    frozen INTEGER NOT NULL DEFAULT 0,
    freeze_reason TEXT,
    registered_by TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS p_dataset_slices (
    dataset_id TEXT NOT NULL,
    slice_id TEXT NOT NULL,
    added_at TEXT NOT NULL,
    PRIMARY KEY (dataset_id, slice_id)
);
CREATE INDEX IF NOT EXISTS idx_p_dataset_slices_slice ON p_dataset_slices (slice_id);

CREATE TABLE IF NOT EXISTS p_dataset_lineage (
    parent_dataset_id TEXT NOT NULL,
    child_dataset_id TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (parent_dataset_id, child_dataset_id)
);
CREATE INDEX IF NOT EXISTS idx_p_lineage_child ON p_dataset_lineage (child_dataset_id);

CREATE TABLE IF NOT EXISTS p_publications (
    publication_id TEXT PRIMARY KEY,
    slice_id TEXT NOT NULL,
    published_at TEXT NOT NULL,
    basis TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_p_publications_slice ON p_publications (slice_id);

CREATE TABLE IF NOT EXISTS p_dispositions (
    disposition_id TEXT PRIMARY KEY,
    slice_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    origin TEXT NOT NULL,
    description TEXT NOT NULL,
    status TEXT NOT NULL,
    raised_by TEXT,
    fulfilled_by TEXT,
    fulfillment_note TEXT,
    created_at TEXT NOT NULL,
    fulfilled_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_p_dispositions_slice ON p_dispositions (slice_id, status);
"""

_PROJECTION_TABLES = [
    "p_dispositions",
    "p_publications",
    "p_dataset_lineage",
    "p_dataset_slices",
    "p_datasets",
    "p_decisions",
    "p_request_slices",
    "p_requests",
    "p_slice_reviews",
    "p_slices",
    "p_rights",
    "p_assets",
    "p_orgs",
]


def create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(_DDL)


def reset(conn: sqlite3.Connection) -> None:
    for table in _PROJECTION_TABLES:
        conn.execute(f"DELETE FROM {table}")


def _loads(raw: str | None, default: Any) -> Any:
    return json.loads(raw) if raw is not None else default


def apply(conn: sqlite3.Connection, event: dict[str, Any]) -> None:
    handler = _HANDLERS.get(event["event_type"])
    if handler is not None:
        handler(conn, event)


def _on_org_registered(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "INSERT OR IGNORE INTO p_orgs(org_id, name, created_at) VALUES (?, ?, ?)",
        (p["org_id"], p.get("name", p["org_id"]), event["occurred_at"]),
    )


def _on_asset_ingested(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "INSERT INTO p_assets(asset_id, batch_id, collector_id, title, content_hash, "
        "authorization_fingerprint, status, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, 'active', ?)",
        (
            event["aggregate_id"],
            p["batch_id"],
            p["collector_id"],
            p.get("title"),
            p["content_hash"],
            p.get("authorization_fingerprint"),
            event["occurred_at"],
        ),
    )


def _on_asset_disputed(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "UPDATE p_assets SET status = 'quarantined', dispute_reason = ?, stored_hash = ? "
        "WHERE asset_id = ?",
        (p.get("reason"), p.get("existing_content_hash"), event["aggregate_id"]),
    )


def _on_right_declared(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "INSERT INTO p_rights(right_id, asset_id, declarer_id, licensor_id, slice_ids, "
        "basis_type, territories, languages, purpose_tags, products, embargo_not_before, "
        "not_after, notes, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            p["right_id"],
            p["asset_id"],
            p["declarer_id"],
            p.get("licensor_id", p["declarer_id"]),
            json.dumps(p.get("slice_ids"), ensure_ascii=False) if p.get("slice_ids") is not None else None,
            p.get("basis_type", "declared"),
            json.dumps(p.get("territories", ["*"]), ensure_ascii=False),
            json.dumps(p.get("languages", ["*"]), ensure_ascii=False),
            json.dumps(p.get("purpose_tags", ["*"]), ensure_ascii=False),
            json.dumps(p.get("products", ["*"]), ensure_ascii=False),
            p.get("embargo_not_before"),
            p.get("not_after"),
            p.get("notes"),
            event["occurred_at"],
        ),
    )


def _on_slice_cut(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "INSERT INTO p_slices(slice_id, asset_id, ordinal, content_hash, sensitivity, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            event["aggregate_id"],
            p["asset_id"],
            p["ordinal"],
            p["content_hash"],
            json.dumps(p.get("sensitivity", {}), ensure_ascii=False),
            event["occurred_at"],
        ),
    )


def _on_slice_reviewed(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "INSERT INTO p_slice_reviews(slice_id, review_kind, reviewer_id, passed, notes, reviewed_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            event["aggregate_id"],
            p["review_kind"],
            p["reviewer_id"],
            1 if p["passed"] else 0,
            p.get("notes"),
            event["occurred_at"],
        ),
    )


def _on_use_requested(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "INSERT INTO p_requests(request_id, applicant_id, purpose, slice_ids, territories, "
        "languages, products, materials, materials_fingerprint, status, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)",
        (
            event["aggregate_id"],
            p["applicant_id"],
            p["purpose"],
            json.dumps(p["slice_ids"], ensure_ascii=False),
            json.dumps(p["territories"], ensure_ascii=False),
            json.dumps(p["languages"], ensure_ascii=False),
            json.dumps(p["products"], ensure_ascii=False),
            json.dumps(p.get("materials", {}), ensure_ascii=False),
            p["materials_fingerprint"],
            event["occurred_at"],
        ),
    )
    for slice_id in p["slice_ids"]:
        conn.execute(
            "INSERT OR IGNORE INTO p_request_slices(request_id, slice_id) VALUES (?, ?)",
            (event["aggregate_id"], slice_id),
        )


def _on_use_approved(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "INSERT INTO p_decisions(decision_id, request_id, decider_id, approved, effective, "
        "grants, valid_from, valid_until, notes, decided_at) "
        "VALUES (?, ?, ?, 1, 1, ?, ?, ?, ?, ?)",
        (
            event["aggregate_id"],
            p["request_id"],
            p["decider_id"],
            json.dumps(p["grants"], ensure_ascii=False),
            p.get("valid_from"),
            p.get("valid_until"),
            p.get("notes"),
            event["occurred_at"],
        ),
    )
    conn.execute(
        "UPDATE p_requests SET status = 'approved', decision_id = ?, decided_at = ? "
        "WHERE request_id = ?",
        (event["aggregate_id"], event["occurred_at"], p["request_id"]),
    )


def _on_use_rejected(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "INSERT INTO p_decisions(decision_id, request_id, decider_id, approved, effective, "
        "grants, notes, decided_at) VALUES (?, ?, ?, 0, 0, '[]', ?, ?)",
        (event["aggregate_id"], p["request_id"], p["decider_id"], p.get("reason"),
         event["occurred_at"]),
    )
    conn.execute(
        "UPDATE p_requests SET status = 'rejected', decision_id = ?, decided_at = ?, "
        "reject_reason = ? WHERE request_id = ?",
        (event["aggregate_id"], event["occurred_at"], p.get("reason"), p["request_id"]),
    )


def _on_request_superseded(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "UPDATE p_requests SET status = 'superseded', superseded_by = ? WHERE request_id = ?",
        (p["superseded_by"], event["aggregate_id"]),
    )
    conn.execute(
        "UPDATE p_decisions SET effective = 0, ineffective_reason = 'request_superseded' "
        "WHERE request_id = ? AND effective = 1",
        (event["aggregate_id"],),
    )


def _on_decision_effectiveness_changed(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "UPDATE p_decisions SET effective = ?, ineffective_reason = ? WHERE decision_id = ?",
        (1 if p["effective"] else 0, None if p["effective"] else p.get("reason"),
         event["aggregate_id"]),
    )


def _on_right_withdrawn(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "UPDATE p_rights SET withdrawn = 1, withdrawn_at = ?, withdraw_reason = ? "
        "WHERE right_id = ?",
        (p["effective_at"], p.get("reason"), p["right_id"]),
    )


def _on_right_expired(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "UPDATE p_rights SET active = 0, inactive_reason = 'expired' WHERE right_id = ?",
        (p["right_id"],),
    )


def _on_slice_frozen(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "UPDATE p_slices SET frozen = 1, freeze_reason = ?, frozen_at = ? WHERE slice_id = ?",
        (p["reason"], p.get("frozen_at", event["occurred_at"]), event["aggregate_id"]),
    )


def _on_dataset_registered(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "INSERT INTO p_datasets(dataset_id, name, product, registered_by, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (p["dataset_id"], p.get("name", p["dataset_id"]), p["product"],
         p.get("registered_by"), event["occurred_at"]),
    )
    for slice_id in p["slice_ids"]:
        conn.execute(
            "INSERT OR IGNORE INTO p_dataset_slices(dataset_id, slice_id, added_at) "
            "VALUES (?, ?, ?)",
            (p["dataset_id"], slice_id, event["occurred_at"]),
        )


def _on_dataset_derivation(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "INSERT OR IGNORE INTO p_dataset_lineage(parent_dataset_id, child_dataset_id, recorded_at) "
        "VALUES (?, ?, ?)",
        (p["parent_dataset_id"], p["dataset_id"] if "dataset_id" in p else p["child_dataset_id"],
         event["occurred_at"]),
    )
    # 父数据集中的切片流入子数据集
    conn.execute(
        "INSERT OR IGNORE INTO p_dataset_slices(dataset_id, slice_id, added_at) "
        "SELECT ?, slice_id, ? FROM p_dataset_slices WHERE dataset_id = ?",
        (p["child_dataset_id"], event["occurred_at"], p["parent_dataset_id"]),
    )
    # 父已冻结时，子数据集一并冻结
    parent = conn.execute(
        "SELECT frozen, freeze_reason FROM p_datasets WHERE dataset_id = ?",
        (p["parent_dataset_id"],),
    ).fetchone()
    if parent is not None and parent["frozen"]:
        conn.execute(
            "UPDATE p_datasets SET frozen = 1, freeze_reason = ? WHERE dataset_id = ?",
            (parent["freeze_reason"], p["child_dataset_id"]),
        )


def _on_dataset_frozen(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "UPDATE p_datasets SET frozen = 1, freeze_reason = ? WHERE dataset_id = ?",
        (p["reason"], p["dataset_id"]),
    )
    # 冻结沿派生边向下传播：所有（传递）子数据集一并冻结
    for child in dataset_descendants(conn, p["dataset_id"]):
        conn.execute(
            "UPDATE p_datasets SET frozen = 1, freeze_reason = ? WHERE dataset_id = ? AND frozen = 0",
            (f"derived_from:{p['dataset_id']}", child),
        )


def _on_publication_recorded(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "INSERT INTO p_publications(publication_id, slice_id, published_at, basis, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (p["publication_id"], event["aggregate_id"], p["published_at"],
         json.dumps(p["basis"], ensure_ascii=False), event["occurred_at"]),
    )


def _on_disposition_raised(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "INSERT INTO p_dispositions(disposition_id, slice_id, kind, origin, description, "
        "status, raised_by, created_at) VALUES (?, ?, ?, ?, ?, 'open', ?, ?)",
        (p["disposition_id"], p["slice_id"], p["kind"],
         json.dumps(p.get("origin", {}), ensure_ascii=False), p["description"],
         p.get("raised_by"), event["occurred_at"]),
    )


def _on_disposition_fulfilled(conn: sqlite3.Connection, event: dict) -> None:
    p = event["payload"]
    conn.execute(
        "UPDATE p_dispositions SET status = 'fulfilled', fulfilled_by = ?, fulfillment_note = ?, "
        "fulfilled_at = ? WHERE disposition_id = ?",
        (p.get("fulfilled_by"), p.get("note"), event["occurred_at"], p["disposition_id"]),
    )


_HANDLERS = {
    "ORG_REGISTERED": _on_org_registered,
    "ASSET_INGESTED": _on_asset_ingested,
    "ASSET_DISPUTED": _on_asset_disputed,
    "RIGHT_DECLARED": _on_right_declared,
    "SLICE_CUT": _on_slice_cut,
    "SLICE_REVIEWED": _on_slice_reviewed,
    "USE_REQUESTED": _on_use_requested,
    "USE_APPROVED": _on_use_approved,
    "USE_REJECTED": _on_use_rejected,
    "REQUEST_SUPERSEDED": _on_request_superseded,
    "DECISION_EFFECTIVENESS_CHANGED": _on_decision_effectiveness_changed,
    "RIGHT_WITHDRAWN": _on_right_withdrawn,
    "RIGHT_EXPIRED": _on_right_expired,
    "SLICE_FROZEN": _on_slice_frozen,
    "DATASET_REGISTERED": _on_dataset_registered,
    "DATASET_DERIVATION_RECORDED": _on_dataset_derivation,
    "DATASET_FROZEN": _on_dataset_frozen,
    "PUBLICATION_RECORDED": _on_publication_recorded,
    "DISPOSITION_RAISED": _on_disposition_raised,
    "DISPOSITION_FULFILLED": _on_disposition_fulfilled,
    "REVIEW_OPENED": lambda conn, event: None,
}


# ---------------------------------------------------------------------------
# 只读查询辅助
# ---------------------------------------------------------------------------


def get_org(conn: sqlite3.Connection, org_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM p_orgs WHERE org_id = ?", (org_id,)).fetchone()


def get_asset(conn: sqlite3.Connection, asset_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM p_assets WHERE asset_id = ?", (asset_id,)).fetchone()


def get_right(conn: sqlite3.Connection, right_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM p_rights WHERE right_id = ?", (right_id,)).fetchone()


def rights_for_asset(conn: sqlite3.Connection, asset_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM p_rights WHERE asset_id = ? ORDER BY created_at", (asset_id,)
    ).fetchall()


def get_slice(conn: sqlite3.Connection, slice_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM p_slices WHERE slice_id = ?", (slice_id,)).fetchone()


def get_request(conn: sqlite3.Connection, request_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM p_requests WHERE request_id = ?", (request_id,)).fetchone()


def get_decision(conn: sqlite3.Connection, decision_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM p_decisions WHERE decision_id = ?", (decision_id,)
    ).fetchone()


def get_dataset(conn: sqlite3.Connection, dataset_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM p_datasets WHERE dataset_id = ?", (dataset_id,)
    ).fetchone()


def slices_for_asset(conn: sqlite3.Connection, asset_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM p_slices WHERE asset_id = ? ORDER BY ordinal", (asset_id,)
    ).fetchall()


def slices_for_dataset(conn: sqlite3.Connection, dataset_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT s.* FROM p_slices s JOIN p_dataset_slices d ON d.slice_id = s.slice_id "
        "WHERE d.dataset_id = ? ORDER BY s.asset_id, s.ordinal",
        (dataset_id,),
    ).fetchall()


def publications_for_slice(conn: sqlite3.Connection, slice_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM p_publications WHERE slice_id = ? ORDER BY published_at", (slice_id,)
    ).fetchall()


def disposition_exists(conn: sqlite3.Connection, disposition_id: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM p_dispositions WHERE disposition_id = ?", (disposition_id,)
    ).fetchone() is not None


def decisions_for_slice(conn: sqlite3.Connection, slice_id: str) -> list[sqlite3.Row]:
    """覆盖某切片的所有决定（按决定时间）。"""
    return conn.execute(
        "SELECT d.* FROM p_decisions d JOIN p_request_slices rs ON rs.request_id = d.request_id "
        "WHERE rs.slice_id = ? ORDER BY d.decided_at",
        (slice_id,),
    ).fetchall()


def latest_reviews(conn: sqlite3.Connection, slice_id: str) -> dict[str, dict]:
    """每种审校职责的最新一次记录。"""
    rows = conn.execute(
        "SELECT r.* FROM p_slice_reviews r JOIN ("
        "SELECT review_kind, MAX(id) AS max_id FROM p_slice_reviews WHERE slice_id = ? "
        "GROUP BY review_kind) t ON r.id = t.max_id",
        (slice_id,),
    ).fetchall()
    return {
        row["review_kind"]: {
            "reviewer_id": row["reviewer_id"],
            "passed": bool(row["passed"]),
            "notes": row["notes"],
            "reviewed_at": row["reviewed_at"],
        }
        for row in rows
    }


def datasets_containing_slice(conn: sqlite3.Connection, slice_id: str) -> list[str]:
    rows = conn.execute(
        "SELECT dataset_id FROM p_dataset_slices WHERE slice_id = ?", (slice_id,)
    ).fetchall()
    return [row["dataset_id"] for row in rows]


def dataset_descendants(conn: sqlite3.Connection, dataset_id: str) -> list[str]:
    """沿派生边向下闭包：所有（传递）子数据集。"""
    rows = conn.execute(
        "WITH RECURSIVE descendants AS ("
        "SELECT child_dataset_id AS id FROM p_dataset_lineage WHERE parent_dataset_id = ? "
        "UNION ALL SELECT l.child_dataset_id FROM p_dataset_lineage l "
        "JOIN descendants d ON l.parent_dataset_id = d.id) "
        "SELECT id FROM descendants",
        (dataset_id,),
    ).fetchall()
    return [row["id"] for row in rows]


def dataset_parents(conn: sqlite3.Connection, dataset_id: str) -> list[str]:
    rows = conn.execute(
        "WITH RECURSIVE ancestors AS ("
        "SELECT parent_dataset_id AS id FROM p_dataset_lineage WHERE child_dataset_id = ? "
        "UNION ALL SELECT l.parent_dataset_id FROM p_dataset_lineage l "
        "JOIN ancestors a ON l.child_dataset_id = a.id) "
        "SELECT id FROM ancestors",
        (dataset_id,),
    ).fetchall()
    return [row["id"] for row in rows]
