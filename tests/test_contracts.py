from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from corpus_clearance.contracts import validate_event


class ContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schema = json.loads((ROOT / "contracts" / "domain.schema.json").read_text(encoding="utf-8"))
        cls.sample = json.loads((ROOT / "data" / "sample.json").read_text(encoding="utf-8"))

    def test_sample_is_valid(self) -> None:
        self.assertEqual([], validate_event(self.sample, self.schema))

    def test_missing_envelope_fields_have_stable_order(self) -> None:
        issues = validate_event({}, self.schema)
        self.assertEqual(sorted(issue.field for issue in issues), [issue.field for issue in issues])
        self.assertIn("event_id", {issue.field for issue in issues})

    def test_time_and_version_boundaries(self) -> None:
        event = dict(self.sample, occurred_at="2026-09-24T12:00:00", version=0)
        codes = {(issue.field, issue.code) for issue in validate_event(event, self.schema)}
        self.assertIn(("occurred_at", "timezone_required"), codes)
        self.assertIn(("version", "positive_integer"), codes)

    def test_event_specific_payload_is_required(self) -> None:
        event = dict(self.sample, event_type="ASSET_INGESTED", payload={})
        issues = validate_event(event, self.schema)
        self.assertIn(("payload.source_id", "required"), [(issue.field, issue.code) for issue in issues])

    def test_unknown_event_is_rejected(self) -> None:
        event = dict(self.sample, event_type="UNKNOWN")
        issues = validate_event(event, self.schema)
        self.assertIn(("event_type", "unsupported_value"), [(issue.field, issue.code) for issue in issues])


if __name__ == "__main__":
    unittest.main()
