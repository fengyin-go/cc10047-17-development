"""拒收数据批量补交的回归用例：只用标准库 unittest，不额外装依赖。

运行：cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import threading
import unittest

from app.services.ingest_correction import IngestCorrectionService


class CorrectionFlowTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service = IngestCorrectionService()
        self.batch = self.service.replay({
            "idempotency_key": "case-batch",
            "reads": [
                {"partition": "p1", "offsets": [101]},
                {"partition": "p2", "offsets": [205]},
            ],
        })["batch"]

    def item(self, offset_key: str) -> dict:
        for item in self.batch["items"]:
            if item["offset_key"] == offset_key:
                return item
        raise AssertionError(f"{offset_key} 不在批次里")

    def test_seed_rows_have_expected_missing_fields(self) -> None:
        todo = {row["offset_key"]: row for row in self.service.todos()["items"]}
        self.assertEqual(todo["p1:101"]["missing_fields"], ["金额"])
        self.assertEqual(todo["p2:205"]["missing_fields"], ["日期"])

    def test_replay_creates_one_correction_per_original_offset(self) -> None:
        first = self.service.replay({
            "idempotency_key": "another-batch",
            "reads": [
                {"partition": "p1", "offsets": [101]},
                {"partition": "p2", "offsets": [205]},
            ],
        })
        self.assertEqual(first["created_corrections"], 0)
        self.assertEqual(first["duplicate_corrections"], 2)
        detail = self.service.version_detail(7)
        self.assertEqual(detail["counts"]["corrections"], 2)

    def test_same_batch_token_only_takes_effect_once(self) -> None:
        results: list[dict] = []

        def submit() -> None:
            results.append(self.service.replay({
                "idempotency_key": "case-batch",
                "reads": [{"partition": "p1", "offsets": [101]}],
            }))

        threads = [threading.Thread(target=submit) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(sum(1 for item in results if item["effective"]), 0)
        detail = self.service.version_detail(7)
        self.assertEqual(detail["batches"][0]["effective_submissions"], 1)

    def test_row_confirmation_releases_fixed_row_before_whole_batch(self) -> None:
        batch_id = self.batch["id"]
        self.service.fix_correction(batch_id, {
            "partition": "p1", "offset": 101, "values": {"amount": 199},
        })

        conflict = self.service.confirm_all(batch_id)
        self.assertFalse(conflict["ok"])
        self.assertTrue(conflict["conflict"])
        self.assertEqual([row["offset_key"] for row in conflict["waiting_for"]], ["p2:205"])

        row = self.service.confirm_row(batch_id, {"partition": "p1", "offset": 101})
        self.assertTrue(row["ok"])
        self.assertEqual(row["boundary"]["mode"], "逐行确认")
        self.assertEqual(row["boundary"]["offsets"], ["p1:101"])

        detail = self.service.version_detail(7)
        accepted = [item["offset_key"] for item in detail["accepted"]]
        rejected = [item["offset_key"] for item in detail["rejected"]]
        self.assertEqual(accepted, ["p1:101"])
        self.assertIn("p2:205", rejected)

    def test_whole_batch_confirmation_waits_for_both_rows(self) -> None:
        batch_id = self.batch["id"]
        for payload in (
            {"partition": "p1", "offset": 101, "values": {"amount": 199}},
            {"partition": "p2", "offset": 205, "values": {"order_date": "2026-11-21"}},
        ):
            self.assertTrue(self.service.fix_correction(batch_id, payload)["ok"])

        result = self.service.confirm_all(batch_id)
        self.assertTrue(result["ok"])
        self.assertEqual(result["boundary"]["mode"], "整批确认")
        self.assertEqual(result["boundary"]["offsets"], ["p1:101", "p2:205"])

    def test_same_business_key_new_version_does_not_replace_original_offset(self) -> None:
        self.service.fix_correction(self.batch["id"], {
            "partition": "p1", "offset": 101, "values": {"amount": 199},
        })
        result = self.service.add_new_version({
            "partition": "p2",
            "offset": 206,
            "business_key": "ORD-1001",
            "reject_id": 1,
        })
        self.assertTrue(result["created"])
        self.assertFalse(result["replaced"])
        self.assertTrue(result["original_still_valid"])

        detail = self.service.version_detail(7)
        offsets = {item["offset_key"]: item for item in detail["accepted"] + detail["rejected"]}
        self.assertEqual(offsets["p1:101"]["values"]["amount"], 199)
        self.assertEqual(offsets["p2:206"]["kind"], "同业务键新版本")
        self.assertEqual(offsets["p2:206"]["same_key_of"], 1)

    def test_checkpoint_realigns_next_read_offsets_by_receipts(self) -> None:
        batch_id = self.batch["id"]
        self.service.fix_correction(batch_id, {
            "partition": "p1", "offset": 101, "values": {"amount": 199},
        })
        self.service.confirm_row(batch_id, {"partition": "p1", "offset": 101})
        self.service.add_new_version({
            "partition": "p2", "offset": 206, "business_key": "ORD-1001", "reject_id": 1,
        })
        checkpoint = self.service.checkpoint()
        partitions = {item["partition"]: item for item in checkpoint["partitions"]}
        self.assertEqual(partitions["p1"]["next_read_offset"], 102)
        self.assertEqual(partitions["p1"]["last_confirmed_offset"], 101)
        self.assertEqual(partitions["p2"]["next_read_offset"], 205)
        self.assertEqual(partitions["p2"]["pending_offsets"], [205, 206])
        self.assertEqual(len(partitions["p1"]["receipts"]), 1)


if __name__ == "__main__":
    unittest.main()
