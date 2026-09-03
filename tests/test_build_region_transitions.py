import unittest
from unittest.mock import patch

from scripts import build_region_transitions as transitions


class RegionTransitionTest(unittest.TestCase):
    def test_known_adjustments_missing_from_klass_are_curated(self) -> None:
        curated = set(transitions.curated_adjustments())
        expected = {
            ("2017-01-01", "1102", "1124"),
            ("2019-03-01", "5004", "5040"),
            ("2019-05-15", "1021", "1017"),
            ("2020-03-01", "3014", "3016"),
            ("2020-03-01", "5033", "5025"),
            ("2020-07-01", "3407", "3443"),
            ("2020-10-01", "1103", "1124"),
            ("2022-01-01", "3048", "3005"),
            ("2022-01-01", "3005", "3048"),
            ("2022-01-01", "4618", "4619"),
            ("2022-01-01", "3405", "3440"),
        }

        self.assertTrue(expected <= curated)

    def test_split_marks_only_the_split_source_as_unsafe(self) -> None:
        municipality_changes = [
            {"changeOccurred": "2020-01-01", "oldCode": "1401", "newCode": "4602"},
            {"changeOccurred": "2020-01-01", "oldCode": "1439", "newCode": "4602"},
        ]
        district_changes = [
            {
                "changeOccurred": "2020-01-01",
                "oldCode": "14390208",
                "newCode": "46490310",
            }
        ]

        with patch.object(transitions, "curated_adjustments", return_value={}):
            rows = transitions.build_municipality_rows(
                municipality_changes, district_changes
            )

        event = {(row["from_id"], row["to_id"]): row for row in rows}
        self.assertEqual(event[("1439", "4602")]["mapping_quality"], "split_merge")
        self.assertEqual(event[("1439", "4649")]["mapping_quality"], "split_merge")
        self.assertEqual(event[("1401", "4602")]["mapping_quality"], "safe_merge")
        self.assertEqual(event[("1401", "4602")]["safe_to_aggregate"], "true")
        self.assertEqual(len({row["event_id"] for row in rows}), 1)

    def test_persistent_source_is_a_boundary_adjustment(self) -> None:
        municipality_changes = [
            {"changeOccurred": "2026-01-01", "oldCode": "3118", "newCode": "3118"},
        ]
        district_changes = [
            {
                "changeOccurred": "2026-01-01",
                "oldCode": "31181303",
                "newCode": "32070606",
            }
        ]

        with patch.object(transitions, "curated_adjustments", return_value={}):
            rows = transitions.build_municipality_rows(
                municipality_changes, district_changes
            )

        self.assertEqual(rows[0]["change_type"], "boundary_adjustment")
        self.assertEqual(rows[0]["mapping_quality"], "boundary_adjustment")
        self.assertEqual(rows[0]["safe_to_aggregate"], "false")

    def test_pure_merge_is_safe(self) -> None:
        changes = [
            {"changeOccurred": "2018-01-01", "oldCode": "16", "newCode": "50"},
            {"changeOccurred": "2018-01-01", "oldCode": "17", "newCode": "50"},
        ]
        rows = transitions.build_level_rows(changes, "county", 2, "test", "test")

        self.assertEqual({row["change_type"] for row in rows}, {"merge"})
        self.assertEqual({row["mapping_quality"] for row in rows}, {"safe_merge"})
        self.assertEqual({row["safe_to_aggregate"] for row in rows}, {"true"})


if __name__ == "__main__":
    unittest.main()
