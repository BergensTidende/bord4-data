#!/usr/bin/env python3
"""Build flat region transition datasets from raw transition mappings."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_DIR / "data" / "raw" / "region-transitions"
DIST_DIR = PROJECT_DIR / "data" / "dist"

CSV_COLUMNS = [
    "from_year",
    "to_year",
    "level",
    "from_id",
    "to_id",
    "change_type",
    "source_file",
]


def level_from_filename(path: Path) -> str:
    if path.name.startswith("fylker_"):
        return "county"
    if path.name.startswith("kommuner_"):
        return "municipality"
    raise ValueError(f"Cannot infer region level from {path.name}")


def normalize_mapping(mapping: dict[str, str | list[str]]) -> list[tuple[str, str]]:
    """Return edges as (from_id, to_id), regardless of raw mapping orientation."""
    edges: list[tuple[str, str]] = []

    for key, value in mapping.items():
        if isinstance(value, list):
            for from_id in value:
                edges.append((from_id, key))
        else:
            edges.append((key, value))

    return edges


def classify_edges(edges: list[tuple[str, str]]) -> dict[tuple[str, str], str]:
    from_to_count: dict[str, set[str]] = defaultdict(set)
    to_from_count: dict[str, set[str]] = defaultdict(set)

    for from_id, to_id in edges:
        from_to_count[from_id].add(to_id)
        to_from_count[to_id].add(from_id)

    change_types: dict[tuple[str, str], str] = {}
    for from_id, to_id in edges:
        has_split = len(from_to_count[from_id]) > 1
        has_merge = len(to_from_count[to_id]) > 1

        if has_split and has_merge:
            change_type = "boundary_change"
        elif has_split:
            change_type = "split"
        elif has_merge:
            change_type = "merge"
        else:
            change_type = "renumbered"

        change_types[(from_id, to_id)] = change_type

    return change_types


def build_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    for path in sorted(RAW_DIR.glob("*.json")):
        from_year, to_year, mapping = json.loads(path.read_text(encoding="utf-8"))
        edges = normalize_mapping(mapping)
        change_types = classify_edges(edges)

        for from_id, to_id in sorted(edges):
            rows.append(
                {
                    "from_year": from_year,
                    "to_year": to_year,
                    "level": level_from_filename(path),
                    "from_id": from_id,
                    "to_id": to_id,
                    "change_type": change_types[(from_id, to_id)],
                    "source_file": str(path.relative_to(PROJECT_DIR)),
                }
            )

    return rows


def validate_rows(rows: list[dict[str, str]]) -> None:
    seen: set[tuple[str, str, str, str, str]] = set()

    for row in rows:
        key = (
            row["from_year"],
            row["to_year"],
            row["level"],
            row["from_id"],
            row["to_id"],
        )
        if key in seen:
            raise ValueError(f"Duplicate transition row: {key}")
        seen.add(key)

        expected_length = 2 if row["level"] == "county" else 4
        if len(row["from_id"]) != expected_length or len(row["to_id"]) != expected_length:
            raise ValueError(f"Unexpected code length in row: {row}")


def write_csv(rows: list[dict[str, str]]) -> None:
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DIST_DIR / "norwegian_region_transitions.csv"

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_json(rows: list[dict[str, str]]) -> None:
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DIST_DIR / "norwegian_region_transitions.json"
    output_path.write_text(
        json.dumps(rows, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    rows = build_rows()
    validate_rows(rows)
    write_csv(rows)
    write_json(rows)
    print(f"Wrote {len(rows)} region transition rows to {DIST_DIR}")


if __name__ == "__main__":
    main()
