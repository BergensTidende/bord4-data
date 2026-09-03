#!/usr/bin/env python3
"""Build municipality and county transitions from SSB Klass since 2000."""

from __future__ import annotations

import csv
import json
import urllib.request
from collections import defaultdict, deque
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_DIR / "data" / "raw" / "region-transitions"
DIST_DIR = PROJECT_DIR / "data" / "dist"
START_DATE = "2000-01-01"

SSB_KLASS_BASE_URL = "https://data.ssb.no/api/klass/v1/classifications"
SSB_MUNICIPALITY_CHANGES_URL = f"{SSB_KLASS_BASE_URL}/131/changes?from={START_DATE}"
SSB_COUNTY_CHANGES_URL = f"{SSB_KLASS_BASE_URL}/104/changes?from={START_DATE}"
SSB_BASIC_DISTRICT_CHANGES_URL = f"{SSB_KLASS_BASE_URL}/1/changes?from={START_DATE}"
CURATED_ADJUSTMENTS_FILE = RAW_DIR / "subdistrict_adjustments.csv"

CSV_COLUMNS = [
    "effective_date",
    "from_year",
    "to_year",
    "level",
    "from_id",
    "to_id",
    "change_type",
    "mapping_quality",
    "safe_to_aggregate",
    "event_id",
    "source_file",
    "source_url",
    "note",
]


def fetch_changes(url: str) -> list[dict[str, str]]:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        data = json.load(response)
    return data["codeChanges"]


def api_edges(changes: list[dict[str, str]], width: int) -> set[tuple[str, str, str]]:
    edges: set[tuple[str, str, str]] = set()
    for change in changes:
        date = str(change.get("changeOccurred", ""))
        from_id = str(change.get("oldCode", ""))
        to_id = str(change.get("newCode", ""))
        if date >= START_DATE and len(from_id) == width and len(to_id) == width:
            edges.add((date, from_id, to_id))
    return edges


def basic_district_edges(
    changes: list[dict[str, str]],
) -> set[tuple[str, str, str]]:
    edges: set[tuple[str, str, str]] = set()
    for date, old_code, new_code in api_edges(changes, 8):
        from_id, to_id = old_code[:4], new_code[:4]
        if from_id != to_id:
            edges.add((date, from_id, to_id))
    return edges


def curated_adjustments() -> dict[tuple[str, str, str], dict[str, str]]:
    with CURATED_ADJUSTMENTS_FILE.open(encoding="utf-8", newline="") as f:
        return {
            (row["effective_date"], row["from_id"], row["to_id"]): row
            for row in csv.DictReader(f)
        }


def connected_components(edges: set[tuple[str, str]]) -> list[set[str]]:
    neighbours: dict[str, set[str]] = defaultdict(set)
    for from_id, to_id in edges:
        neighbours[from_id].add(to_id)
        neighbours[to_id].add(from_id)

    components: list[set[str]] = []
    unseen = set(neighbours)
    while unseen:
        root = min(unseen)
        component: set[str] = set()
        queue = deque([root])
        while queue:
            code = queue.popleft()
            if code in component:
                continue
            component.add(code)
            queue.extend(neighbours[code] - component)
        unseen -= component
        components.append(component)
    return components


def classify_rows(
    level: str,
    all_edges: set[tuple[str, str, str]],
    ceased_sources: set[tuple[str, str]],
    source_for_edge: dict[tuple[str, str, str], tuple[str, str, str]],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for date in sorted({date for date, _, _ in all_edges}):
        dated_edges = {
            (old, new) for edge_date, old, new in all_edges if edge_date == date
        }
        targets_by_source: dict[str, set[str]] = defaultdict(set)
        sources_by_target: dict[str, set[str]] = defaultdict(set)
        for from_id, to_id in dated_edges:
            targets_by_source[from_id].add(to_id)
            sources_by_target[to_id].add(from_id)

        component_for_code: dict[str, str] = {}
        for component in connected_components(dated_edges):
            component_key = min(component)
            component_for_code.update({code: component_key for code in component})

        for from_id, to_id in sorted(dated_edges):
            is_boundary_adjustment = (date, from_id) not in ceased_sources
            has_split = len(targets_by_source[from_id]) > 1
            has_merge = len(sources_by_target[to_id]) > 1
            if is_boundary_adjustment:
                change_type = "boundary_adjustment"
                mapping_quality = "boundary_adjustment"
            else:
                if has_split and has_merge:
                    change_type = "boundary_change"
                elif has_split:
                    change_type = "split"
                elif has_merge:
                    change_type = "merge"
                else:
                    change_type = "renumbered"
                # Mapping quality answers whether this entire source region has
                # one unambiguous successor. A split elsewhere in the same
                # reform event must not make this source unsafe.
                mapping_quality = "split_merge" if has_split else "safe_merge"

            source_file, source_url, note = source_for_edge[(date, from_id, to_id)]
            to_year = date[:4]
            rows.append(
                {
                    "effective_date": date,
                    "from_year": str(int(to_year) - 1),
                    "to_year": to_year,
                    "level": level,
                    "from_id": from_id,
                    "to_id": to_id,
                    "change_type": change_type,
                    "mapping_quality": mapping_quality,
                    "safe_to_aggregate": str(mapping_quality == "safe_merge").lower(),
                    "event_id": f"{level}-{date}-{component_for_code[from_id]}",
                    "source_file": source_file,
                    "source_url": source_url,
                    "note": note,
                }
            )
    return rows


def build_level_rows(
    changes: list[dict[str, str]],
    level: str,
    width: int,
    source_label: str,
    source_url: str,
) -> list[dict[str, str]]:
    raw_edges = api_edges(changes, width)
    self_edges = {(date, old) for date, old, new in raw_edges if old == new}
    edges = {(date, old, new) for date, old, new in raw_edges if old != new}
    ceased_sources = {
        (date, old) for date, old, _ in edges if (date, old) not in self_edges
    }
    sources = {edge: (source_label, source_url, "") for edge in edges}
    return classify_rows(level, edges, ceased_sources, sources)


def build_municipality_rows(
    municipality_changes: list[dict[str, str]],
    district_changes: list[dict[str, str]],
) -> list[dict[str, str]]:
    raw_municipality_edges = api_edges(municipality_changes, 4)
    self_edges = {
        (date, old)
        for date, old, new in raw_municipality_edges
        if old == new
    }
    municipality_edges = {
        (date, old, new)
        for date, old, new in raw_municipality_edges
        if old != new
    }
    ceased_sources = {
        (date, old)
        for date, old, _ in municipality_edges
        if (date, old) not in self_edges
    }
    district_edges = basic_district_edges(district_changes)
    curated = curated_adjustments()
    edges = municipality_edges | district_edges | set(curated)

    sources: dict[tuple[str, str, str], tuple[str, str, str]] = {}
    for edge in edges:
        if edge in municipality_edges:
            sources[edge] = ("ssb-klass:131", SSB_MUNICIPALITY_CHANGES_URL, "")
        elif edge in district_edges:
            sources[edge] = (
                "ssb-klass:1",
                SSB_BASIC_DISTRICT_CHANGES_URL,
                "Avledet fra endring i hel grunnkrets.",
            )
        else:
            item = curated[edge]
            sources[edge] = (
                str(CURATED_ADJUSTMENTS_FILE.relative_to(PROJECT_DIR)),
                item["source_url"],
                item["note"],
            )
    return classify_rows("municipality", edges, ceased_sources, sources)


def build_rows() -> list[dict[str, str]]:
    municipality_changes = fetch_changes(SSB_MUNICIPALITY_CHANGES_URL)
    county_changes = fetch_changes(SSB_COUNTY_CHANGES_URL)
    district_changes = fetch_changes(SSB_BASIC_DISTRICT_CHANGES_URL)
    rows = build_municipality_rows(municipality_changes, district_changes)
    rows += build_level_rows(
        county_changes,
        "county",
        2,
        "ssb-klass:104",
        SSB_COUNTY_CHANGES_URL,
    )
    return sorted(
        rows,
        key=lambda row: (
            row["effective_date"],
            row["level"],
            row["from_id"],
            row["to_id"],
        ),
    )


def validate_rows(rows: list[dict[str, str]]) -> None:
    seen: set[tuple[str, str, str, str]] = set()
    for row in rows:
        key = (row["effective_date"], row["level"], row["from_id"], row["to_id"])
        if key in seen:
            raise ValueError(f"Duplicate transition row: {key}")
        seen.add(key)
        expected_length = 2 if row["level"] == "county" else 4
        if len(row["from_id"]) != expected_length or len(row["to_id"]) != expected_length:
            raise ValueError(f"Unexpected code length in row: {row}")
        if row["mapping_quality"] not in {
            "safe_merge",
            "split_merge",
            "boundary_adjustment",
        }:
            raise ValueError(f"Unexpected mapping quality: {row}")
        expected_safe = str(row["mapping_quality"] == "safe_merge").lower()
        if row["safe_to_aggregate"] != expected_safe:
            raise ValueError(f"Inconsistent aggregation flag: {row}")

    vagsoy = {
        (row["to_id"], row["mapping_quality"])
        for row in rows
        if row["level"] == "municipality" and row["from_id"] == "1439"
    }
    if vagsoy != {("4602", "split_merge"), ("4649", "split_merge")}:
        raise ValueError(f"Unexpected mappings for Vågsøy: {vagsoy}")

    generated_edges = {
        (row["effective_date"], row["from_id"], row["to_id"])
        for row in rows
        if row["level"] == "municipality"
    }
    missing_curated = set(curated_adjustments()) - generated_edges
    if missing_curated:
        raise ValueError(f"Missing curated adjustments: {sorted(missing_curated)}")


def write_csv(rows: list[dict[str, str]]) -> None:
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    with (DIST_DIR / "norwegian_region_transitions.csv").open(
        "w", newline="", encoding="utf-8"
    ) as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_json(rows: list[dict[str, str]]) -> None:
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    (DIST_DIR / "norwegian_region_transitions.json").write_text(
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
