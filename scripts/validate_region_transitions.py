#!/usr/bin/env python3
"""Audit that region transitions can trace recent history to current regions."""

from __future__ import annotations

import csv
from collections import defaultdict, deque
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent.parent
DIST_DIR = PROJECT_DIR / "data" / "dist"
START_DATE = "2000-01-01"


def read_csv(name: str) -> list[dict[str, str]]:
    with (DIST_DIR / name).open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def trace_to_current(
    start: str,
    successors: dict[str, set[str]],
    current: set[str],
) -> set[str]:
    results: set[str] = set()
    seen: set[str] = set()
    queue = deque([start])
    while queue:
        code = queue.popleft()
        if code in seen:
            continue
        seen.add(code)
        if code in current:
            results.add(code)
        else:
            queue.extend(successors.get(code, set()))
    return results


def validate() -> None:
    transitions = read_csv("norwegian_region_transitions.csv")
    municipalities = read_csv("norwegian_municipalities.csv")
    history = read_csv("norwegian_regions_history.csv")

    current = {row["kommunenummer"] for row in municipalities}
    successors: dict[str, set[str]] = defaultdict(set)
    for row in transitions:
        if (
            row["level"] == "municipality"
            and row["mapping_quality"] != "boundary_adjustment"
        ):
            successors[row["from_id"]].add(row["to_id"])

    historical = {
        row["regionnummer"]
        for row in history
        if row["nivå"] == "kommune"
        and (not row["gyldig_til"] or row["gyldig_til"] >= START_DATE)
    }
    untraceable = sorted(
        code for code in historical - current if not trace_to_current(code, successors, current)
    )
    if untraceable:
        raise ValueError(
            "Historical municipality codes without a path to a current code: "
            + ", ".join(untraceable)
        )

    current_counties = {
        row["regionnummer"]
        for row in history
        if row["nivå"] == "fylke" and row["er_gjeldende"] == "true"
    }
    historical_counties = {
        row["regionnummer"]
        for row in history
        if row["nivå"] == "fylke"
        and (not row["gyldig_til"] or row["gyldig_til"] >= START_DATE)
    }
    county_successors: dict[str, set[str]] = defaultdict(set)
    for row in transitions:
        if row["level"] == "county" and row["mapping_quality"] != "boundary_adjustment":
            county_successors[row["from_id"]].add(row["to_id"])
    untraceable_counties = sorted(
        code
        for code in historical_counties - current_counties
        if not trace_to_current(code, county_successors, current_counties)
    )
    if untraceable_counties:
        raise ValueError(
            "Historical county codes without a path to a current code: "
            + ", ".join(untraceable_counties)
        )

    rows_by_source: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    transition_dates: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in transitions:
        source_key = (row["effective_date"], row["level"], row["from_id"])
        rows_by_source[source_key].append(row)
        if row["mapping_quality"] != "boundary_adjustment":
            transition_dates[(row["level"], row["from_id"])].add(
                row["effective_date"]
            )

    for source_key, source_rows in rows_by_source.items():
        qualities = {row["mapping_quality"] for row in source_rows}
        targets = {row["to_id"] for row in source_rows}
        if len(qualities) != 1:
            raise ValueError(f"Mixed mapping quality for {source_key}: {qualities}")
        quality = next(iter(qualities))
        if quality == "safe_merge" and len(targets) != 1:
            raise ValueError(f"Safe source has multiple targets: {source_key}: {targets}")
        if quality == "split_merge" and len(targets) < 2:
            raise ValueError(f"Split source has fewer than two targets: {source_key}")

    repeated_sources = {
        key: dates for key, dates in transition_dates.items() if len(dates) > 1
    }
    if repeated_sources:
        raise ValueError(
            "Region codes have transitions on multiple dates; tracing needs dates: "
            f"{repeated_sources}"
        )

    vagsoy = {
        row["to_id"]
        for row in transitions
        if row["level"] == "municipality"
        and row["from_id"] == "1439"
        and row["mapping_quality"] == "split_merge"
    }
    if vagsoy != {"4602", "4649"}:
        raise ValueError(f"Unexpected mappings for Vågsøy: {vagsoy}")

    trondelag = {
        (row["from_id"], row["to_id"])
        for row in transitions
        if row["level"] == "county" and row["from_id"] in {"16", "17"}
    }
    if trondelag != {("16", "50"), ("17", "50")}:
        raise ValueError(f"Missing county transitions to Trøndelag: {trondelag}")

    safe_successors: dict[str, set[str]] = defaultdict(set)
    for row in transitions:
        if row["level"] == "municipality" and row["mapping_quality"] == "safe_merge":
            safe_successors[row["from_id"]].add(row["to_id"])
    expected_safe_paths = {
        "1617": {"5056"},
        "5013": {"5056"},
        "1703": {"5007"},
        "5005": {"5007"},
        "1750": {"5060"},
        "5050": {"5060"},
        "1141": {"1103"},
        "1142": {"1103"},
    }
    for code, expected in expected_safe_paths.items():
        actual = trace_to_current(code, safe_successors, current)
        if actual != expected:
            raise ValueError(f"Unexpected safe mapping for {code}: {actual}, expected {expected}")

    if trace_to_current("1439", safe_successors, current):
        raise ValueError("Vågsøy must not have a safe whole-municipality mapping")

    print(
        f"Validated {len(transitions)} transitions; "
        f"all {len(historical)} municipality and {len(historical_counties)} county "
        "codes since 2000 are traceable."
    )


if __name__ == "__main__":
    validate()
