#!/usr/bin/env python3
"""Build a small lookup table for current Norwegian municipalities."""

from __future__ import annotations

import csv
import json
import urllib.parse
import urllib.request
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent.parent
DIST_DIR = PROJECT_DIR / "data" / "dist"
AS_OF_DATE = "2026-01-01"
HISTORY_START_DATE = "1838-01-01"

MUNICIPALITY_CLASSIFICATION_ID = "131"
COUNTY_CLASSIFICATION_ID = "104"

CSV_COLUMNS = [
    "kommunenummer",
    "kommunenavn",
    "fylkesnummer",
    "fylkesnavn",
    "siste_endret",
]


def fetch_classification_codes(classification_id: str, as_of_date: str) -> list[dict[str, str]]:
    params = urllib.parse.urlencode({"from": HISTORY_START_DATE, "to": as_of_date})
    url = f"https://data.ssb.no/api/klass/v1/classifications/{classification_id}/codes?{params}"
    request = urllib.request.Request(url, headers={"Accept": "application/json"})

    with urllib.request.urlopen(request, timeout=30) as response:
        data = json.load(response)

    return data["codes"]


def display_name(name: str) -> str:
    """Use the Norwegian first name where SSB includes parallel official names."""
    return name.split(" - ", 1)[0]


def is_current(code: dict[str, str], as_of_date: str) -> bool:
    return code.get("validToInRequestedRange") == as_of_date


def valid_from_year(code: dict[str, str]) -> str:
    valid_from = code.get("validFromInRequestedRange", "")
    return valid_from[:4]


def build_rows_for_current_municipalities(as_of_date: str = AS_OF_DATE) -> list[dict[str, str]]:
    municipality_codes = fetch_classification_codes(MUNICIPALITY_CLASSIFICATION_ID, as_of_date)
    county_codes = fetch_classification_codes(COUNTY_CLASSIFICATION_ID, as_of_date)

    counties = {
        county["code"]: county
        for county in county_codes
        if is_current(county, as_of_date)
    }

    rows: list[dict[str, str]] = []
    for municipality in municipality_codes:
        if not is_current(municipality, as_of_date):
            continue

        municipality_id = municipality["code"]
        county_id = municipality_id[:2]

        if county_id == "99":
            continue

        changed_years = [
            valid_from_year(municipality),
            valid_from_year(counties[county_id]),
        ]
        last_changed_year = max((year for year in changed_years if year), default="")

        rows.append(
            {
                "kommunenummer": municipality_id,
                "kommunenavn": display_name(municipality["name"]),
                "fylkesnummer": county_id,
                "fylkesnavn": display_name(counties[county_id]["name"]),
                "siste_endret": last_changed_year,
            }
        )

    return sorted(rows, key=lambda row: row["kommunenummer"])


def validate_rows(rows: list[dict[str, str]]) -> None:
    seen: set[str] = set()

    for row in rows:
        municipality_id = row["kommunenummer"]
        county_id = row["fylkesnummer"]

        if municipality_id in seen:
            raise ValueError(f"Duplicate municipality: {municipality_id}")
        seen.add(municipality_id)

        if len(municipality_id) != 4:
            raise ValueError(f"Unexpected municipality id: {municipality_id}")
        if len(county_id) != 2:
            raise ValueError(f"Unexpected county id: {county_id}")
        if not row["kommunenavn"] or not row["fylkesnavn"]:
            raise ValueError(f"Missing name in row: {row}")


def write_csv(rows: list[dict[str, str]]) -> None:
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DIST_DIR / "norwegian_municipalities.csv"

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    rows = build_rows_for_current_municipalities()
    validate_rows(rows)
    write_csv(rows)
    print(f"Wrote {len(rows)} municipality rows to {DIST_DIR}")


if __name__ == "__main__":
    main()
