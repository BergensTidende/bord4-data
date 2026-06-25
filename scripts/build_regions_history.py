#!/usr/bin/env python3
"""Build historical lookup rows for Norwegian counties and municipalities."""

from __future__ import annotations

import csv

from build_current_municipalities import (
    AS_OF_DATE,
    COUNTY_CLASSIFICATION_ID,
    DIST_DIR,
    MUNICIPALITY_CLASSIFICATION_ID,
    display_name,
    fetch_classification_codes,
)


CSV_COLUMNS = [
    "nivå",
    "regionnummer",
    "regionnavn",
    "fylkesnummer",
    "fylkesnavn",
    "gyldig_fra",
    "gyldig_til",
    "er_gjeldende",
]


def valid_from(code: dict[str, str]) -> str:
    return code.get("validFromInRequestedRange", "")


def valid_to(code: dict[str, str]) -> str:
    valid_to_value = code.get("validToInRequestedRange", "")
    return "" if valid_to_value == AS_OF_DATE else valid_to_value


def is_current(code: dict[str, str]) -> str:
    return str(code.get("validToInRequestedRange") == AS_OF_DATE).lower()


def overlaps(left: dict[str, str], right: dict[str, str]) -> bool:
    left_from = valid_from(left)
    left_to = code_end(left)
    right_from = valid_from(right)
    right_to = code_end(right)

    return left_from < right_to and right_from < left_to


def code_end(code: dict[str, str]) -> str:
    return code.get("validToInRequestedRange", "") or AS_OF_DATE


def county_for_municipality(
    municipality: dict[str, str],
    counties_by_id: dict[str, list[dict[str, str]]],
) -> dict[str, str]:
    county_id = municipality["code"][:2]
    candidates = counties_by_id.get(county_id, [])

    for county in candidates:
        if overlaps(municipality, county):
            return county

    if candidates:
        return sorted(candidates, key=valid_from)[0]

    raise ValueError(f"Could not find county for municipality {municipality['code']}")


def county_row(county: dict[str, str]) -> dict[str, str]:
    county_id = county["code"]
    county_name = display_name(county["name"])

    return {
        "nivå": "fylke",
        "regionnummer": county_id,
        "regionnavn": county_name,
        "fylkesnummer": county_id,
        "fylkesnavn": county_name,
        "gyldig_fra": valid_from(county),
        "gyldig_til": valid_to(county),
        "er_gjeldende": is_current(county),
    }


def municipality_row(
    municipality: dict[str, str],
    counties_by_id: dict[str, list[dict[str, str]]],
) -> dict[str, str]:
    county = county_for_municipality(municipality, counties_by_id)

    return {
        "nivå": "kommune",
        "regionnummer": municipality["code"],
        "regionnavn": display_name(municipality["name"]),
        "fylkesnummer": county["code"],
        "fylkesnavn": display_name(county["name"]),
        "gyldig_fra": valid_from(municipality),
        "gyldig_til": valid_to(municipality),
        "er_gjeldende": is_current(municipality),
    }


def build_rows() -> list[dict[str, str]]:
    counties = fetch_classification_codes(COUNTY_CLASSIFICATION_ID, AS_OF_DATE)
    municipalities = fetch_classification_codes(MUNICIPALITY_CLASSIFICATION_ID, AS_OF_DATE)
    counties_by_id: dict[str, list[dict[str, str]]] = {}

    counties = [county for county in counties if county["code"] != "99"]
    municipalities = [
        municipality
        for municipality in municipalities
        if not municipality["code"].startswith("99")
    ]

    for county in counties:
        counties_by_id.setdefault(county["code"], []).append(county)

    rows = [county_row(county) for county in counties]
    rows.extend(municipality_row(municipality, counties_by_id) for municipality in municipalities)

    return sorted(
        rows,
        key=lambda row: (
            row["nivå"],
            row["regionnummer"],
            row["gyldig_fra"],
            row["gyldig_til"],
        ),
    )


def validate_rows(rows: list[dict[str, str]]) -> None:
    seen: set[tuple[str, str, str, str]] = set()

    for row in rows:
        key = (
            row["nivå"],
            row["regionnummer"],
            row["gyldig_fra"],
            row["gyldig_til"],
        )
        if key in seen:
            raise ValueError(f"Duplicate history row: {key}")
        seen.add(key)

        if row["nivå"] not in {"fylke", "kommune"}:
            raise ValueError(f"Unexpected level: {row}")
        if row["nivå"] == "fylke" and len(row["regionnummer"]) != 2:
            raise ValueError(f"Unexpected county id: {row}")
        if row["nivå"] == "kommune" and len(row["regionnummer"]) != 4:
            raise ValueError(f"Unexpected municipality id: {row}")
        if len(row["fylkesnummer"]) != 2:
            raise ValueError(f"Unexpected county id: {row}")
        if row["er_gjeldende"] not in {"true", "false"}:
            raise ValueError(f"Unexpected current flag: {row}")


def write_csv(rows: list[dict[str, str]]) -> None:
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DIST_DIR / "norwegian_regions_history.csv"

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    rows = build_rows()
    validate_rows(rows)
    write_csv(rows)
    print(f"Wrote {len(rows)} region history rows to {DIST_DIR}")


if __name__ == "__main__":
    main()
