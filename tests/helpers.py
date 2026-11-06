"""Small builders for fake USAspending checkpoint CSVs. No network."""
import csv
from pathlib import Path

FIELDS = [
    "contract_transaction_unique_key", "contract_award_unique_key", "award_id_piid",
    "parent_award_id_piid", "modification_number", "transaction_number",
    "action_type_code", "action_date", "federal_action_obligation",
    "total_dollars_obligated", "awarding_agency_name", "recipient_name",
    "prime_award_base_transaction_description", "award_description",
    "transaction_description", "usaspending_permalink",
    "primary_place_of_performance_state_code", "type_of_set_aside",
]


def row(n: int, code: str = "F", fao="-1000.50", date="2026-03-02", key=None, mod="P00001", **extra):
    key = key or f"CONT_AWD_ABC{n:03d}_9700_-NONE-_-NONE-"
    r = {
        "contract_transaction_unique_key": f"{key}_{mod}_{n}",
        "contract_award_unique_key": key,
        "award_id_piid": f"ABC{n:03d}",
        "parent_award_id_piid": "",
        "modification_number": mod,
        "transaction_number": str(n),
        "action_type_code": code,
        "action_date": date,
        "federal_action_obligation": fao,
        "total_dollars_obligated": "5000",
        "awarding_agency_name": "Department of Testing",
        "recipient_name": f"VENDOR {n}",
        "prime_award_base_transaction_description": "base text",
        "award_description": "award text",
        "transaction_description": "txn text",
        "usaspending_permalink": f"https://www.usaspending.gov/award/{key}/",
        "primary_place_of_performance_state_code": "VA",
        "type_of_set_aside": "",
    }
    r.update(extra)
    return r


def write_checkpoint(directory: Path, name: str, rows: list[dict]) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)
    return path
