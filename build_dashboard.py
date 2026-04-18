"""
build_dashboard.py -- Aggregate termination data and build dashboard JSONs.

Streams the termination rows (one row per termination modification) and
reduces to one record per contract_award_unique_key, keeping the most severe
termination action (Default/Cause > Convenience). Emits JSON to web/data/.

Run:
    python3 build_dashboard.py
"""

import csv
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

import yaml

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

BULK_CSV       = Path("data/terminations_bulk.csv")
CHECKPOINT_DIR = Path("data/bulk_checkpoints")
WEB_DATA_DIR   = Path("web/data")
CONFIG_PATH    = Path("config.yaml")

TODAY = date.today()
TODAY_STR = TODAY.isoformat()

with open(CONFIG_PATH) as _f:
    CONFIG = yaml.safe_load(_f)

TERMINATION_CODES = dict(CONFIG["fetch"]["termination_codes"])   # {code: label}
PRICING_LABELS    = dict(CONFIG["labels"]["pricing_types"])


def _val(row: dict, key: str) -> str | None:
    v = row.get(key, "")
    if v and str(v).strip() and str(v).strip().lower() not in ("nan", "none", ""):
        return str(v).strip()
    return None


def _float(row: dict, key: str) -> float | None:
    v = _val(row, key)
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _stream_csv(path: Path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        yield from csv.DictReader(f)


def _maybe_hydrate_from_r2() -> None:
    """If no local data and R2 credentials are set, pull checkpoints from R2."""
    if BULK_CSV.exists() and BULK_CSV.stat().st_size > 0:
        return
    if any(cp.stat().st_size > 0 for cp in CHECKPOINT_DIR.glob("FY*.csv")):
        return
    import os as _os
    if not _os.environ.get("CF_R2_ACCOUNT_ID"):
        return
    print("No local data found -- hydrating checkpoints from R2...")
    import r2_sync
    r2_sync.download_state(CHECKPOINT_DIR)


def _pick_sources() -> list[Path]:
    """Prefer merged bulk CSV; fall back to per-agency checkpoints."""
    _maybe_hydrate_from_r2()
    if BULK_CSV.exists() and BULK_CSV.stat().st_size > 0:
        return [BULK_CSV]
    cps = sorted(CHECKPOINT_DIR.glob("FY*.csv"))
    return [cp for cp in cps if cp.stat().st_size > 0]


def stream_and_aggregate() -> list[dict]:
    """One record per termination modification. A contract can have multiple
    termination mods (partial terminations, re-filings, etc.) and we keep
    every one of them."""
    sources = _pick_sources()
    if not sources:
        raise FileNotFoundError("No data found -- run fetch_awards.py first.")
    print(f"Reading from {len(sources)} source(s)...")

    records: list[dict] = []
    total_rows = 0
    skipped_no_key = 0

    for path in sources:
        print(f"  Streaming {path.name}...", end=" ", flush=True)
        file_rows = 0
        for row in _stream_csv(path):
            total_rows += 1
            file_rows += 1
            if total_rows % 100_000 == 0:
                print(f"{total_rows // 1000}k...", end=" ", flush=True)

            key = _val(row, "contract_award_unique_key")
            if not key:
                skipped_no_key += 1
                continue

            action = (_val(row, "action_type_code") or "").upper()
            if action not in TERMINATION_CODES:
                continue

            fao = _float(row, "federal_action_obligation")
            records.append({
                "key":                      key,
                "piid":                     _val(row, "award_id_piid"),
                "parent_piid":              _val(row, "parent_award_id_piid"),
                "modification_number":      _val(row, "modification_number"),
                "action_type_code":         action,
                "action_type":              _val(row, "action_type") or TERMINATION_CODES.get(action),
                "action_date":              _val(row, "action_date") or "",
                "total_obligated":          _float(row, "total_dollars_obligated"),
                # Signed: matches FPDS federal_action_obligation exactly.
                # Negative = money pulled back (the normal termination case);
                # positive = termination mod that added money (settlements /
                # rescissions / accounting corrections, ~0.5% of rows).
                "federal_action_obligation": round(fao, 2) if fao is not None else None,
                "ceiling":                  _float(row, "potential_total_value_of_award"),
                "pop_start":                _val(row, "period_of_performance_start_date"),
                "pop_end":                  _val(row, "period_of_performance_current_end_date"),
                "department":               _val(row, "awarding_agency_name"),
                "sub_agency":               _val(row, "awarding_sub_agency_name"),
                "awarding_office":          _val(row, "awarding_office_name"),
                "funding_office":           _val(row, "funding_office_name"),
                "recipient_uei":            _val(row, "recipient_uei"),
                "recipient_name":           _val(row, "recipient_name"),
                "recipient_parent":         _val(row, "recipient_parent_name"),
                "award_description":        _val(row, "award_description"),
                "base_description":         _val(row, "prime_award_base_transaction_description"),
                "txn_description":          _val(row, "transaction_description"),
                "naics_code":               _val(row, "naics_code"),
                "naics_description":        _val(row, "naics_description"),
                "psc_code":                 _val(row, "product_or_service_code"),
                "psc_description":          _val(row, "product_or_service_code_description"),
                "pricing_type":             _val(row, "type_of_contract_pricing_code"),
                "pricing_label":            _val(row, "type_of_contract_pricing"),
                "set_aside":                _val(row, "type_of_set_aside") or _val(row, "type_of_set_aside_code") or "NONE",
                "place_state":              _val(row, "primary_place_of_performance_state_code"),
                "usaspending_link":         _val(row, "usaspending_permalink"),
            })

        print(f"{file_rows:,} rows")

    unique_contracts = len({r["key"] for r in records})
    print(f"  Total: {total_rows:,} termination rows, "
          f"{skipped_no_key:,} skipped (no key), {len(records):,} termination mods across {unique_contracts:,} contracts")

    return records


def _best_description(c: dict) -> str | None:
    cands = [c.get("base_description"), c.get("award_description"), c.get("txn_description")]
    cands = [s for s in cands if s]
    if not cands:
        return None
    return max(cands, key=len)


def enrich_contracts(records: list) -> list:
    for c in records:
        c["pricing_type_label"] = PRICING_LABELS.get(c.get("pricing_type") or "", c.get("pricing_label"))
        c["description"] = _best_description(c)
        c["termination_reason"] = TERMINATION_CODES.get(c.get("action_type_code") or "", c.get("action_type"))
    return records


def build_contracts_json(records: list) -> list:
    out = []
    for c in records:
        fao = c.get("federal_action_obligation")
        out.append({
            "key":                   c["key"],
            "piid":                  c.get("piid"),
            "parent_piid":           c.get("parent_piid"),
            "mod_number":            c.get("modification_number"),
            "termination_code":      c.get("action_type_code"),
            "termination_reason":    c.get("termination_reason"),
            "termination_date":      (c.get("action_date") or "")[:10] or None,
            "total_obligated":       round(c["total_obligated"]) if c.get("total_obligated") else None,
            "federal_action_obligation": round(fao) if fao is not None else None,
            "ceiling":               round(c["ceiling"]) if c.get("ceiling") else None,
            "contractor":            c.get("recipient_name"),
            "contractor_parent":     c.get("recipient_parent"),
            "department":            c.get("department"),
            "sub_agency":            c.get("sub_agency"),
            "awarding_office":       c.get("awarding_office"),
            "funding_office":        c.get("funding_office"),
            "description":           c.get("description"),
            "naics":                 c.get("naics_code"),
            "naics_desc":            c.get("naics_description"),
            "psc":                   c.get("psc_code"),
            "psc_desc":              c.get("psc_description"),
            "pricing":               c.get("pricing_type_label"),
            "set_aside":             c.get("set_aside"),
            "pop_start":             (c.get("pop_start") or "")[:10] or None,
            "pop_end":               (c.get("pop_end") or "")[:10] or None,
            "place_state":           c.get("place_state"),
            "link":                  c.get("usaspending_link"),
        })

    # Sort by termination_date desc (most recent first).
    out.sort(key=lambda r: r["termination_date"] or "", reverse=True)
    return out


def build_summary(records: list) -> dict:
    by_reason = defaultdict(int)
    net_change = 0.0
    contractors = set()
    agencies = set()
    contracts = set()
    latest_term_date = ""
    for r in records:
        by_reason[r.get("termination_reason") or "Unknown"] += 1
        fao = r.get("federal_action_obligation")
        if fao is not None:
            net_change += fao
        if r.get("contractor"):
            contractors.add(r["contractor"])
        if r.get("department"):
            agencies.add(r["department"])
        contracts.add(r["key"])
        td = r.get("termination_date") or ""
        if td and td > latest_term_date:
            latest_term_date = td

    return {
        "total_terminations":       len(records),
        "unique_contracts":         len(contracts),
        "by_reason":                dict(by_reason),
        # Signed net sum of federal_action_obligation across all termination
        # mods. Negative = money net pulled back (the expected sign).
        "net_dollar_change":        round(net_change),
        "unique_contractors":       len(contractors),
        "unique_agencies":          len(agencies),
        "built_at":                 datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
        "latest_termination_date":  latest_term_date[:10] or None,
        "as_of":                    TODAY_STR,
    }


def build_filter_options(records: list) -> dict:
    def unique_sorted(field: str, limit: int = 500) -> list:
        vals = {r.get(field) for r in records if r.get(field)}
        return sorted(vals)[:limit]

    reasons = sorted({r["termination_reason"] for r in records if r.get("termination_reason")})
    naics_2 = sorted({r["naics"][:2] for r in records if r.get("naics") and len(r["naics"]) >= 2})
    return {
        "termination_reasons":  reasons,
        "departments":          unique_sorted("department"),
        "sub_agencies":         unique_sorted("sub_agency"),
        "pricing_types":        unique_sorted("pricing"),
        "set_asides":           unique_sorted("set_aside"),
        "states":               unique_sorted("place_state"),
        "naics_2digit":         naics_2,
    }


def build_config_mirror() -> dict:
    return {
        "as_of":              TODAY_STR,
        "fetch":              CONFIG["fetch"],
        "termination_codes":  TERMINATION_CODES,
    }


def main():
    sources = _pick_sources()
    if not sources:
        print("No data found -- run fetch_awards.py first.")
        return

    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    raw = stream_and_aggregate()
    raw = enrich_contracts(raw)

    print("\nBuilding dashboard JSONs...")
    records = build_contracts_json(raw)
    print(f"  terminations.json: {len(records):,} records")

    summary = build_summary(records)
    print(f"  summary.json: {summary['total_terminations']:,} terminations, "
          f"net ${summary['net_dollar_change']/1e9:.2f}B (negative = pulled back)")

    filters = build_filter_options(records)
    config_mirror = build_config_mirror()

    outputs = {
        "terminations.json": records,
        "summary.json":      summary,
        "filters.json":      filters,
        "config.json":       config_mirror,
    }

    for fname, data in outputs.items():
        path = WEB_DATA_DIR / fname
        path.write_text(json.dumps(data, indent=2, default=str))
        print(f"  Wrote {path}")

    print("\nDone. Commit web/data/ to deploy.")


if __name__ == "__main__":
    main()
