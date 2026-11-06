"""
publish_hf.py -- publish the termination table as a Hugging Face dataset.

Dry run by default: builds the files, checks them against the source CSVs,
prints a report, and uploads nothing. Add --upload for the real thing.

    python3 publish_hf.py                    # build + verify into build/hf/
    python3 publish_hf.py --check-summary    # also match web/data/summary.json
    HF_TOKEN=... python3 publish_hf.py --upload

Upload is ONE commit holding the Parquet table and the dataset card. The
dataset repo is created if it does not exist.

Run it after build_dashboard.py, from the repo root, with the checkpoint CSVs
in data/bulk_checkpoints/ (the workflow pulls them from R2 first).
"""

import argparse
import csv
import glob
import json
import os
import sys
from collections import Counter
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import build_dashboard as bd
import settings

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

OUT_DIR = Path("build/hf")
TABLE_PATH = "data/terminations.parquet"
CARD_PATH = "README.md"

# A smaller table than the one already published usually means missing agency
# files, not fewer terminations.
MAX_SHRINK = 0.005

STR, F64, DATE = pa.string(), pa.float64(), pa.date32()

# (column, source key in the enriched record, Arrow type, meaning)
COLUMNS = [
    ("transaction_key", "transaction_key", STR, "USAspending `contract_transaction_unique_key`. One row per transaction. This is the row id."),
    ("award_key", "key", STR, "USAspending `contract_award_unique_key`, the contract. `https://www.usaspending.gov/award/<award_key>/` opens it."),
    ("piid", "piid", STR, "Procurement instrument id of the contract or order."),
    ("parent_piid", "parent_piid", STR, "PIID of the parent vehicle, for orders under an IDV."),
    ("modification_number", "modification_number", STR, "Modification number, as text (`P00001`, `0003`)."),
    ("transaction_number", "transaction_number", STR, "Transaction number within the modification, as text."),
    ("termination_code", "action_type_code", STR, "`action_type_code`: E, F or X."),
    ("termination_reason", "termination_reason", STR, "**Derived.** The label for `termination_code` from `config.yaml`."),
    ("action_date", "action_date", DATE, "Date of the termination modification."),
    ("total_obligated", "total_obligated", F64, "`total_dollars_obligated` on the contract at that transaction, in dollars."),
    ("federal_action_obligation", "federal_action_obligation", F64, "Dollars this transaction added to the contract. Negative = money pulled back. Positive on about 0.5% of rows."),
    ("potential_total_value", "ceiling", F64, "`potential_total_value_of_award`, in dollars."),
    ("pop_start", "pop_start", DATE, "Period of performance start."),
    ("pop_end", "pop_end", DATE, "Period of performance current end."),
    ("awarding_agency", "department", STR, "Awarding agency."),
    ("awarding_sub_agency", "sub_agency", STR, "Awarding sub-agency."),
    ("awarding_office", "awarding_office", STR, "Awarding office."),
    ("funding_office", "funding_office", STR, "Funding office."),
    ("recipient_uei", "recipient_uei", STR, "Vendor's Unique Entity ID."),
    ("recipient_name", "recipient_name", STR, "Vendor name."),
    ("recipient_parent_name", "recipient_parent", STR, "Vendor's parent company name, as USAspending records it."),
    ("award_description", "award_description", STR, "Description of the award."),
    ("base_description", "base_description", STR, "`prime_award_base_transaction_description`."),
    ("transaction_description", "txn_description", STR, "Description of this transaction. Often the same as the others."),
    ("naics_code", "naics_code", STR, "NAICS code, as text."),
    ("naics_description", "naics_description", STR, "NAICS description."),
    ("psc_code", "psc_code", STR, "Product or service code, as text."),
    ("psc_description", "psc_description", STR, "PSC description."),
    ("pricing_code", "pricing_type", STR, "`type_of_contract_pricing_code`."),
    ("pricing_label", "pricing_label", STR, "`type_of_contract_pricing`."),
    ("set_aside", "set_aside", STR, "Set-aside type. **Derived:** `NONE` where the source is blank."),
    ("place_state", "place_state", STR, "Primary place of performance, state code."),
    ("usaspending_link", "usaspending_link", STR, "`usaspending_permalink`."),
]


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def _date(value):
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def table_rows(raw: list[dict]) -> list[dict]:
    """Shape enriched records (from build_dashboard) into dataset rows."""
    rows = []
    for rec in raw:
        row = {}
        for name, source, typ, _ in COLUMNS:
            value = rec.get(source)
            if typ == DATE:
                value = _date(value)
            elif typ == STR and value is not None:
                value = str(value)
            row[name] = value
        rows.append(row)
    rows.sort(key=lambda r: (r["action_date"] or date.min, r["transaction_key"] or ""), reverse=True)
    return rows


def to_arrow(rows: list[dict]) -> pa.Table:
    schema = pa.schema([(name, typ) for name, _, typ, _ in COLUMNS])
    return pa.Table.from_pylist(rows, schema=schema)


# ---------------------------------------------------------------------------
# Independent source check. Reads the checkpoint CSVs directly, not through
# build_dashboard, so it is a second path to the same numbers.
# ---------------------------------------------------------------------------

def source_index(checkpoint_dir: Path | None = None) -> dict:
    checkpoint_dir = checkpoint_dir or bd.CHECKPOINT_DIR   # read at call time, not import time
    codes = set(bd.TERMINATION_CODES)
    by_txn = {}
    no_key = 0
    for path in sorted(glob.glob(str(checkpoint_dir / "FY*.csv"))):
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                if (row.get("action_type_code") or "").strip().upper() not in codes:
                    continue
                if not (row.get("contract_award_unique_key") or "").strip():
                    no_key += 1
                    continue
                txn = (row.get("contract_transaction_unique_key") or "").strip()
                by_txn[txn] = (
                    (row.get("action_date") or "")[:10],
                    (row.get("action_type_code") or "").strip().upper(),
                    (row.get("modification_number") or "").strip(),
                    row.get("federal_action_obligation"),
                )
    return {"by_txn": by_txn, "no_key": no_key}


def verify(table: pa.Table, source: dict, summary: dict | None = None) -> list[str]:
    """Return a list of problems. Empty means every check passed."""
    problems = []
    n = table.num_rows
    by_txn = source["by_txn"]

    if n == 0:
        problems.append("the table has no rows")
    if n != len(by_txn):
        problems.append(f"table has {n:,} rows, the source files have {len(by_txn):,} termination transactions")

    keys = table.column("transaction_key").to_pylist()
    if any(not k for k in keys):
        problems.append("some rows have no transaction_key")
    if len(set(keys)) != len(keys):
        problems.append(f"transaction_key is not unique ({len(keys) - len(set(keys)):,} repeats)")
    if table.column("award_key").null_count:
        problems.append("some rows have no award_key")

    missing = [k for k in keys if k not in by_txn]
    if missing:
        problems.append(f"{len(missing):,} rows are not in the source files, e.g. {missing[0]}")

    mismatched = 0
    first = None
    cols = {c: table.column(c).to_pylist() for c in
            ("action_date", "termination_code", "modification_number", "federal_action_obligation")}
    for i, k in enumerate(keys):
        src = by_txn.get(k)
        if not src:
            continue
        s_date, s_code, s_mod, s_fao = src
        got_date = cols["action_date"][i]
        fao = cols["federal_action_obligation"][i]
        s_fao_num = float(s_fao) if s_fao not in (None, "") else None
        same = (
            (got_date.isoformat() if got_date else "") == s_date
            and cols["termination_code"][i] == s_code
            and (cols["modification_number"][i] or "") == s_mod
            and ((fao is None and s_fao_num is None) or
                 (fao is not None and s_fao_num is not None and abs(fao - s_fao_num) < 0.005))
        )
        if not same:
            mismatched += 1
            first = first or k
    if mismatched:
        problems.append(f"{mismatched:,} rows differ from the source on date, code, modification or amount, e.g. {first}")

    for name, _, typ, _ in COLUMNS:
        if table.schema.field(name).type != typ:
            problems.append(f"column {name} is {table.schema.field(name).type}, expected {typ}")

    if summary is not None and summary.get("total_terminations") != n:
        problems.append(
            f"table has {n:,} rows but web/data/summary.json says {summary.get('total_terminations'):,}"
        )
    return problems


# ---------------------------------------------------------------------------
# Dataset card
# ---------------------------------------------------------------------------

def build_card(table: pa.Table, as_of: str, no_key_dropped: int) -> str:
    n = table.num_rows
    dates = [d for d in table.column("action_date").to_pylist() if d]
    reasons = Counter(table.column("termination_reason").to_pylist())
    awards = len(set(table.column("award_key").to_pylist()))
    agencies = len({a for a in table.column("awarding_agency").to_pylist() if a})
    fy = bd.CONFIG["fetch"]["fiscal_years"]
    code_rows = "\n".join(f"| `{c}` | {label} |" for c, label in bd.TERMINATION_CODES.items())
    reason_rows = "\n".join(f"| {r} | {c:,} |" for r, c in reasons.most_common())
    col_rows = "\n".join(f"| `{name}` | {desc} |" for name, _, _, desc in COLUMNS)
    return f"""---
license: other
license_name: us-govt-public-domain
language:
- en
tags:
- government
- procurement
- federal-contracting
- usaspending
pretty_name: Federal contract terminations
size_categories:
- 10K<n<100K
configs:
- config_name: default
  data_files: {TABLE_PATH}
---

# Federal contract terminations

Every termination modification on a federal prime contract, fiscal years {fy[0]} to {fy[-1]}, from
[USAspending](https://www.usaspending.gov/)'s Award Data Archive. One row per transaction, with the
reason (convenience, default or cause), the dollars pulled back, the agency and the vendor.

Live site with filters: {settings.SITE_URL}. Code: {settings.GITHUB_URL}.

## As of

* Built **{as_of}**. Rebuilt monthly, after USAspending republishes its archive.
* {n:,} transactions on {awards:,} contracts, from {agencies:,} agencies.
* Termination dates run from **{min(dates)}** to **{max(dates)}**.
* Not the same as "every termination ever": see Caveats.

| Reason | Rows |
|---|---|
{reason_rows}

## File

`{TABLE_PATH}`: the table. ID and code columns are text, so leading zeros survive. Dates are dates.
Amounts are dollars, to the cent.

## How a termination is identified

USAspending gives every contract transaction an `action_type_code`. Three codes mean a termination:

| Code | Reason |
|---|---|
{code_rows}

A row is kept when its code is one of those. Everything else in the archive is dropped. `termination_reason`
is the label above. Nothing else is inferred: the reason is the code the contracting officer reported.

## Columns

| Column | Meaning |
|---|---|
{col_rows}

Everything is copied from the USAspending file as published, except the two columns marked **derived**.

## Caveats

* **Prime contracts only.** Subawards, grants and loans are not here.
* **One row per transaction.** A contract with a partial termination, a rescission and a re-filing has several
  rows. Count contracts with `award_key`, not rows. `(award_key, modification_number)` can repeat across
  transactions, so use `transaction_key` as the row id.
* **Money can go either way.** `federal_action_obligation` is negative where money was pulled back, positive
  where the modification added money (settlements, corrections).
* **Convenience is not fault.** Termination for convenience says the government chose to end it. It says nothing
  about the contractor's performance.
* **Late reports.** Agencies report modifications after the fact. A termination appears only after the archive
  refresh that follows its report, so recent months fill in later.
* **Dropped rows.** Termination rows with no `contract_award_unique_key` are dropped: {no_key_dropped:,} in this build.
* **Only fiscal years {", ".join(str(y) for y in fy)}.**

## License

US government works are in the public domain. This table is a filtered copy of USAspending data and carries no
additional restrictions. It is not an official USAspending product.
"""


# ---------------------------------------------------------------------------
# Hugging Face
# ---------------------------------------------------------------------------

def published_rows(api, repo_id: str) -> int:
    """Row count of the table already on the Hub. 0 for a new repo or one with no table yet."""
    from huggingface_hub import hf_hub_download
    from huggingface_hub.errors import EntryNotFoundError, RepositoryNotFoundError
    try:
        path = hf_hub_download(repo_id, TABLE_PATH, repo_type="dataset", token=os.environ.get("HF_TOKEN"))
    except (EntryNotFoundError, RepositoryNotFoundError):
        return 0
    return pq.ParquetFile(path).metadata.num_rows


def upload(out_dir: Path, new_rows: int, allow_shrink: bool = False, api=None) -> str:
    from huggingface_hub import CommitOperationAdd, HfApi
    api = api or HfApi(token=os.environ["HF_TOKEN"])
    repo_id = settings.HF_DATASET
    api.create_repo(repo_id, repo_type="dataset", exist_ok=True)
    before = published_rows(api, repo_id)
    if before and new_rows < before * (1 - MAX_SHRINK) and not allow_shrink:
        sys.exit(f"Refusing to upload: {new_rows:,} rows would replace {before:,} already published "
                 f"(more than {100 * MAX_SHRINK:.1f}% smaller). Use --allow-shrink if that is right.")
    info = api.create_commit(
        repo_id=repo_id,
        repo_type="dataset",
        operations=[
            CommitOperationAdd(TABLE_PATH, str(out_dir / TABLE_PATH)),
            CommitOperationAdd(CARD_PATH, str(out_dir / CARD_PATH)),
        ],
        commit_message=f"Rebuild terminations table ({new_rows:,} rows, as of {bd.TODAY_STR})",
    )
    return info.commit_url


# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=OUT_DIR, help="where to write the files (default build/hf)")
    ap.add_argument("--upload", action="store_true", help="commit to Hugging Face (needs HF_TOKEN)")
    ap.add_argument("--check-summary", action="store_true",
                    help="require the row count to equal web/data/summary.json (use right after build_dashboard.py)")
    ap.add_argument("--allow-shrink", action="store_true", help="upload even if the table is smaller than the published one")
    args = ap.parse_args(argv)

    raw = bd.enrich_contracts(bd.stream_and_aggregate())
    rows = table_rows(raw)
    table = to_arrow(rows)

    source = source_index()
    summary = json.loads((bd.WEB_DATA_DIR / "summary.json").read_text()) if args.check_summary else None
    problems = verify(table, source, summary)

    out = args.out
    (out / "data").mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out / TABLE_PATH, compression="zstd", row_group_size=50_000)
    (out / CARD_PATH).write_text(build_card(table, bd.TODAY_STR, source["no_key"]))

    size = (out / TABLE_PATH).stat().st_size
    dates = [d for d in table.column("action_date").to_pylist() if d]
    print(f"\n{settings.HF_DATASET}")
    print(f"  rows            {table.num_rows:,}")
    print(f"  source rows     {len(source['by_txn']):,}  (no key, dropped: {source['no_key']:,})")
    print(f"  contracts       {len(set(table.column('award_key').to_pylist())):,}")
    print(f"  date range      {min(dates)} to {max(dates)}")
    print(f"  {TABLE_PATH}  {size / 1e6:.1f} MB")
    print(f"  {CARD_PATH}  {(out / CARD_PATH).stat().st_size / 1e3:.1f} KB")
    print(f"  written to      {out}/")

    if problems:
        print("\nVERIFICATION FAILED")
        for p in problems:
            print(f"  - {p}")
        return 1
    print("  verified: row count, unique transaction_key, date/code/modification/amount match the source files")

    if not args.upload:
        print("\nDry run: nothing uploaded. Add --upload to publish.")
        return 0
    print(f"\nUploaded: {upload(out, table.num_rows, args.allow_shrink)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
