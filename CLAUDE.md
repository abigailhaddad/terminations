# terminations

Dashboard of every federal contract termination modification since FY2025,
sourced from USASpending bulk archives. Filters at ingestion to the three
`action_type_code` values that indicate termination: `E` (Default), `F`
(Convenience), `X` (Cause). Every termination mod becomes its own row, so
contracts with multiple termination mods (partial terminations, rescissions,
re-filings) appear multiple times.

Forked from the `dod-contract-vehicles` repo and reuses its frontend (same
`shared.css`, `shared.js`, Bootstrap 5 + DataTables + Chart.js stack).

## Data pipeline (run in order)

### Step 1 -- `fetch_awards.py`
Downloads transaction-level contract records from USASpending bulk archives.
- Fetches the toptier-agency list dynamically from
  `files.usaspending.gov/reference_data/agency_codes.csv`
- Downloads one ZIP per agency per fiscal year
- **Filters rows at ingestion** to `action_type_code in {E, F, X}` -- every
  non-termination row is discarded before the checkpoint CSV is written
- Checkpoints per agency/FY at `data/bulk_checkpoints/FY{year}_{code}.csv`
- Resume-safe: re-running skips completed files

```bash
python3 fetch_awards.py                    # all agencies, FY from config.yaml
python3 fetch_awards.py --fy 2026          # one year
python3 fetch_awards.py --agencies 097 036 # specific agencies
python3 fetch_awards.py --force-current-fy # refresh just current FY
```

### Step 2 -- `build_dashboard.py`
Streams termination rows, one record per termination modification (a contract
with N termination mods produces N records). Builds dashboard JSONs.
- `federal_action_obligation` preserved with USASpending's sign convention: negative =
  money pulled back (the normal termination case), positive = mod added money
  (settlements / rescissions, ~0.5% of rows). Summary exposes `net_dollar_change`
  as the signed sum.
- Output: `web/data/{terminations.json, summary.json, filters.json, config.json}`

## Config

`config.yaml` drives both scripts:

```yaml
fetch:
  fiscal_years: [2025, 2026]
  termination_codes: {E: ..., F: ..., X: ...}

labels:
  pricing_types: {J: Firm Fixed Price, ...}
```

## `terminations.json` format

The file is **columnar and dictionary-encoded**, not a JSON array of row
objects. `build_dashboard.py` writes it via `encode_terminations()`;
`decodeTerminations()` in `web/index.html` turns it back into plain row
objects before anything else touches it. `decode_terminations()` in
`build_dashboard.py` is the Python mirror, used by `run_checks.py`.

```jsonc
{
  "v": 2,
  "n": 67702,
  "dicts": { "department": ["Department of Agriculture", ...],
             "_text": ["<description / mod_note strings>", ...], ... },
  "cols":  { "department": [8, 8, 21, ...],   // index into dicts.department, -1 = null
             "description": [412, 9, ...],    // index into dicts._text
             "key": ["CONT_AWD_...", ...],    // stored verbatim
             "total_obligated": [null, 201959, ...] }
}
```

This is not premature optimization: the plain array-of-objects form was 91 MB
and **Cloudflare Pages hard-rejects any single file over 25 MiB**, so the site
could not be deployed at all. The encoding gets it to ~10.4 MiB. See the
comment block above `encode_terminations()` for what the 91 MB consisted of.

Decoded fields — this is exactly what the dashboard sees:

| Field | Meaning |
|---|---|
| `key` | `contract_award_unique_key` (USASpending unique ID); stored verbatim |
| `piid` | **Derived** from `key` (3rd `_`-delimited segment), not stored |
| `link` | **Derived**: `https://www.usaspending.gov/award/{key}/`, not stored |
| `termination_reason` | Human-friendly label (`termination_code` is not shipped) |
| `termination_date` | `action_date` of the termination mod |
| `total_obligated` | Cumulative `total_dollars_obligated` at termination |
| `federal_action_obligation` | Raw signed value from USASpending on this termination mod (negative = pulled back) |
| `contractor` | Recipient name |
| `department`, `sub_agency` | Issuing agency |
| `description`, `mod_note` | Free text, pooled in `dicts._text` (identical on 77% of rows) |
| `naics`, `psc` | NAICS + PSC codes |
| `pricing` | Contract pricing type (FFP, T&M, etc.) |
| `set_aside` | Set-aside type |
| `state` | Primary place of performance state, `"Unknown"` when absent |

Deliberately **not** shipped, because nothing in `web/` reads them:
`mod_number`, `termination_code`, `ceiling`, `contractor_parent`,
`parent_piid`, `awarding_office`, `funding_office`, `naics_desc`, `psc_desc`,
`pop_start`, `pop_end`. They are still built by `build_contracts_json()`; only
`encode_terminations()` drops them. To surface one in the UI, add it to
`DICT_FIELDS` / `TEXT_FIELDS` / `NUM_FIELDS` in `build_dashboard.py` **and** to
`decodeTerminations()` in `web/index.html` — `run_checks.py` fails the build if
the table renders a field the payload does not carry.

## Checks

`python3 run_checks.py` -- runs against the built `web/`, wired into the
workflow between the build and the commit. Enforces the Cloudflare Pages
25 MiB per-file limit, the presence of `web/404.html`, payload well-formedness
and encode/decode round-tripping, and that no filter list comes out empty.

## Frontend

`web/index.html` uses the same `ServerSideFilterManager` (subclassed as
`TermFilterManager`) as `dod-contract-vehicles`, same DataTables setup, same
color palette. All filtering is client-side against the data loaded from
`web/data/terminations.json`.

Charts: top agencies by termination count, count by reason.
Stats: total terminations, Convenience / Default / Cause counts, total deobligated.

## Deployment

Static files out of `web/`. Commit `web/data/` after each build.

Moving from Vercel to Cloudflare Pages (publish directory `web/`, no build
command). Two Pages constraints shape this repo:
- **25 MiB per file, hard.** Enforced by `run_checks.py`; see the
  `terminations.json` format section.
- **No default 404.** Without `web/404.html`, Pages serves `index.html` with
  HTTP 200 for every unmatched path.

`vercel.json` / `.vercelignore` are still present and untouched so the current
Vercel deployment keeps working until the cutover.

## Caveats

- Contract data is contractor- and contracting-officer-reported; late
  modifications show up only after the next bulk archive refresh.
- Terminations "for convenience" are not necessarily contractor-fault.
- `total_obligated` reflects the value at termination time, not any
  subsequent adjustments.
- Grants, loans, and non-contract awards are out of scope -- this is
  procurement contracts only.
