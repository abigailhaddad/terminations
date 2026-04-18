# terminations

Dashboard of every federal contract terminated since FY2025, sourced from
USASpending bulk archives (FPDS). Filters at ingestion to the three FPDS
`action_type_code` values that indicate termination: `E` (Default), `F`
(Convenience), `X` (Cause).

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
- May be IP-blocked after ~50 agencies; re-run from a new IP to continue

```bash
python3 fetch_awards.py                    # all agencies, FY from config.yaml
python3 fetch_awards.py --fy 2026          # one year
python3 fetch_awards.py --agencies 097 036 # specific agencies
python3 fetch_awards.py --force-current-fy # refresh just current FY
```

### Step 2 -- `build_dashboard.py`
Streams termination rows, reduces to one record per
`contract_award_unique_key`, builds dashboard JSONs.
- Most-severe-wins: Default/Cause (severity 2) outrank Convenience (severity 1)
- Tie-breaker: latest `action_date`
- `termination_deobligated` = sum of `federal_action_obligation` across all
  termination mods for the contract
- Output: `web/data/{terminations.json, summary.json, filters.json, config.json}`

## Config

`config.yaml` drives both scripts:

```yaml
fetch:
  fiscal_years: [2025, 2026]
  termination_codes: {E: ..., F: ..., X: ...}

labels:
  termination_severity: {X: 2, E: 2, F: 1}
  pricing_types: {J: Firm Fixed Price, ...}
```

## Key fields in `terminations.json`

| Field | Meaning |
|---|---|
| `key` | `contract_award_unique_key` (USASpending unique ID) |
| `piid` | Procurement Instrument Identifier |
| `mod_number` | Modification number of the termination mod |
| `termination_code` | `E` / `F` / `X` |
| `termination_reason` | Human-friendly label |
| `termination_date` | `action_date` of the termination mod |
| `total_obligated` | Cumulative `total_dollars_obligated` at termination |
| `termination_deobligated` | Sum of `federal_action_obligation` across all termination mods |
| `ceiling` | `potential_total_value_of_award` |
| `contractor`, `contractor_parent` | Recipient name + parent |
| `department`, `sub_agency`, `awarding_office` | Issuing agency |
| `naics`, `psc` | NAICS + PSC codes |
| `pricing` | Contract pricing type (FFP, T&M, etc.) |
| `set_aside` | Set-aside type |
| `place_state` | Primary place of performance state |
| `link` | USASpending permalink |

## Frontend

`web/index.html` uses the same `ServerSideFilterManager` (subclassed as
`TermFilterManager`) as `dod-contract-vehicles`, same DataTables setup, same
color palette. All filtering is client-side against the data loaded from
`web/data/terminations.json`.

Charts: top agencies by termination count, count by reason.
Stats: total terminations, Convenience / Default / Cause counts, total deobligated.

## Deployment

Vercel: `vercel.json` routes `/ → web/`. Commit `web/data/` after each build.

## Caveats

- FPDS is contractor- and contracting-officer-reported; late modifications
  show up only after the next bulk archive refresh.
- Terminations "for convenience" are not necessarily contractor-fault.
- `total_obligated` reflects the value at termination time, not any
  subsequent adjustments.
- Grants, loans, and non-contract awards are out of scope -- this is
  FPDS-only.
