# Federal Contract Terminations

A searchable dashboard of every federal contract terminated since FY2025 — with the
reason (Default, Convenience, or Cause), deobligated amount, agency, and vendor.
Data from the USASpending bulk archives, which source from FPDS.

**[View the dashboard](#)** (deploy to Vercel)

## How it works

```
fetch_awards.py         USASpending bulk archives → data/terminations_bulk.csv
                        (downloads all toptier-agency ZIPs for FY2025–FY2026,
                         streams each CSV, keeps only rows where action_type_code
                         is E / F / X — every other modification is discarded)

build_dashboard.py      bulk CSV → web/data/*.json
                        (reduces to one row per contract, keeps the most severe
                         termination, sums deobligated amount, outputs JSONs)
```

## Quick start

```bash
pip install -r requirements.txt

python3 fetch_awards.py              # FY2025 + FY2026, all toptier agencies
python3 build_dashboard.py           # build web/data/*.json
cd web && python3 -m http.server     # view at http://localhost:8000
```

The first `fetch_awards.py` run takes a while (one ZIP per toptier agency per
fiscal year, often several GB each). Re-runs are fast — finished agency/FY pairs
are checkpointed at `data/bulk_checkpoints/` and skipped.

If USASpending IP-blocks mid-run, the script stops cleanly and saves progress;
re-run from a new IP to continue.

## Termination codes

| Code | Reason                     | Meaning |
|------|----------------------------|---------|
| `F`  | Terminate for Convenience  | Government ended the contract because it was in the government's interest. No contractor fault. |
| `E`  | Terminate for Default      | Contractor failed to perform. (Commercial-items / simplified acquisition.) |
| `X`  | Terminate for Cause        | Contractor failed to perform. (Non-commercial.) |

When a contract has multiple termination mods, the most severe wins (Default /
Cause > Convenience).

## Config

Edit `config.yaml` to change fiscal years or termination codes.

## CI/CD

`.github/workflows/fetch.yml` mirrors the `dod-contract-vehicles` pattern:

- Monthly schedule + manual `workflow_dispatch`
- Checkpoints persist on Cloudflare R2 between runs (prefix `terminations/`)
- **Self-chains on IP block**: when USASpending stops answering mid-run, the
  fetch job kicks off a new run with `gh workflow run fetch.yml`, which lands
  on a new runner IP and picks up where the last one left off.
- On `status=done`, the build job runs `build_dashboard.py` and commits the
  refreshed `web/data/*.json` back to `main` (Vercel auto-deploys).

Required secrets (same ones you already have for DoD):
`CF_R2_ACCOUNT_ID`, `CF_R2_BUCKET`, `CF_R2_ACCESS_KEY_ID`, `CF_R2_SECRET_ACCESS_KEY`.

## Files

```
fetch_awards.py        -- USASpending bulk download, termination filter
build_dashboard.py     -- Aggregate + build web/data/*.json
config.yaml            -- Fiscal years + termination-code labels
web/index.html         -- Dashboard (DataTables + Chart.js)
web/methodology.html   -- Methodology page
web/shared/            -- Filter manager, CSS (copied from dod-contract-vehicles)
web/data/*.json        -- Dashboard data (committed for Vercel)
data/                  -- Raw data (gitignored)
vercel.json            -- Routes / → web/
```
