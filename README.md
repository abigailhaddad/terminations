# Federal Contract Terminations

**Live site:** https://terminations.vercel.app

Every federal contract termination modification since FY2025 — with the reason
(Default, Convenience, or Cause), the deobligated amount, the agency that ended
it, and the vendor. You can filter, sort, and download the table as CSV.

## What counts as a "termination"

Every modification to a federal contract carries an action code. Three of those
codes mean the contract was terminated:

| Code | Reason | What it means |
|------|--------|---------------|
| `F` | **Terminate for Convenience** | The government ended the contract because it was in the government's interest. No contractor fault is implied — this can reflect a change in mission, appropriations, or agency priorities. |
| `E` | **Terminate for Default** | The contractor failed to perform. Used on commercial-items and simplified-acquisition contracts. |
| `X` | **Terminate for Cause** | The contractor failed to perform. Used on non-commercial contracts. |

## What you're seeing

- **One row per termination modification.** A contract can be partially
  terminated, have a termination rescinded and re-filed, etc. Every one of
  those modifications shows up as its own row, so a contract with multiple
  termination mods appears multiple times.
- **Deobligated amount** is the dollars pulled back off the contract by that
  specific termination modification.
- **Only prime contracts** (agency → direct vendor) are included. Subawards
  (prime contractor → subcontractor) live in a different dataset that doesn't
  carry termination codes and aren't covered here — see the methodology page
  for the full caveat list.

## Where the data comes from

USASpending's public
[Award Data Archive](https://www.usaspending.gov/download_center/award_data_archive) —
one ZIP per toptier federal agency per fiscal year, republished monthly. The
pipeline grabs those files, filters them down to rows with an `action_type_code`
of `E`, `F`, or `X`, and writes them out for the dashboard. The methodology page
has the exact URL pattern and a full list of caveats.

## How often does this update?

There's a GitHub Actions workflow that re-runs the pipeline monthly and pushes
the refreshed data to the live site. The dashboard shows the build timestamp
and the date of the most recent termination in the header.

## Running it yourself

```bash
pip install -r requirements.txt
python3 fetch_awards.py              # downloads bulk archives, filters to terminations
python3 build_dashboard.py           # builds the dashboard JSON files
cd web && python3 -m http.server     # view locally at http://localhost:8000
```

Fiscal years and termination codes are in `config.yaml`.

## Credits

- Data: [USASpending.gov](https://www.usaspending.gov/)
- Code: https://github.com/abigailhaddad/terminations
- Frontend structure adapted from the
  [dod-contract-vehicles](https://github.com/abigailhaddad/dod-contract-vehicles)
  dashboard.
