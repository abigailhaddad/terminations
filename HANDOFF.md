# Terminations: operator guide

For whoever runs this next. It assumes you can read a GitHub Actions log and edit a YAML file, and not much more.

## What it does

On the 1st of every month at 09:00 UTC, a GitHub Action:

1. Downloads USAspending's contract archive, one file per agency per fiscal year (2025 and 2026), and keeps only the rows whose `action_type_code` is E, F or X: the three kinds of termination.
2. Builds four JSON files for the website (`web/data/`) and runs the checks in `run_checks.py`.
3. Commits those files, deploys `web/` to Cloudflare Pages, and fast-forwards a `prod` branch.
4. If the repo secret `HF_TOKEN` exists, publishes the table to Hugging Face. Without it this step does nothing.

No LLM is involved anywhere. The only running cost is GitHub Actions time and whatever your Cloudflare plan charges.

As of 2026-11-06 the live site says 67,966 terminations, built 2026-11-01, latest termination dated 2026-09-30.

## Where things live

| What | Where |
|---|---|
| The site | https://terminations.abigailhaddad.com (Cloudflare Pages project `terminations`) |
| A second copy | https://terminations.vercel.app. On 2026-11-06 it served the same numbers as the main site. It appears to follow the `prod` branch. See "Moving it" |
| The code | this repo |
| The published data (small) | `web/data/*.json`, committed to this repo. `terminations.json` is about 10 MB and is columnar, see `CLAUDE.md` |
| The raw downloads | a Cloudflare R2 bucket, under the prefix in `config.yaml` (`terminations/`): one CSV per agency per fiscal year. About 150 MB when last copied to a laptop. **This is the only full copy.** |
| The Hugging Face dataset | `abigailhaddad/contract-terminations`. Not published yet, see below |
| Run history | the repo's Actions tab |

## Secrets and variables

Read from the workflow files. I could not list what is set on the repo, so check Settings > Secrets and variables > Actions.

| Name | Kind | Used for |
|---|---|---|
| `CF_R2_ACCOUNT_ID`, `CF_R2_BUCKET`, `CF_R2_ACCESS_KEY_ID`, `CF_R2_SECRET_ACCESS_KEY` | secrets | reading and writing the R2 bucket |
| `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID` | secrets | deploying to Cloudflare Pages (`fetch.yml` and `deploy.yml`) |
| `HF_TOKEN` | secret, **new and optional** | publishing to Hugging Face. A token with write access to the dataset |
| `CLOUDFLARE_PROJECT` | variable, optional | the Pages project name. Defaults to `terminations` |
| `GITHUB_TOKEN` | built in | pushing the data commit and the `prod` branch |

## The Hugging Face dataset

Nothing has been uploaded yet. The table, the dataset card and the upload script are ready.

- Try it first: `python3 publish_hf.py`. It builds `build/hf/` and checks the table against the source CSVs row by row (row count, unique `transaction_key`, date, code, modification and amount). It uploads nothing.
- To go live, add the `HF_TOKEN` repo secret. The next monthly run publishes. To publish sooner, run the "Fetch Terminations" workflow by hand, or run `HF_TOKEN=... python3 publish_hf.py --upload` yourself.
- One upload is one commit with two files: `data/terminations.parquet` and `README.md` (the dataset card, generated from the data). The script creates the dataset repo if it is missing.
- It refuses to upload a table more than 0.5% smaller than the one already published. `--allow-shrink` overrides that.
- The dataset id is `site.hf_dataset` in `config.yaml`. Change it there.

## Moving it to a new owner

Everything that names the current owner:

- `config.yaml`, the `site:` block: GitHub repo, site address, Pages project, Hugging Face dataset, R2 prefix. Each can also be set by environment variable (`settings.py` lists them).
- The GitHub links in the footers of `web/index.html`, `web/methodology.html` and `web/404.html`. They are static. A test fails if they drift from `config.yaml`.
- `README.md`, `CLAUDE.md`.
- The Pages project name in the two workflows. Set the `CLOUDFLARE_PROJECT` variable instead of editing them.
- The custom domain, in Cloudflare DNS and on the Pages project.
- A new R2 bucket and the four `CF_R2_*` secrets.

You start with an empty R2 bucket. The first monthly run then downloads every agency and year again, which is slow. The fetch job's time limit is 350 minutes, and if USAspending blocks the runner it starts another run and carries on. I have not run it from an empty bucket.

The Vercel copy: `vercel.json` only sets the output folder to `web`. If you don't use Vercel, delete `vercel.json`, `.vercelignore` and the `promote` job in `fetch.yml`, and shut the project down in Vercel.

Both pages carry the banner "This is experimental, please do not rely on this." Decide whether that is still right.

## When it fails

A run is red when something is missing, on purpose. Finished downloads are kept and uploaded to R2, so a re-run only fetches what is left.

| What you see | Meaning |
|---|---|
| Fetch step fails, status file says `incomplete` | Some agency/year downloads failed or a checkpoint file could not be read. The log lists them. Re-run the workflow |
| Fetch step stops with "Could not auto-detect the archive datestamp" | USAspending's index page changed or was down. Re-run later. `ALLOW_STALE_DATESTAMP=1` forces an old date, but that turns every download into a 404 and records them as done, so don't |
| Status `blocked`, a new run starts itself | USAspending blocked the runner's IP. Normal. It chains runs until done |
| "Check built site" fails with "terminations fell" or "moved backwards" | Part of the data is missing. See below |
| "Check built site" fails on the 25 MiB limit | Cloudflare Pages rejects any file over 25 MiB. `terminations.json` is about 10 MB today |
| Hugging Face job fails | Site and `prod` still update. Read the log. A shrink refusal means the new table is smaller than the published one |

The drop check exists because of the 2026-09-01 build. It published 66,883 terminations, down from 67,702, and the latest termination date jumped back from 2026-07-02 to 2026-04-01. Nothing failed. The next month repaired it. Totals normally grow 0.1% to 1.5% a month. The check allows a 0.5% drop.

## Things to know about the data

- **A row is a transaction.** A contract with several termination modifications appears several times. Five pairs in the FY2025 Defense file even share contract, modification number, date and code, with different amounts. The row id is USAspending's `contract_transaction_unique_key`.
- **Prime contracts only.** No subawards, grants or loans.
- **Late reports.** Agencies report modifications after the fact, and USAspending republishes the archive monthly. Recent months fill in later, and a few rows disappear between builds. 11 of the 67,468 rows in an April 2026 copy are not in the November build. All 11 are dated 2026-01-30 to 2026-02-03, all have `2036` in their award keys, and 10 of them have a $0 amount.
- **Termination for convenience is not fault.**
- **Rows without a `contract_award_unique_key` are dropped.** None were found in the copy checked.
- The site rounds dollars to whole numbers for display. The Hugging Face table keeps cents.
- `methodology.html` has the full caveat list.

## Running it and testing it

```bash
pip install -r requirements-test.txt
python3 -m pytest                  # no network
python3 run_checks.py              # checks on the committed site
python3 fetch_awards.py            # real downloads, hours
python3 build_dashboard.py
python3 publish_hf.py              # dry run
```

The tests run on every pull request (`.github/workflows/tests.yml`).
