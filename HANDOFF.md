# Handoff

## What this is
Every federal contract termination since October 2024, with the reason, the agency, the vendor and the dollars. It is built from USAspending's contract data.
- Code: https://github.com/abigailhaddad/terminations
- Data: https://huggingface.co/datasets/abigailhaddad/contract-terminations (columns and limits: see the dataset card)
- Site: https://terminations.abigailhaddad.com (caveats: its Methodology page)

## What runs on its own
Everything. Nothing here is done by hand.
- `.github/workflows/fetch.yml` runs monthly: downloads USAspending's files, rebuilds the site data, runs the checks in `run_checks.py` (which use `health.py`), deploys the site and publishes the table to Hugging Face. You can also start it by hand from the Actions tab.
- `.github/workflows/deploy.yml` redeploys the site when someone pushes to `main`.
- `.github/workflows/tests.yml` runs the tests on pull requests and pushes to `main`.

## Secrets and variables
- `CF_R2_ACCOUNT_ID`, `CF_R2_BUCKET`, `CF_R2_ACCESS_KEY_ID`, `CF_R2_SECRET_ACCESS_KEY`: read and write the Cloudflare R2 bucket that holds the raw downloads.
- `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`: deploy the site to Cloudflare Pages.
- `HF_TOKEN`: publish to Hugging Face. Without it the run skips that step.
- `CLOUDFLARE_PROJECT` (variable, optional): the Pages project name. The default is `terminations`.

## If you take it over
- Edit the `site:` block of `config.yaml`: GitHub repo, site address, Pages project, Hugging Face dataset id and R2 folder. Each can also be set by environment variable (`settings.py` lists them).
- Also change the static links in `web/index.html`, `web/methodology.html` and `web/404.html` (a test fails if they drift from `config.yaml`), `README.md` and `tests/test_settings.py`. To find them: `grep -rn abigailhaddad . --exclude-dir=.git`.
- Create your own R2 bucket, the four `CF_R2_*` secrets, a Pages project with the Cloudflare secrets, and a Hugging Face token.
- The raw downloads live only in the R2 bucket. A new bucket starts empty, so the first run downloads every agency and year again. That is slow. The job's limit is 350 minutes, and if USAspending blocks the runner it starts another run. I have not run it from an empty bucket.
- `terminations.vercel.app` is an older Vercel copy. `vercel.json`, `.vercelignore` and the `promote` job in `fetch.yml` exist for it. Delete them if you don't use Vercel.

## When it breaks
Open the Actions tab. A run goes red on purpose when data is missing. Finished downloads are kept, so a re-run fetches only what is left.
- Fetch fails and the log lists agency or year downloads: run it again.
- "Could not auto-detect the archive datestamp": USAspending's index page changed or was down. Run it later. Don't set `ALLOW_STALE_DATESTAMP=1`: it turns every download into a recorded 404.
- "terminations fell" or "moved backwards": part of the data is missing. The checks and the reason for them are at the top of `health.py`.
- The Hugging Face job fails: the site still updates. A refusal means the new table is more than 0.5% smaller than the published one (`--allow-shrink` overrides it).

## Costs
No model is called.
