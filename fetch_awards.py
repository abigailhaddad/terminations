"""
fetch_awards.py -- Download federal contract TERMINATIONS from USASpending bulk archives.

Pulls one ZIP per toptier agency per fiscal year from files.usaspending.gov,
streams each CSV, and keeps ONLY the rows whose action_type_code is a
termination (E = Default, F = Convenience, X = Cause). Non-termination rows
are discarded at ingestion so the checkpoint CSVs stay tiny.

Each agency/FY is checkpointed individually. Re-running skips completed files.
Safe to interrupt and resume.

Run:
    python3 fetch_awards.py                         # all agencies, FY from config.yaml
    python3 fetch_awards.py --fy 2026               # one year
    python3 fetch_awards.py --agencies 097 036      # specific agencies
    python3 fetch_awards.py --force                 # re-download everything
"""

import argparse
import csv
import io
import os
import re
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import requests
import yaml

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

ARCHIVE_BASE   = "https://files.usaspending.gov/award_data_archive/"
AGENCY_CSV_URL = "https://files.usaspending.gov/reference_data/agency_codes.csv"
CHECKPOINT_DIR = Path("data/bulk_checkpoints")
OUTPUT_CSV     = Path("data/terminations_bulk.csv")
CONFIG_PATH    = Path("config.yaml")


with open(CONFIG_PATH) as _f:
    CONFIG = yaml.safe_load(_f)

DEFAULT_FY        = list(CONFIG["fetch"]["fiscal_years"])
TERMINATION_CODES = set(CONFIG["fetch"]["termination_codes"].keys())


STALE_FALLBACK_DATESTAMP = "20260306"
DATESTAMP = None  # set in main(); detecting it needs the network


def _get_latest_datestamp() -> str:
    """Scrape the archive index page for the most recent bulk datestamp.

    If that fails the run stops. The old behavior was to fall back to a fixed
    date, which makes every download a 404, and a 404 writes a `.not_found`
    marker that later runs treat as "done". Set ALLOW_STALE_DATESTAMP=1 to use
    the fixed fallback anyway.
    """
    try:
        r = requests.get(ARCHIVE_BASE, timeout=15)
        r.raise_for_status()
        dates = re.findall(r"Contracts_Full_(\d{8})\.zip", r.text)
        if dates:
            latest = max(dates)
            print(f"Auto-detected datestamp: {latest}")
            return latest
        reason = "no Contracts_Full_<date>.zip links on the index page"
    except Exception as exc:
        reason = str(exc)
    if os.environ.get("ALLOW_STALE_DATESTAMP") == "1":
        print(f"Could not auto-detect datestamp ({reason}), using fallback {STALE_FALLBACK_DATESTAMP}")
        return STALE_FALLBACK_DATESTAMP
    sys.exit(f"Could not auto-detect the archive datestamp ({reason}). Stopping so no "
             f".not_found markers are written. ALLOW_STALE_DATESTAMP=1 overrides.")


def is_termination_row(row: dict) -> bool:
    """True when the row's action_type_code is one of the termination codes."""
    return (row.get("action_type_code") or "").strip().upper() in TERMINATION_CODES


def get_agencies() -> dict[str, str]:
    """Fetch the toptier agency list from USASpending reference data."""
    r = requests.get(AGENCY_CSV_URL, timeout=30)
    r.raise_for_status()
    rows = list(csv.DictReader(io.StringIO(r.text)))
    agencies: dict[str, str] = {}
    for row in rows:
        code = row.get("CGAC AGENCY CODE", "").strip()
        if row.get("TOPTIER_FLAG", "").strip() == "TRUE" and code and code not in agencies:
            agencies[code] = row["AGENCY NAME"]
    print(f"Loaded {len(agencies)} toptier agency codes")
    return agencies


NOT_FOUND  = "NOT_FOUND"
IP_BLOCKED = "IP_BLOCKED"
FAILED     = "FAILED"


def download_zip(url: str, max_retries: int = 3) -> str:
    """Download to a temp file. Returns path or sentinel string."""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, stream=True, timeout=600)
            if r.status_code == 404:
                return NOT_FOUND
            if r.status_code >= 500:
                return IP_BLOCKED
            r.raise_for_status()
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
            downloaded = 0
            last_print = 0
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    tmp.write(chunk)
                    downloaded += len(chunk)
                    mb = downloaded / 1024 / 1024
                    if mb - last_print >= 50:
                        print(f"{mb:.0f}MB...", end=" ", flush=True)
                        last_print = mb
            tmp.close()
            return tmp.name
        except requests.exceptions.ConnectionError:
            return IP_BLOCKED
        except Exception as exc:
            wait = min(30 * (attempt + 1), 180)
            print(f"\n    retry {attempt+1}/{max_retries} in {wait}s ({exc})...")
            time.sleep(wait)
    return FAILED


def checkpoint_path(fy: int, code: str) -> Path:
    return CHECKPOINT_DIR / f"FY{fy}_{code}.csv"


def not_found_path(fy: int, code: str) -> Path:
    return CHECKPOINT_DIR / f"FY{fy}_{code}.not_found"


def is_done(fy: int, code: str) -> bool:
    return checkpoint_path(fy, code).exists() or not_found_path(fy, code).exists()


def main() -> None:
    parser = argparse.ArgumentParser(description="Download federal contract terminations from USASpending bulk archives")
    parser.add_argument("--fy", nargs="+", type=int, default=DEFAULT_FY,
                        help=f"Fiscal years to download (default: {DEFAULT_FY})")
    parser.add_argument("--agencies", nargs="+", default=None,
                        help="Specific agency codes (default: all toptier)")
    parser.add_argument("--force", action="store_true",
                        help="Re-download even if checkpoint exists")
    parser.add_argument("--force-current-fy", action="store_true",
                        help="Re-download current FY only (for monthly refresh)")
    parser.add_argument("--summary-file", default="",
                        help="Append a markdown per-run summary to this file (e.g. $GITHUB_STEP_SUMMARY)")
    args = parser.parse_args()

    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    global DATESTAMP
    DATESTAMP = _get_latest_datestamp()

    agencies = get_agencies()
    if args.agencies:
        agencies = {c: agencies.get(c, f"Agency {c}") for c in args.agencies}

    if args.force:
        for fy in args.fy:
            for code in agencies:
                for p in [checkpoint_path(fy, code), not_found_path(fy, code)]:
                    if p.exists():
                        print(f"  --force: removing {p.name}")
                        p.unlink()

    if args.force_current_fy:
        current = max(args.fy)
        for code in agencies:
            for p in [checkpoint_path(current, code), not_found_path(current, code)]:
                if p.exists():
                    p.unlink()
        print(f"  --force-current-fy: cleared FY{current} checkpoints")

    already = sum(1 for fy in args.fy for c in agencies if is_done(fy, c))
    todo    = sum(1 for fy in args.fy for c in agencies if not is_done(fy, c))
    print(f"Already done: {already}  To download: {todo}")
    print(f"Years: {args.fy}")
    print(f"Termination codes kept: {sorted(TERMINATION_CODES)}\n")

    ip_blocked = False
    failed: list[tuple[int, str]] = []   # (fy, agency code) that did not finish
    total_kept = 0
    total_scanned = 0
    run_results: list[tuple[int, str, str, int, int]] = []  # (fy, code, name, scanned, kept)

    for fy in args.fy:
        if ip_blocked:
            break
        fy_done = sum(1 for c in agencies if is_done(fy, c))
        fy_todo = len(agencies) - fy_done
        print(f"\n{'='*60}")
        print(f"FY{fy}  --  {fy_done} done, {fy_todo} to download")
        print(f"{'='*60}")

        for code, name in agencies.items():
            if is_done(fy, code):
                continue

            url = f"{ARCHIVE_BASE}FY{fy}_{code}_Contracts_Full_{DATESTAMP}.zip"
            print(f"  [{code}] {name[:40]:<40} FY{fy}...", end=" ", flush=True)
            resp = download_zip(url)

            if resp is IP_BLOCKED:
                print("IP BLOCKED -- stopping.")
                ip_blocked = True
                break
            if resp is FAILED:
                print("FAILED -- will retry next run")
                failed.append((fy, code))
                continue
            if resp is NOT_FOUND:
                print("404")
                not_found_path(fy, code).touch()
                continue

            zip_path = resp
            zip_mb = os.path.getsize(zip_path) / 1024 / 1024
            print(f"{zip_mb:.1f} MB  |  scanning...", end=" ", flush=True)

            rows_scanned = 0
            rows_kept = 0
            cp = checkpoint_path(fy, code)
            # Written under a temp name and renamed only when the scan finishes,
            # so a cancel, timeout or kill mid-write can never leave a partial
            # FY*.csv that is_done() counts as complete and r2_sync uploads.
            tmp_cp = cp.with_name(cp.name + ".tmp")

            try:
                with zipfile.ZipFile(zip_path) as zf:
                    csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
                    if not csv_names:
                        print("no CSV in zip")
                        cp.touch()
                        continue

                    with open(tmp_cp, "w", newline="", encoding="utf-8") as out_f:
                        writer = None  # initialized from first CSV's header

                        for csv_name in csv_names:
                            with zf.open(csv_name) as raw:
                                reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"))
                                if writer is None:
                                    writer = csv.DictWriter(out_f, fieldnames=reader.fieldnames)
                                    writer.writeheader()

                                for row in reader:
                                    rows_scanned += 1
                                    if rows_scanned % 500_000 == 0:
                                        print(f"{rows_scanned//1000}k...", end=" ", flush=True)

                                    if not is_termination_row(row):
                                        continue

                                    writer.writerow(row)
                                    rows_kept += 1

                    os.replace(tmp_cp, cp)

            except Exception as exc:
                print(f"\n    ERROR: {exc}")
                if tmp_cp.exists():
                    tmp_cp.unlink()
                os.unlink(zip_path)
                failed.append((fy, code))
                continue
            except BaseException:
                # KeyboardInterrupt / SystemExit (a cancelled run): drop the partial file
                if tmp_cp.exists():
                    tmp_cp.unlink()
                os.unlink(zip_path)
                raise

            os.unlink(zip_path)
            total_kept += rows_kept
            total_scanned += rows_scanned
            run_results.append((fy, code, name, rows_scanned, rows_kept))
            print(f"scanned {rows_scanned:,}  ->  kept {rows_kept:,} terminations")

    # Merge all checkpoints into one CSV
    print(f"\n{'='*60}")
    print("Merging checkpoints...")

    unreadable: list[str] = []   # checkpoint files that could not be read
    all_fieldnames: list[str] = []
    seen: set[str] = set()
    checkpoint_files = sorted(CHECKPOINT_DIR.glob("FY*.csv"))
    for cp_file in checkpoint_files:
        if cp_file.stat().st_size > 0:
            try:
                with open(cp_file, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for col in (reader.fieldnames or []):
                        if col not in seen:
                            all_fieldnames.append(col)
                            seen.add(col)
            except Exception:
                unreadable.append(cp_file.name)

    if not all_fieldnames:
        # Real data always has terminations. Returning quietly here left the
        # status file unwritten and the workflow green.
        sys.exit("No terminations found in any checkpoint -- stopping.")

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_fieldnames, extrasaction="ignore")
        writer.writeheader()
        for cp_file in checkpoint_files:
            if cp_file.stat().st_size == 0:
                continue
            try:
                with open(cp_file, newline="", encoding="utf-8") as cf:
                    reader = csv.DictReader(cf)
                    for row in reader:
                        writer.writerow(row)
                        total_rows += 1
            except Exception:
                if cp_file.name not in unreadable:
                    unreadable.append(cp_file.name)

    num_files = sum(1 for cp_file in checkpoint_files if cp_file.stat().st_size > 0)
    print(f"Wrote {total_rows:,} termination rows to {OUTPUT_CSV}")
    print(f"From {num_files} agency/FY files")

    if unreadable:
        print(f"Could not read {len(unreadable)} checkpoint file(s): {', '.join(unreadable)}")
    problem = bool(failed or unreadable)
    status = "blocked" if ip_blocked else ("incomplete" if problem else "done")
    Path("data/scan_status.txt").write_text(status)

    if ip_blocked:
        print(f"\nIP blocked -- re-run to continue. Progress saved.")
    elif problem:
        listing = ", ".join(f"FY{fy}/{code}" for fy, code in failed) or "none"
        print(f"\nIncomplete. Downloads that did not finish: {listing}")
    else:
        print("\nDone!")

    if args.summary_file and run_results:
        lines = [f"## This run: {len(run_results)} agency/FY file(s) downloaded\n"]
        lines.append(f"**Total rows scanned:** {total_scanned:,}  ")
        lines.append(f"**Termination rows kept:** {total_kept:,}  ")
        state = ('IP blocked -- chaining next run' if ip_blocked
                 else f'INCOMPLETE -- {len(failed)} failed' if problem else 'Complete')
        lines.append(f"**Status:** {state}\n")
        lines.append("| FY | Agency | Rows scanned | Terminations kept |")
        lines.append("|----|--------|--------------|-------------------|")
        for fy, code, name, scanned, kept in run_results:
            lines.append(f"| {fy} | {name} ({code}) | {scanned:,} | {kept:,} |")
        lines.append("")
        with open(args.summary_file, "a") as f:
            f.write("\n".join(lines) + "\n")

    if problem and not ip_blocked:
        # Red run, so the build and deploy are skipped. Finished files are
        # kept and uploaded to R2; the next run retries only the failed ones.
        sys.exit(1)


if __name__ == "__main__":
    main()
