"""
r2_sync.py -- Sync bulk checkpoint files to/from Cloudflare R2.

Separate R2 prefix from dod-contract-vehicles / procurement-methods to avoid
collisions. Credentials from environment variables:
  CF_R2_ACCOUNT_ID, CF_R2_BUCKET, CF_R2_ACCESS_KEY_ID, CF_R2_SECRET_ACCESS_KEY
"""

import os
from pathlib import Path

import boto3
from botocore.config import Config

ACCOUNT_ID = os.environ["CF_R2_ACCOUNT_ID"]
BUCKET     = os.environ["CF_R2_BUCKET"]
ACCESS_KEY = os.environ["CF_R2_ACCESS_KEY_ID"]
SECRET_KEY = os.environ["CF_R2_SECRET_ACCESS_KEY"]

DEFAULT_PREFIX = "terminations/"
DEFAULT_SUFFIXES = (".csv", ".not_found")


def _client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def download_state(local_dir: Path, prefix: str = DEFAULT_PREFIX) -> int:
    local_dir.mkdir(parents=True, exist_ok=True)
    s3 = _client()
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            local_path = local_dir / Path(key).name
            if not local_path.exists() or local_path.stat().st_mtime < obj["LastModified"].timestamp():
                print(f"  R2 -> {local_path.name}")
                s3.download_file(BUCKET, key, str(local_path))
                count += 1
    print(f"Downloaded {count} files from R2")
    return count


def upload_state(local_dir: Path, prefix: str = DEFAULT_PREFIX,
                 suffixes: tuple = DEFAULT_SUFFIXES, mirror: bool = False) -> int:
    s3 = _client()
    uploaded_keys = set()
    count = 0
    for f in sorted(local_dir.iterdir()):
        if f.suffix in suffixes:
            key = prefix + f.name
            s3.upload_file(str(f), BUCKET, key)
            uploaded_keys.add(key)
            count += 1
    print(f"Uploaded {count} files to R2")

    if mirror:
        paginator = s3.get_paginator("list_objects_v2")
        deleted = 0
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"] not in uploaded_keys:
                    s3.delete_object(Bucket=BUCKET, Key=obj["Key"])
                    deleted += 1
                    print(f"  Deleted orphan: {obj['Key']}")
        if deleted:
            print(f"Deleted {deleted} orphan files")

    return count


def summarize(prefix: str = DEFAULT_PREFIX, summary_file: str = "") -> None:
    """List everything in R2 under prefix and print (and optionally write markdown)."""
    s3 = _client()
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            files.append(obj)

    csvs       = [f for f in files if f["Key"].endswith(".csv")]
    not_founds = [f for f in files if f["Key"].endswith(".not_found")]
    total_mb   = sum(f["Size"] for f in csvs) / 1024 / 1024

    # Group by FY prefix (files are named FY{YYYY}_{agency}.csv)
    by_fy: dict = {}
    for f in csvs:
        name = Path(f["Key"]).name
        fy = name.split("_")[0]
        by_fy.setdefault(fy, []).append((name, f["Size"]))

    print(f"\n{'='*55}")
    print(f"R2: {len(csvs)} CSV files ({total_mb:.1f} MB) + {len(not_founds)} 404 markers")
    for fy in sorted(by_fy, reverse=True):
        entries = sorted(by_fy[fy])
        fy_mb = sum(s for _, s in entries) / 1024 / 1024
        print(f"  {fy}: {len(entries)} agencies, {fy_mb:.1f} MB")
        for name, size in entries:
            print(f"    {name:<55} {size/1024:>8.1f} KB")
    print(f"{'='*55}")

    if summary_file:
        lines = []
        lines.append(f"## R2 State: {len(csvs)} agency/FY files ({total_mb:.1f} MB)\n")
        lines.append(f"- Termination CSVs: **{len(csvs)}** ({total_mb:.1f} MB total)")
        lines.append(f"- 404 markers (no archive): **{len(not_founds)}**\n")
        for fy in sorted(by_fy, reverse=True):
            entries = sorted(by_fy[fy])
            fy_mb = sum(s for _, s in entries) / 1024 / 1024
            lines.append(f"### {fy} -- {len(entries)} agencies, {fy_mb:.1f} MB\n")
            lines.append("| File | Size |")
            lines.append("|------|------|")
            for name, size in entries:
                lines.append(f"| `{name}` | {size/1024:.1f} KB |")
            lines.append("")
        with open(summary_file, "a") as f:
            f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["upload", "download", "summary"])
    parser.add_argument("--dir", default="data/bulk_checkpoints")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--suffix", action="append",
                        help="File extension to include (upload only). Default: .csv, .not_found")
    parser.add_argument("--mirror", action="store_true",
                        help="Upload only: delete R2 objects under --prefix not in this upload.")
    parser.add_argument("--summary-file", default="",
                        help="summary only: append markdown to this file (e.g. $GITHUB_STEP_SUMMARY)")
    args = parser.parse_args()
    d = Path(args.dir)
    if args.action == "download":
        download_state(d, prefix=args.prefix)
    elif args.action == "summary":
        summarize(prefix=args.prefix, summary_file=args.summary_file)
    else:
        suffixes = tuple(args.suffix) if args.suffix else DEFAULT_SUFFIXES
        upload_state(d, prefix=args.prefix, suffixes=suffixes, mirror=args.mirror)
