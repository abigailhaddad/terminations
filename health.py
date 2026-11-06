"""
health.py -- checks that turn a quietly bad build into a red run.

The monthly build once shipped a site with 817 fewer terminations (-1.2%) whose
latest termination date had jumped back from 2026-07-02 to 2026-04-01 -- the
2026-09-01 commit of web/data/summary.json. Nothing failed. The next month's
run repaired it. The checks below would have stopped it.

Counts only grow in normal months (+0.1% to +1.5% across the history), so a
drop of more than MAX_TOTAL_DROP, or a latest date that moves backwards, means
part of the data is missing.
"""

import json
import subprocess

# 0.5% of ~68,000 is ~340 rows. The bad September build lost 1.2%.
MAX_TOTAL_DROP = 0.005


def build_problems(new: dict, previous: dict | None) -> list[str]:
    """Compare a fresh summary.json with the last committed one."""
    problems = []
    total = new.get("total_terminations") or 0
    if total <= 0:
        problems.append("the build has no terminations at all")
    if not previous:
        return problems

    before = previous.get("total_terminations") or 0
    if before and total < before * (1 - MAX_TOTAL_DROP):
        problems.append(
            f"terminations fell from {before:,} to {total:,} "
            f"({100 * (total - before) / before:+.1f}%). Allowed drop is "
            f"{100 * MAX_TOTAL_DROP:.1f}%. Some agency files are probably missing."
        )

    latest_before = previous.get("latest_termination_date") or ""
    latest_now = new.get("latest_termination_date") or ""
    if latest_before and latest_now < latest_before:
        problems.append(
            f"latest termination date moved backwards, {latest_before} to "
            f"{latest_now}. Recent data is probably missing."
        )
    return problems


def previous_summary(path: str = "web/data/summary.json", ref: str = "HEAD") -> dict | None:
    """The summary committed at `ref`, or None when git or the file is absent."""
    try:
        out = subprocess.run(
            ["git", "show", f"{ref}:{path}"],
            capture_output=True, text=True, check=True,
        ).stdout
        return json.loads(out)
    except (OSError, subprocess.CalledProcessError, json.JSONDecodeError):
        return None
