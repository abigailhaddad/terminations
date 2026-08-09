"""
run_checks.py -- Regression checks on the built site under web/.

Run:
    python3 run_checks.py

Exits non-zero on the first failing check. Wired into .github/workflows/fetch.yml
so a bad build never reaches the deploy.

The checks exist because each of these has actually broken:
  * terminations.json grew to 91 MB, over Cloudflare Pages' hard 25 MiB
    per-file limit, making the site undeployable.
  * Cloudflare Pages has no default 404 -- with no web/404.html it serves
    index.html with HTTP 200 for every unmatched path.
  * build_filter_options() read "place_state" from records that carry
    "state", silently emitting an empty states list.
  * The payload is now columnar; a writer that drops a field the table still
    renders would produce a page of blank cells rather than an error.
"""

import json
import re
import sys
from pathlib import Path

import build_dashboard as bd

WEB = Path(__file__).parent / "web"
DATA = WEB / "data"

failures: list[str] = []
passes: list[str] = []


def check(name: str):
    """Decorator: run a check fn, record pass/fail, keep going."""
    def wrap(fn):
        try:
            detail = fn()
        except Exception as exc:  # noqa: BLE001 -- report, do not abort
            failures.append(f"{name}: {exc}")
        else:
            passes.append(f"{name}" + (f" -- {detail}" if detail else ""))
        return fn
    return wrap


@check("no file under web/ exceeds the Cloudflare Pages 25 MiB limit")
def _sizes():
    oversized = bd.check_web_file_sizes(WEB)
    if oversized:
        listing = ", ".join(f"{p} ({s:,} bytes)" for p, s in oversized)
        raise AssertionError(
            f"{listing} > {bd.MAX_WEB_FILE_BYTES:,} bytes. Cloudflare Pages "
            f"will reject the deploy."
        )
    files = [(p, p.stat().st_size) for p in WEB.rglob("*") if p.is_file()]
    biggest, size = max(files, key=lambda t: t[1])
    return (
        f"{len(files)} files, largest {biggest.relative_to(WEB)} at {size:,} bytes "
        f"({100 * size / bd.MAX_WEB_FILE_BYTES:.0f}% of limit)"
    )


@check("web/404.html exists")
def _has_404():
    p = WEB / "404.html"
    if not p.is_file() or p.stat().st_size == 0:
        raise AssertionError(
            "missing -- Cloudflare Pages would serve index.html with HTTP 200 "
            "for every unmatched path."
        )
    if "404" not in p.read_text():
        raise AssertionError("does not mention 404")
    return f"{p.stat().st_size:,} bytes"


@check("terminations.json is a well-formed v%d payload" % bd.PAYLOAD_VERSION)
def _payload_shape():
    payload = json.loads((DATA / "terminations.json").read_text())
    if payload.get("v") != bd.PAYLOAD_VERSION:
        raise AssertionError(f"version {payload.get('v')!r}")
    n = payload["n"]
    cols, dicts = payload["cols"], payload["dicts"]

    expected_cols = set(bd.DICT_FIELDS + bd.TEXT_FIELDS + bd.NUM_FIELDS + bd.RAW_FIELDS)
    if set(cols) != expected_cols:
        raise AssertionError(f"columns {sorted(set(cols) ^ expected_cols)} unexpected/missing")

    for field, col in cols.items():
        if len(col) != n:
            raise AssertionError(f"column {field} has {len(col)} entries, expected {n}")

    for field in bd.DICT_FIELDS:
        size = len(dicts[field])
        bad = [j for j in cols[field] if j >= size or j < -1]
        if bad:
            raise AssertionError(f"{field} has out-of-range index {bad[0]} (dict size {size})")

    text_size = len(dicts["_text"])
    for field in bd.TEXT_FIELDS:
        bad = [j for j in cols[field] if j >= text_size or j < -1]
        if bad:
            raise AssertionError(f"{field} indexes outside the text pool: {bad[0]}")

    return f"{n:,} rows, {len(dicts['_text']):,} pooled strings"


@check("payload decodes and re-encodes identically")
def _roundtrip():
    payload = json.loads((DATA / "terminations.json").read_text())
    rows = bd.decode_terminations(payload)
    again = bd.encode_terminations(rows)
    if again != payload:
        raise AssertionError("encode(decode(payload)) != payload")
    return f"{len(rows):,} rows"


@check("piid and link are derivable from key on every row")
def _derived():
    payload = json.loads((DATA / "terminations.json").read_text())
    missing = [k for k in payload["cols"]["key"] if not bd.piid_from_key(k)]
    if missing:
        raise AssertionError(
            f"{len(missing)} row(s) yield no piid from key, e.g. {missing[0]!r}. "
            f"The PIID column and its USASpending link would render blank."
        )
    return f"{len(payload['cols']['key']):,} keys"


@check("row count matches summary.json")
def _counts():
    payload = json.loads((DATA / "terminations.json").read_text())
    summary = json.loads((DATA / "summary.json").read_text())
    if payload["n"] != summary["total_terminations"]:
        raise AssertionError(
            f"payload has {payload['n']:,} rows, summary says "
            f"{summary['total_terminations']:,}"
        )
    return f"{payload['n']:,}"


@check("every field the dashboard renders is present in the payload")
def _fields_cover_ui():
    html = (WEB / "index.html").read_text()

    # Only the COLUMNS const describes termination rows. The map's CSV download
    # builds its own `columns` array over per-state aggregates, so a blanket
    # search for "field:" would pick up names that never belong to a row.
    block = re.search(r"const COLUMNS = \[(.*?)\n    \];", html, re.S)
    if not block:
        raise AssertionError("could not locate the COLUMNS array in index.html")
    used = set(re.findall(r"field:\s*'([a-z_]+)'", block.group(1)))
    # DataTables column bindings read straight off the row object.
    used |= set(re.findall(r"data:\s*'([a-z_]+)'", html))
    if not used:
        raise AssertionError("parsed no field names out of index.html -- check the regex")
    missing = sorted(used - set(bd.DECODED_FIELDS))
    if missing:
        raise AssertionError(
            f"index.html renders {missing} but decodeTerminations() does not "
            f"produce them -- those cells would be blank."
        )
    return f"{len(used)} fields referenced, all covered"


@check("filters.json has no empty option lists")
def _filters():
    filters = json.loads((DATA / "filters.json").read_text())
    empty = sorted(k for k, v in filters.items() if not v)
    if empty:
        raise AssertionError(
            f"{empty} are empty -- usually a field-name mismatch between "
            f"build_filter_options() and build_contracts_json()."
        )
    return ", ".join(f"{k}={len(v)}" for k, v in filters.items())


def main() -> int:
    for line in passes:
        print(f"  PASS  {line}")
    for line in failures:
        print(f"  FAIL  {line}")
    print(f"\n{len(passes)} passed, {len(failures)} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
