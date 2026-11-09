import pytest

import fetch_awards as fa
from helpers import row, write_checkpoint


def test_termination_codes_are_recognised_whatever_the_case_or_spacing():
    for code in ("E", "F", "X", "f", " x "):
        assert fa.is_termination_row({"action_type_code": code})


def test_other_codes_and_blanks_are_not_terminations():
    for code in ("A", "B", "C", "D", "G", "", None):
        assert not fa.is_termination_row({"action_type_code": code})
    assert not fa.is_termination_row({})


class _Resp:
    def __init__(self, text="", ok=True):
        self.text, self._ok = text, ok

    def raise_for_status(self):
        if not self._ok:
            raise RuntimeError("503")


def test_datestamp_is_the_latest_one_on_the_index_page(monkeypatch):
    html = "Contracts_Full_20260101.zip Contracts_Full_20261101.zip Contracts_Full_20260601.zip"
    monkeypatch.setattr(fa.requests, "get", lambda *a, **k: _Resp(html))
    assert fa._get_latest_datestamp() == "20261101"


def test_datestamp_failure_stops_the_run_instead_of_guessing(monkeypatch):
    monkeypatch.delenv("ALLOW_STALE_DATESTAMP", raising=False)
    monkeypatch.setattr(fa.requests, "get", lambda *a, **k: _Resp(ok=False))
    with pytest.raises(SystemExit) as exc:
        fa._get_latest_datestamp()
    assert "ALLOW_STALE_DATESTAMP" in str(exc.value)


def test_datestamp_page_without_links_also_stops(monkeypatch):
    monkeypatch.delenv("ALLOW_STALE_DATESTAMP", raising=False)
    monkeypatch.setattr(fa.requests, "get", lambda *a, **k: _Resp("nothing here"))
    with pytest.raises(SystemExit):
        fa._get_latest_datestamp()


def test_datestamp_fallback_only_when_asked(monkeypatch):
    monkeypatch.setenv("ALLOW_STALE_DATESTAMP", "1")
    monkeypatch.setattr(fa.requests, "get", lambda *a, **k: _Resp(ok=False))
    assert fa._get_latest_datestamp() == fa.STALE_FALLBACK_DATESTAMP


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """Point the fetcher at a temp dir with one finished checkpoint."""
    monkeypatch.chdir(tmp_path)
    ckpt = tmp_path / "data" / "bulk_checkpoints"
    monkeypatch.setattr(fa, "CHECKPOINT_DIR", ckpt)
    monkeypatch.setattr(fa, "OUTPUT_CSV", tmp_path / "data" / "terminations_bulk.csv")
    write_checkpoint(ckpt, "FY2026_097.csv", [row(1), row(2, "X")])
    monkeypatch.setattr(fa, "_get_latest_datestamp", lambda: "20261101")
    monkeypatch.setattr(fa, "get_agencies", lambda: {"097": "Defense", "036": "Veterans"})
    monkeypatch.setattr("sys.argv", ["fetch_awards.py", "--fy", "2026"])
    return tmp_path


def test_a_failed_download_makes_the_run_fail_and_status_incomplete(sandbox, monkeypatch):
    monkeypatch.setattr(fa, "download_zip", lambda url: fa.FAILED)
    with pytest.raises(SystemExit) as exc:
        fa.main()
    assert exc.value.code == 1
    assert (sandbox / "data" / "scan_status.txt").read_text() == "incomplete"


def test_an_ip_block_chains_the_next_run_and_does_not_fail(sandbox, monkeypatch):
    monkeypatch.setattr(fa, "download_zip", lambda url: fa.IP_BLOCKED)
    fa.main()   # no SystemExit
    assert (sandbox / "data" / "scan_status.txt").read_text() == "blocked"


def test_when_everything_is_already_downloaded_the_status_is_done(sandbox, monkeypatch):
    write = write_checkpoint(fa.CHECKPOINT_DIR, "FY2026_036.csv", [row(3)])
    assert write.exists()
    monkeypatch.setattr(fa, "download_zip", lambda url: pytest.fail("should not download"))
    fa.main()
    assert (sandbox / "data" / "scan_status.txt").read_text() == "done"
    assert fa.OUTPUT_CSV.read_text().count("\n") == 4   # header + 3 rows


def test_no_terminations_anywhere_is_an_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fa, "CHECKPOINT_DIR", tmp_path / "ck")
    monkeypatch.setattr(fa, "OUTPUT_CSV", tmp_path / "out.csv")
    monkeypatch.setattr(fa, "_get_latest_datestamp", lambda: "20261101")
    monkeypatch.setattr(fa, "get_agencies", lambda: {})
    monkeypatch.setattr("sys.argv", ["fetch_awards.py", "--fy", "2026"])
    with pytest.raises(SystemExit) as exc:
        fa.main()
    assert "No terminations" in str(exc.value)


def _zip_with_rows(path, n):
    import zipfile
    from helpers import row
    lines = ["action_type_code,id"] + [f"F,{i}" for i in range(n)]
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("c.csv", "\n".join(lines) + "\n")


def test_an_interrupted_scan_leaves_no_checkpoint_behind(sandbox, monkeypatch):
    """A cancel/kill mid-write must not leave a partial FY*.csv: is_done() would
    count it as finished and the always() R2 upload would keep it."""
    zp = sandbox / "a.zip"
    _zip_with_rows(zp, 10)
    monkeypatch.setattr(fa, "download_zip", lambda url: str(zp))
    calls = {"n": 0}

    def boom(row):
        calls["n"] += 1
        if calls["n"] == 5:
            raise KeyboardInterrupt
        return True

    monkeypatch.setattr(fa, "is_termination_row", boom)
    with pytest.raises(KeyboardInterrupt):
        fa.main()
    ckpt = sandbox / "data" / "bulk_checkpoints"
    assert not fa.is_done(2026, "036")
    assert not list(ckpt.glob("FY2026_036*"))


def test_a_finished_scan_leaves_the_checkpoint_and_no_temp_file(sandbox, monkeypatch):
    zp = sandbox / "a.zip"
    _zip_with_rows(zp, 3)
    monkeypatch.setattr(fa, "download_zip", lambda url: str(zp))
    fa.main()
    ckpt = sandbox / "data" / "bulk_checkpoints"
    assert len((ckpt / "FY2026_036.csv").read_text().splitlines()) == 4
    assert not list(ckpt.glob("*.tmp"))
