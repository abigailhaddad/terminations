from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import build_dashboard as bd
import publish_hf as ph
from helpers import row, write_checkpoint


@pytest.fixture
def built(tmp_path, monkeypatch):
    """A tiny checkpoint dir run through the same path publish_hf.main uses."""
    ck = tmp_path / "ck"
    rows = [
        row(1, "F", "-1000.50", "2026-03-02", mod="P00001"),
        row(2, "E", "-20.25", "2026-03-05", mod="0003"),
        row(3, "X", "75.10", "2026-02-01", mod="P00002"),
        row(4, "F", "", "2026-01-15"),
        row(5, "A"),                                      # not a termination
    ]
    write_checkpoint(ck, "FY2026_097.csv", rows)
    monkeypatch.setattr(bd, "BULK_CSV", tmp_path / "none.csv")
    monkeypatch.setattr(bd, "CHECKPOINT_DIR", ck)
    raw = bd.enrich_contracts(bd.stream_and_aggregate())
    table = ph.to_arrow(ph.table_rows(raw))
    return table, ph.source_index(ck), ck


def test_table_shape_and_order(built):
    table, source, _ = built
    assert table.num_rows == 4 == len(source["by_txn"])
    assert table.column("action_date").to_pylist() == sorted(
        table.column("action_date").to_pylist(), reverse=True)
    assert [f.name for f in table.schema] == [c[0] for c in ph.COLUMNS]


def test_ids_stay_text_and_dates_are_dates(built):
    table, _, _ = built
    assert table.schema.field("modification_number").type == pa.string()
    assert "0003" in table.column("modification_number").to_pylist()
    assert table.schema.field("action_date").type == pa.date32()
    assert table.schema.field("federal_action_obligation").type == pa.float64()


def test_a_correct_table_verifies(built):
    table, source, _ = built
    assert ph.verify(table, source) == []


def test_a_dropped_row_is_caught(built):
    table, source, _ = built
    assert any("source files have" in p for p in ph.verify(table.slice(1), source))


def test_a_changed_amount_is_caught(built):
    table, source, _ = built
    rows = table.to_pylist()
    rows[0]["federal_action_obligation"] += 10
    problems = ph.verify(pa.Table.from_pylist(rows, schema=table.schema), source)
    assert any("differ from the source" in p for p in problems)


def test_a_repeated_transaction_key_is_caught(built):
    table, source, _ = built
    rows = table.to_pylist()
    rows[1]["transaction_key"] = rows[0]["transaction_key"]
    problems = ph.verify(pa.Table.from_pylist(rows, schema=table.schema), source)
    assert any("not unique" in p for p in problems)


def test_a_row_not_in_the_source_is_caught(built):
    table, source, _ = built
    rows = table.to_pylist()
    rows[0]["transaction_key"] = "NOT_IN_SOURCE"
    problems = ph.verify(pa.Table.from_pylist(rows, schema=table.schema), source)
    assert any("not in the source" in p for p in problems)


def test_summary_mismatch_is_caught(built):
    table, source, _ = built
    assert any("summary.json" in p for p in ph.verify(table, source, {"total_terminations": 99}))
    assert ph.verify(table, source, {"total_terminations": 4}) == []


def test_two_transactions_on_the_same_modification_are_both_kept(tmp_path, monkeypatch):
    ck = tmp_path / "ck"
    k = "CONT_AWD_SAME_9700_-NONE-_-NONE-"
    write_checkpoint(ck, "FY2026_097.csv", [row(1, key=k, mod="P00004"), row(2, key=k, mod="P00004", fao="9.99")])
    monkeypatch.setattr(bd, "BULK_CSV", tmp_path / "none.csv")
    monkeypatch.setattr(bd, "CHECKPOINT_DIR", ck)
    table = ph.to_arrow(ph.table_rows(bd.enrich_contracts(bd.stream_and_aggregate())))
    assert table.num_rows == 2 and ph.verify(table, ph.source_index(ck)) == []


def test_card_states_the_facts_from_the_table(built):
    table, _, _ = built
    card = ph.build_card(table, "2026-11-06", no_key_dropped=0)
    assert "license_name: us-govt-public-domain" in card
    assert "4 transactions on 4 contracts" in card
    assert "**2026-01-15** to **2026-03-05**" in card
    assert "Built **2026-11-06**" in card
    assert "Terminate for Convenience" in card and "`transaction_key`" in card
    assert ph.settings.GITHUB_URL in card and ph.settings.SITE_URL in card
    assert "**Derived.**" in card


# --- Hugging Face side, with a fake client: nothing here touches the network ---

class FakeApi:
    def __init__(self):
        self.created, self.commits = [], []

    def create_repo(self, repo_id, repo_type, exist_ok):
        self.created.append((repo_id, repo_type, exist_ok))

    def create_commit(self, **kw):
        self.commits.append(kw)
        return type("Info", (), {"commit_url": "https://huggingface.co/fake/commit/abc"})()


def _files(tmp_path):
    (tmp_path / "data").mkdir()
    (tmp_path / ph.TABLE_PATH).write_bytes(b"parquet")
    (tmp_path / ph.CARD_PATH).write_text("card")
    return tmp_path


def test_upload_is_one_commit_with_both_files_and_creates_the_repo(tmp_path, monkeypatch):
    monkeypatch.setattr(ph, "published_rows", lambda api, repo: 0)
    api = FakeApi()
    url = ph.upload(_files(tmp_path), 1000, api=api)
    assert url.endswith("abc")
    assert api.created == [(ph.settings.HF_DATASET, "dataset", True)]
    assert len(api.commits) == 1
    paths = sorted(op.path_in_repo for op in api.commits[0]["operations"])
    assert paths == [ph.CARD_PATH, ph.TABLE_PATH]


def test_a_much_smaller_table_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(ph, "published_rows", lambda api, repo: 1000)
    api = FakeApi()
    with pytest.raises(SystemExit):
        ph.upload(_files(tmp_path), 900, api=api)
    assert api.commits == []


def test_allow_shrink_overrides_the_guard(tmp_path, monkeypatch):
    monkeypatch.setattr(ph, "published_rows", lambda api, repo: 1000)
    api = FakeApi()
    ph.upload(_files(tmp_path), 900, allow_shrink=True, api=api)
    assert len(api.commits) == 1


def test_a_slightly_smaller_table_is_fine(tmp_path, monkeypatch):
    monkeypatch.setattr(ph, "published_rows", lambda api, repo: 1000)
    api = FakeApi()
    ph.upload(_files(tmp_path), 996, api=api)       # -0.4%
    assert len(api.commits) == 1


def test_dry_run_writes_files_and_uploads_nothing(built, tmp_path, monkeypatch):
    table, source, ck = built
    monkeypatch.setattr(ph, "upload", lambda *a, **k: pytest.fail("a dry run must not upload"))
    out = tmp_path / "out"
    assert ph.main(["--out", str(out)]) == 0
    written = pq.read_table(out / ph.TABLE_PATH)
    assert written.num_rows == 4
    assert (out / ph.CARD_PATH).read_text().startswith("---")


def test_failed_verification_returns_non_zero_and_never_uploads(built, tmp_path, monkeypatch):
    table, source, ck = built
    write_checkpoint(ck, "FY2026_099.csv", [row(9, "F")])   # source has a row the build ... shares
    monkeypatch.setattr(ph, "to_arrow", lambda rows: table)  # table now lacks that row
    monkeypatch.setattr(ph, "upload", lambda *a, **k: pytest.fail("must not upload"))
    assert ph.main(["--out", str(tmp_path / "o"), "--upload"]) == 1
