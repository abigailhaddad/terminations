import json

import build_dashboard as bd
from helpers import row, write_checkpoint


def _records(tmp_path, monkeypatch, rows):
    ck = tmp_path / "ck"
    write_checkpoint(ck, "FY2026_097.csv", rows)
    monkeypatch.setattr(bd, "BULK_CSV", tmp_path / "none.csv")
    monkeypatch.setattr(bd, "CHECKPOINT_DIR", ck)
    return bd.enrich_contracts(bd.stream_and_aggregate())


def test_one_record_per_termination_transaction(tmp_path, monkeypatch):
    rows = [row(1), row(2, "E"), row(3, "X"), row(4, "A"), row(5, "F", key=" ")]
    rows[4]["contract_award_unique_key"] = ""        # no award key: dropped
    recs = _records(tmp_path, monkeypatch, rows)
    assert [r["action_type_code"] for r in recs] == ["F", "E", "X"]


def test_reason_comes_from_the_code(tmp_path, monkeypatch):
    recs = _records(tmp_path, monkeypatch, [row(1, "F"), row(2, "E"), row(3, "X")])
    assert [r["termination_reason"] for r in recs] == [
        "Terminate for Convenience (complete or partial)",
        "Terminate for Default (complete or partial)",
        "Terminate for Cause",
    ]


def test_two_transactions_on_one_modification_stay_two_rows(tmp_path, monkeypatch):
    a = row(1, key="CONT_AWD_X_9700_-NONE-_-NONE-", mod="P00004")
    b = row(2, key="CONT_AWD_X_9700_-NONE-_-NONE-", mod="P00004", fao="250.00")
    recs = _records(tmp_path, monkeypatch, [a, b])
    assert len(recs) == 2
    assert recs[0]["transaction_key"] != recs[1]["transaction_key"]


def test_amounts_keep_their_sign_and_cents(tmp_path, monkeypatch):
    recs = _records(tmp_path, monkeypatch, [row(1, fao="-1000.55"), row(2, fao="75.10"), row(3, fao="")])
    assert [r["federal_action_obligation"] for r in recs] == [-1000.55, 75.1, None]


def test_set_aside_defaults_to_none(tmp_path, monkeypatch):
    assert _records(tmp_path, monkeypatch, [row(1)])[0]["set_aside"] == "NONE"


def test_payload_round_trips(tmp_path, monkeypatch):
    recs = bd.build_contracts_json(_records(tmp_path, monkeypatch, [row(1), row(2, "E"), row(3, "X")]))
    payload = bd.encode_terminations(recs)
    again = bd.encode_terminations(bd.decode_terminations(json.loads(json.dumps(payload))))
    assert again == payload


def test_piid_comes_out_of_the_key():
    assert bd.piid_from_key("CONT_AWD_N0003025C1010_9700_-NONE-_-NONE-") == "N0003025C1010"
    assert bd.piid_from_key(None) is None
    assert bd.piid_from_key("short") is None


def test_summary_counts_rows_and_contracts(tmp_path, monkeypatch):
    k = "CONT_AWD_SAME_9700_-NONE-_-NONE-"
    recs = bd.build_contracts_json(_records(tmp_path, monkeypatch, [row(1, key=k), row(2, key=k, mod="P00002"), row(3)]))
    s = bd.build_summary(recs)
    assert (s["total_terminations"], s["unique_contracts"]) == (3, 2)


def test_zero_total_obligated_stays_zero_not_null(tmp_path, monkeypatch):
    # A fully de-obligated contract has total_dollars_obligated == 0; it must not turn into null ("--").
    recs = _records(tmp_path, monkeypatch, [row(1, total_dollars_obligated="0"), row(2, total_dollars_obligated="")])
    out = bd.build_contracts_json(recs)
    by_key = {r["piid"]: r["total_obligated"] for r in out}
    assert by_key == {"ABC001": 0, "ABC002": None}
