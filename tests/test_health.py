import health


def test_the_bad_september_2026_build_is_caught():
    august = {"total_terminations": 67702, "latest_termination_date": "2026-07-02"}
    september = {"total_terminations": 66883, "latest_termination_date": "2026-04-01"}
    problems = health.build_problems(september, august)
    assert len(problems) == 2
    assert "fell from 67,702 to 66,883" in problems[0]
    assert "moved backwards" in problems[1]


def test_normal_growth_passes():
    july = {"total_terminations": 67641, "latest_termination_date": "2026-06-04"}
    august = {"total_terminations": 67702, "latest_termination_date": "2026-07-02"}
    assert health.build_problems(august, july) == []


def test_a_small_correction_is_allowed():
    before = {"total_terminations": 67966, "latest_termination_date": "2026-09-30"}
    after = {"total_terminations": 67900, "latest_termination_date": "2026-09-30"}   # -0.1%
    assert health.build_problems(after, before) == []


def test_an_empty_build_is_a_problem_even_with_no_history():
    assert health.build_problems({"total_terminations": 0}, None) != []


def test_no_history_and_real_data_passes():
    assert health.build_problems({"total_terminations": 100, "latest_termination_date": "2026-01-01"}, None) == []


def test_previous_summary_is_none_outside_a_repo(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert health.previous_summary() is None
