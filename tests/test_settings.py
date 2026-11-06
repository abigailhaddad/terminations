import re
from pathlib import Path

import yaml

import settings

ROOT = Path(__file__).resolve().parent.parent


def test_defaults_name_the_current_owner():
    assert settings.GITHUB_URL == "https://github.com/abigailhaddad/terminations"
    assert settings.SITE_URL == "https://terminations.abigailhaddad.com"
    assert settings.PAGES_PROJECT == "terminations"
    assert settings.HF_DATASET == "abigailhaddad/contract-terminations"
    assert settings.R2_PREFIX == "terminations/"


def test_environment_overrides_win():
    env = {
        "TERMINATIONS_GITHUB_REPO": "someone/else",
        "TERMINATIONS_SITE_URL": "https://example.org",
        "TERMINATIONS_PAGES_PROJECT": "mine",
        "TERMINATIONS_HF_REPO": "someone/terms",
        "CF_R2_PREFIX": "x/",
    }
    site = settings.load(environ=env)
    assert (site["github_repo"], site["site_url"], site["pages_project"],
            site["hf_dataset"], site["r2_prefix"]) == (
        "someone/else", "https://example.org", "mine", "someone/terms", "x/")


def test_blank_overrides_are_ignored():
    site = settings.load(environ={"TERMINATIONS_GITHUB_REPO": "  "})
    assert site["github_repo"] == "abigailhaddad/terminations"


def test_static_pages_link_to_the_configured_repo():
    """The three HTML pages can't read settings.py, so this catches drift."""
    for name in ("index.html", "methodology.html", "404.html"):
        links = set(re.findall(r"https://github\.com/[\w.-]+/[\w.-]+", (ROOT / "web" / name).read_text()))
        # methodology.html also credits a third-party repo; only this project's links must match.
        ours = {l for l in links if l.endswith("/terminations")}
        assert ours == {settings.GITHUB_URL}, (name, links)


def test_workflows_take_the_pages_project_from_a_variable():
    for name in ("fetch.yml", "deploy.yml"):
        text = (ROOT / ".github/workflows" / name).read_text()
        assert "vars.CLOUDFLARE_PROJECT || 'terminations'" in text
        assert "--project-name=terminations " not in text


def test_hf_publish_job_does_nothing_without_the_secret():
    wf = yaml.safe_load((ROOT / ".github/workflows/fetch.yml").read_text())
    job = wf["jobs"]["publish_hf"]
    assert job["env"]["HF_TOKEN"] == "${{ secrets.HF_TOKEN }}"
    for step in job["steps"]:
        cond = str(step.get("if", ""))
        assert "env.HF_TOKEN" in cond, step.get("name") or step.get("uses")
    publish = [s for s in job["steps"] if "publish_hf.py" in str(s.get("run", ""))]
    assert len(publish) == 1 and "--upload" in publish[0]["run"] and "--check-summary" in publish[0]["run"]
    assert "!= ''" in publish[0]["if"]
