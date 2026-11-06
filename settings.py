"""
settings.py -- the one place that names the current owner.

Everything that is specific to whoever runs this project (GitHub repo, site
address, Cloudflare Pages project, Hugging Face dataset, R2 prefix) lives in
the `site:` block of config.yaml. Each value can be overridden by an
environment variable, so a new owner can change it without editing code.

Not covered here, because they are static files: the GitHub links in the
footers of web/index.html, web/methodology.html and web/404.html. A test
(tests/test_settings.py) fails if those drift from this file. HANDOFF.md
lists every other place.
"""

import os
from pathlib import Path

import yaml

_CONFIG_PATH = Path(__file__).parent / "config.yaml"

# (config key, environment variable)
_FIELDS = {
    "github_repo":   "TERMINATIONS_GITHUB_REPO",
    "site_url":      "TERMINATIONS_SITE_URL",
    "pages_project": "TERMINATIONS_PAGES_PROJECT",
    "hf_dataset":    "TERMINATIONS_HF_REPO",
    "r2_prefix":     "CF_R2_PREFIX",
}


def load(config_path: Path = _CONFIG_PATH, environ=os.environ) -> dict:
    with open(config_path) as f:
        site = dict(yaml.safe_load(f)["site"])
    for key, env_name in _FIELDS.items():
        override = environ.get(env_name, "").strip()
        if override:
            site[key] = override
    return site


_SITE = load()

GITHUB_REPO = _SITE["github_repo"]
GITHUB_URL = f"https://github.com/{GITHUB_REPO}"
SITE_URL = _SITE["site_url"].rstrip("/")
PAGES_PROJECT = _SITE["pages_project"]
HF_DATASET = _SITE["hf_dataset"]
HF_DATASET_URL = f"https://huggingface.co/datasets/{HF_DATASET}"
R2_PREFIX = _SITE["r2_prefix"]
