"""Run every test from the repo root: the scripts read config.yaml and data/ by relative path."""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
