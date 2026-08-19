"""Golden byte-compat gate for the aspen driver during the extraction.

Runs `run_experiment.py <name> --dry-run` in the aspen checkout and compares
the normalized transcript against the recorded fixture. The dry-run exercises
the experiment catalog, aggregate expansion, cluster bucketing, and the cost
model without touching gcloud, so any extraction step that changes what the
driver would do shows up as a diff here.

Fixtures are recorded by tests/golden/record.sh; re-record only after an
intentional output change.
"""

import os
import re
import subprocess
from pathlib import Path

import pytest

GOLDEN = Path(__file__).parent / "golden"
ASPEN = Path(os.environ.get("ASPEN_REPO", "~/Projects/bft/aspen-bft")).expanduser()
# The aspen driver needs the system interpreter (pyyaml etc.), not the
# cloud-kitchen venv this test runs under.
PYTHON = os.environ.get("GOLDEN_PYTHON", "/usr/bin/python3")

pytestmark = pytest.mark.skipif(
    not (ASPEN / "scripts/benchmarks/run_experiment.py").exists(),
    reason="aspen repo not present",
)


def _normalize(text: str) -> str:
    return re.sub(r"\d{8}_\d{6}", "TIMESTAMP", text)


def _cleanup_empty_run_dirs():
    runs = ASPEN / "runs"
    if runs.is_dir():
        for d in runs.glob("experiment_*"):
            try:
                d.rmdir()  # only succeeds if empty — which dry-run dirs are
            except OSError:
                pass


@pytest.mark.parametrize("name", ["aspen", "full"])
def test_dry_run_matches_golden(name):
    fixture = GOLDEN / f"dryrun_{name}.txt"
    assert fixture.exists(), f"missing fixture {fixture}; run tests/golden/record.sh"
    try:
        proc = subprocess.run(
            [PYTHON, "run_experiment.py", name, "--dry-run"],
            cwd=ASPEN / "scripts/benchmarks",
            capture_output=True, text=True, timeout=120,
        )
    finally:
        _cleanup_empty_run_dirs()
    assert proc.returncode == 0, proc.stderr[-2000:]
    got = _normalize(proc.stdout + proc.stderr)
    want = fixture.read_text()
    assert got == want, (
        f"dry-run output for '{name}' diverged from golden fixture. "
        "If the change is intentional, re-record with tests/golden/record.sh."
    )
