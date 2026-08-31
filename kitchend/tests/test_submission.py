import pytest
from pydantic import ValidationError

from kitchend.api.routes import JobSubmit
from kitchend.config import ProjectConfig
from kitchend.core import submission


def test_http_submission_rejects_removed_structured_sweep_fields():
    with pytest.raises(ValidationError, match="sweep"):
        JobSubmit.model_validate({
            "project": "p",
            "sweep": {"dims": {"payload_size": [4096]}},
        })
    with pytest.raises(ValidationError, match="extra_flags"):
        JobSubmit.model_validate({
            "project": "p", "command": ["runner"],
            "extra_flags": ["--payload-sizes", "4096"],
        })


def test_explicit_argv_is_the_only_stored_executable_config(tmp_path):
    project = ProjectConfig(name="p", repo_path=tmp_path)
    argv = ["runner", "--payload-sizes", "4096"]

    specs = submission.prepare_specs(project, {
        "project": "p", "name": "exact", "command": argv,
    })

    assert specs == [{
        "project": "p", "name": "exact", "command": argv,
    }]


def test_catalog_driver_args_are_canonicalized_before_queueing(tmp_path):
    project = ProjectConfig(name="p", repo_path=tmp_path,
                            driver=("python3", "driver.py"))
    (tmp_path / "driver.py").write_text("")

    specs = submission.prepare_specs(project, {
        "project": "p", "experiments": ["baseline"],
    })

    assert specs[0]["command"] == ["python3", "driver.py", "baseline"]
    assert "driver_args" not in specs[0]
    assert "extra_flags" not in specs[0]


def test_command_and_catalog_names_are_mutually_exclusive(tmp_path):
    project = ProjectConfig(name="p", repo_path=tmp_path)
    with pytest.raises(ValueError, match="mutually exclusive"):
        submission.prepare_specs(project, {
            "project": "p", "experiments": ["baseline"],
            "command": ["runner"],
        })
