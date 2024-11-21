"""Test ``seddy._util``."""

import sys
import json
from unittest import mock

import yaml
import pytest

from seddy import _specs as seddy_specs
from seddy import _util as seddy_util
from seddy._specs import _dag as seddy_specs_dag
from seddy._specs import _io as seddy_specs_io


@pytest.fixture
def workflows_spec():
    """Example workflows specifications."""
    return {
        "version": "1.0",
        "workflows": [
            {
                "spec_type": "dag",
                "name": "spam",
                "version": "1.0",
                "tasks": [
                    {
                        "id": "foo",
                        "type": {"name": "spam-foo", "version": "0.3"},
                        "heartbeat": 60,
                        "timeout": 86400,
                        "task_list": "eggs",
                        "priority": 1,
                    }
                ],
            }
        ],
    }


def test_load_workflows_json(tmp_path, workflows_spec):
    """Test workflows specs loading from JSON."""
    # Build input
    workflows_file = tmp_path / "workflows.json"
    workflows_file.write_text(json.dumps(workflows_spec))

    # Run function
    assert seddy_specs_io._load_specs(workflows_file) == workflows_spec


def test_load_workflows_yaml(tmp_path, workflows_spec):
    """Test workflows specs loading from YAML."""
    # Build input
    workflows_file = tmp_path / "workflows.yml"
    workflows_file.write_text(yaml.safe_dump(workflows_spec))

    # Run function
    assert seddy_specs_io._load_specs(workflows_file) == workflows_spec


def test_load_workflows_yaml_raises(tmp_path, workflows_spec):
    """Test workflows specs loading from YAML raises when unavailable."""
    # Setup environment
    yaml_patch = mock.patch.dict(sys.modules, {"yaml": None, "ruamel.yaml": None})

    # Build input
    workflows_file = tmp_path / "workflows.yml"
    workflows_file.write_text(yaml.safe_dump(workflows_spec))

    # Run function
    with pytest.raises(ModuleNotFoundError), yaml_patch:
        seddy_specs_io._load_specs(workflows_file)


def test_load_workflows_with_incorrect_suffix(tmp_path, workflows_spec):
    """Test workflows specs loading raises for incorrect suffix."""
    # Build input
    workflows_file = tmp_path / "workflows.spam"
    workflows_file.write_text(str(workflows_spec))

    # Run function
    with pytest.raises(ValueError):
        seddy_specs_io._load_specs(workflows_file)


def test_get_workflow(tmp_path, workflows_spec):
    """Test getting workflow from JSON."""
    # Build input
    workflows_file = tmp_path / "workflows.json"
    workflows_file.write_text(json.dumps(workflows_spec))

    # Build expectation
    _task = seddy_specs_dag.Task(id="foo", name="spam-foo", version="0.3", heartbeat=60,
                                 timeout=86400, task_list="eggs", priority=1)
    exp = seddy_specs.DAGWorkflow(name="spam", version="1.0", task_specs=[_task])

    # Run function
    res = seddy_specs_io.get_workflow("spam", "1.0", workflows_file)

    # Check result
    assert (
        isinstance(res, exp.__class__) and
        res.name == exp.name and
        res.version == exp.version and
        res.description == exp.description and
        res.registration == exp.registration and
        res.task_specs == exp.task_specs
    )


@pytest.mark.parametrize(("name", "version"), [("eggs", "1.0"), ("spam", "1.42")])
def test_get_workflow_missing(tmp_path, workflows_spec, name, version):
    """Test getting workflow from JSON raises for missing workfow."""
    # Build input
    workflows_file = tmp_path / "workflows.json"
    workflows_file.write_text(json.dumps(workflows_spec))

    # Run function
    with pytest.raises(seddy_specs_io.WorkflowNotFound) as e:
        seddy_specs_io.get_workflow(name, version, workflows_file)

    # Check result
    assert name in str(e.value)
    assert version in str(e.value)
