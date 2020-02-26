"""Test ``seddy._util``."""

import sys
import json
from unittest import mock

from seddy import _util as seddy_util
import pytest
import yaml
import toml


def test_list_paginated():
    # Build input
    def fn(foo, bar=42, nextPageToken=None):
        spams = {None: [0], "spam": [1, 2, 3], "eggs": [4, 7, 9], "ham": [10, 42, 99]}
        tokens = {None: "spam", "spam": "eggs", "eggs": "ham"}
        resp = {"foo": foo * bar, "spam": spams[nextPageToken]}
        if nextPageToken in tokens:
            resp["nextPageToken"] = tokens[nextPageToken]
        return resp

    kwargs = {"foo": "ab", "bar": 7}

    # Run function
    assert seddy_util.list_paginated(fn, "spam", kwargs) == {
        "foo": "ababababababab",
        "spam": [0, 1, 2, 3, 4, 7, 9, 10, 42, 99],
    }


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
                        "heartbeat": "60",
                        "timeout": "86400",
                        "task_list": "eggs",
                        "priority": "1",
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
    assert seddy_util.load_workflows(workflows_file) == workflows_spec


def test_load_workflows_yaml(tmp_path, workflows_spec):
    """Test workflows specs loading from YAML."""
    # Build input
    workflows_file = tmp_path / "workflows.yml"
    workflows_file.write_text(yaml.safe_dump(workflows_spec))

    # Run function
    assert seddy_util.load_workflows(workflows_file) == workflows_spec


def test_load_workflows_yaml_raises(tmp_path, workflows_spec):
    """Test workflows specs loading from YAML raises when unavailable."""
    # Setup environment
    yaml_patch = mock.patch.dict(sys.modules, {"yaml": None, "ruamel.yaml": None})

    # Build input
    workflows_file = tmp_path / "workflows.yml"
    workflows_file.write_text(yaml.safe_dump(workflows_spec))

    # Run function
    with pytest.raises(ModuleNotFoundError), yaml_patch:
        seddy_util.load_workflows(workflows_file)


def test_load_workflows_toml(tmp_path, workflows_spec):
    """Test workflows specs loading from TOML."""
    # Build input
    workflows_file = tmp_path / "workflows.yml"
    workflows_file.write_text(toml.dumps(workflows_spec))
    print(workflows_file.read_text())

    # Run function
    res = seddy_util.load_workflows(workflows_file)
    print(workflows_spec)
    print(res)
    print(json.dumps(workflows_spec, indent=4))
    print(json.dumps(res, indent=4))
    assert res == workflows_spec


def test_load_workflows_toml_raises(tmp_path, workflows_spec):
    """Test workflows specs loading from TOML raises when unavailable."""
    # Setup environment
    toml_patch = mock.patch.dict(sys.modules, {"toml": None})

    # Build input
    workflows_file = tmp_path / "workflows.toml"
    workflows_file.write_text(toml.dumps(workflows_spec))

    # Run function
    with pytest.raises(ModuleNotFoundError), toml_patch:
        seddy_util.load_workflows(workflows_file)


def test_load_workflows_with_incorrect_suffix(tmp_path, workflows_spec):
    """Test workflows specs loading raises for incorrect suffix."""
    # Build input
    workflows_file = tmp_path / "workflows.spam"
    workflows_file.write_text(str(workflows_spec))

    # Run function
    with pytest.raises(ValueError):
        seddy_util.load_workflows(workflows_file)
