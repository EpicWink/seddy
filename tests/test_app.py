"""Test ``seddy.app``."""

import json
from unittest import mock

from seddy import app as seddy_app
from seddy import decider as seddy_decider
from seddy import decisions as seddy_decisions


def test_run_app(tmp_path):
    """Ensure decider is run with the correct configuration."""
    # Setup environment
    decider_mock = mock.Mock(spec=seddy_decider.Decider)
    decider_class_mock = mock.Mock(return_value=decider_mock)
    decider_class_patch = mock.patch.object(
        seddy_decider, "Decider", decider_class_mock
    )

    # Build input
    decider_spec = {
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
                    },
                    {
                        "id": "bar",
                        "type": {"name": "spam-bar", "version": "0.1"},
                        "heartbeat": "60",
                        "timeout": "86400",
                        "dependencies": "foo",
                    },
                    {
                        "id": "yay",
                        "type": {"name": "spam-foo", "version": "0.3"},
                        "heartbeat": "60",
                        "timeout": "86400",
                        "dependencies": "foo",
                    },
                ],
            }
        ],
    }
    decider_spec_json = tmp_path / "workflows.json"
    decider_spec_json.write_text(json.dumps(decider_spec, indent=4))

    # Run function
    with decider_class_patch:
        seddy_app.run_app(decider_spec_json, "spam", "eggs")

    # Check decider configuration
    decider_class_mock.assert_called_once_with(mock.ANY, "spam", "eggs")
    decider_class_mock.return_value.run.assert_called_once_with()

    workflows = decider_class_mock.call_args_list[0][0][0]
    assert len(workflows) == 1
    assert isinstance(workflows[0], seddy_decisions.DAG)
    assert workflows[0].spec == decider_spec["workflows"][0]
    assert workflows[0].dependants == {"foo": ["bar", "yay"], "bar": [], "yay": []}
