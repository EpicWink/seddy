"""Test ``seddy.app``."""

import io
import sys
import json
import datetime
from unittest import mock

from seddy import execute as seddy_execute
import moto
import boto3


@moto.mock_swf
def test_execute_workflow():
    """Test workflow execution."""
    # Setup environment
    client = boto3.client("swf", region_name="us-east-1")
    client.register_domain(name="spam", workflowExecutionRetentionPeriodInDays="2")
    client.register_workflow_type(
        domain="spam",
        name="foo",
        version="1.1",
        defaultTaskStartToCloseTimeout="NONE",
        defaultExecutionStartToCloseTimeout="60",
        defaultTaskList={"name": "eggs"},
        defaultChildPolicy="ABANDON",
    )
    client.register_workflow_type(
        domain="spam",
        name="bar",
        version="0.42",
        defaultTaskStartToCloseTimeout="NONE",
        defaultExecutionStartToCloseTimeout="60",
        defaultTaskList={"name": "eggs"},
        defaultChildPolicy="ABANDON",
    )

    client_patch = mock.patch.object(boto3, "client", lambda x: {"swf": client}[x])

    # Run function
    with client_patch:
        run_id = seddy_execute.execute_workflow(
            "foo", "1.1", "eggs1234", "spam", {"a": None, "b": [17, 42]}
        )

    # Check result
    resp = client.list_open_workflow_executions(
        domain="spam",
        startTimeFilter={
            "oldestDate": datetime.datetime.now() - datetime.timedelta(seconds=2),
        },
    )
    assert len(resp["executionInfos"]) == 1
    assert resp["executionInfos"][0] == {
        "execution": {"workflowId": "eggs1234", "runId": run_id},
        "workflowType": {"name": "foo", "version": "1.1"},
        "startTimestamp": mock.ANY,
        "executionStatus": "OPEN",
        "cancelRequested": False,
    }


def test_run_app(tmp_path, capsys):
    """Ensure workflow execution app is run correctly."""
    # Setup environment
    execute_mock = mock.Mock(return_value="abcd")
    execute_patch = mock.patch.object(seddy_execute, "execute_workflow", execute_mock)

    # Build input
    input_ = {"a": None, "b": [17, 42]}
    input_json = tmp_path / "input.json"
    input_json.write_text(json.dumps(input_))

    # Run function
    with execute_patch:
        seddy_execute.run_app("foo", "1.1", "eggs1234", "spam", input_json)

    # Check output run ID
    out, err = capsys.readouterr()
    assert out == "abcd\n"

    # Check execution configuration
    execute_mock.assert_called_once_with("foo", "1.1", "eggs1234", "spam", input_)


def test_run_app_stdin(tmp_path, capsys):
    """Ensure workflow execution app is run correctly."""
    # Setup environment
    execute_mock = mock.Mock(return_value="abcd")
    execute_patch = mock.patch.object(seddy_execute, "execute_workflow", execute_mock)

    stdin = io.StringIO()
    stdin_patch = mock.patch.object(sys, "stdin", stdin)

    # Build input
    input_ = {"a": None, "b": [17, 42]}
    stdin.write(json.dumps(input_))
    stdin.seek(0)

    # Run function
    with execute_patch, stdin_patch:
        seddy_execute.run_app("foo", "1.1", "eggs1234", "spam", 0)

    # Check output run ID
    out, err = capsys.readouterr()
    assert out == "abcd\n"

    # Check execution configuration
    execute_mock.assert_called_once_with("foo", "1.1", "eggs1234", "spam", input_)
