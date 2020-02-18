"""Test ``seddy.app``."""

import json
from unittest import mock

from seddy import registration as seddy_registration
from seddy import decisions as seddy_decisions
import moto
import boto3
import pytest
from botocore import exceptions as botocore_exceptions


@pytest.fixture
def workflows():
    """Example workflow type specifications."""
    workflows = [
        seddy_decisions.DAGWorkflow("spam", "1.0", []),
        seddy_decisions.DAGWorkflow(
            "foo",
            "0.42",
            [],
            "The best workflow, bar none.",
        ),
    ]
    workflows[1].registration_defaults = {
        "task_timeout": "NONE",
        "execution_timeout": 60,
        "task_list": "eggs",
        # "task_priority": 2,
    }
    return workflows


@moto.mock_swf
def test_list_workflows():
    """Test registered workflow listing."""
    # Setup environment
    client = boto3.client("swf", region_name="us-east-1")
    client.register_domain(name="spam", workflowExecutionRetentionPeriodInDays="2")
    client.register_workflow_type(domain="spam", name="foo", version="1.0")
    client.register_workflow_type(domain="spam", name="foo", version="1.1")
    client.register_workflow_type(domain="spam", name="bar", version="0.42")
    client.deprecate_workflow_type(
        domain="spam", workflowType={"name": "foo", "version": "1.0"}
    )

    # Run function
    res = seddy_registration.list_workflows("spam", client)
    assert set(res) == {("foo", "1.0"), ("foo", "1.1"), ("bar", "0.42")}  # unordered


@moto.mock_swf
def test_register_workflow():
    """Test workflow registration."""
    # Setup environment
    client = boto3.client("swf", region_name="us-east-1")
    client.register_domain(name="spam", workflowExecutionRetentionPeriodInDays="2")

    # Build input
    workflow = seddy_decisions.DAGWorkflow("foo", "0.42", [], "A workflow.")
    workflow.registration_defaults = {
        "task_timeout": "NONE",
        "execution_timeout": 60,
        "task_list": "eggs",
        # "task_priority": 2,
    }

    # Run function
    seddy_registration.register_workflow(workflow, "spam", client)

    # Check registered workflow
    workflow_info = client.describe_workflow_type(
        domain="spam", workflowType={"name": "foo", "version": "0.42"}
    )
    assert workflow_info["typeInfo"]["status"] == "REGISTERED"
    assert workflow_info["typeInfo"]["description"] == "A workflow."
    assert workflow_info["configuration"]["defaultTaskStartToCloseTimeout"] == "NONE"
    assert workflow_info["configuration"]["defaultExecutionStartToCloseTimeout"] == "60"
    assert workflow_info["configuration"]["defaultTaskList"] == {"name": "eggs"}
    # assert workflow_info["configuration"]["defaultTaskPriority"] == "2"


@moto.mock_swf
def test_register_workflows_all():
    """Test workflows registration, not checking for existing."""
    # Setup environment
    client = boto3.client("swf", region_name="us-east-1")
    client.register_domain(name="spam", workflowExecutionRetentionPeriodInDays="2")
    client.register_workflow_type(domain="spam", name="foo", version="1.0")
    client.register_workflow_type(domain="spam", name="foo", version="1.1")
    client.register_workflow_type(domain="spam", name="bar", version="0.42")
    client.deprecate_workflow_type(
        domain="spam", workflowType={"name": "foo", "version": "1.0"}
    )

    client_patch = mock.patch.object(boto3, "client", lambda x: {"swf": client}[x])

    # Build input
    workflows = [
        seddy_decisions.DAGWorkflow("foo", "0.1", []),
        seddy_decisions.DAGWorkflow("foo", "0.2", []),
        seddy_decisions.DAGWorkflow("bar", "0.17", []),
        seddy_decisions.DAGWorkflow("foo", "1.0", []),
        seddy_decisions.DAGWorkflow("bar", "0.45", []),
    ]

    # Build expectation
    exp_workflow_types = [
        {"name": "foo", "version": "0.1"},
        {"name": "bar", "version": "0.17"},
        {"name": "foo", "version": "0.2"},
        {"name": "bar", "version": "0.42"},
        {"name": "foo", "version": "1.1"},
    ]

    # Run function
    with pytest.raises(botocore_exceptions.ClientError) as e:
        with client_patch:
            seddy_registration.register_workflows(workflows, "spam")
    assert e.value.response["Error"]["Code"] == "TypeAlreadyExistsFault"

    # Check registered workflows
    resp = client.list_workflow_types(domain="spam", registrationStatus="REGISTERED")
    workflow_types = [w["workflowType"] for w in resp["typeInfos"]]
    workflow_types = sorted(workflow_types, key=lambda x: x["version"])
    assert workflow_types == exp_workflow_types


@moto.mock_swf
def test_register_workflows_skips_existing():
    """Test workflows registration, checking for and skipping existing."""
    # Setup environment
    client = boto3.client("swf", region_name="us-east-1")
    client.register_domain(name="spam", workflowExecutionRetentionPeriodInDays="2")
    client.register_workflow_type(domain="spam", name="foo", version="1.0")
    client.register_workflow_type(domain="spam", name="foo", version="1.1")
    client.register_workflow_type(domain="spam", name="bar", version="0.42")
    client.deprecate_workflow_type(
        domain="spam", workflowType={"name": "foo", "version": "1.0"}
    )

    client_patch = mock.patch.object(boto3, "client", lambda x: {"swf": client}[x])

    # Build input
    workflows = [
        seddy_decisions.DAGWorkflow("foo", "0.1", []),
        seddy_decisions.DAGWorkflow("foo", "0.2", []),
        seddy_decisions.DAGWorkflow("bar", "0.17", []),
        seddy_decisions.DAGWorkflow("foo", "1.0", []),
        seddy_decisions.DAGWorkflow("bar", "0.45", []),
    ]

    # Build expectation
    exp_workflow_types = [
        {"name": "foo", "version": "0.1"},
        {"name": "bar", "version": "0.17"},
        {"name": "foo", "version": "0.2"},
        {"name": "bar", "version": "0.42"},
        {"name": "bar", "version": "0.45"},
        {"name": "foo", "version": "1.1"},
    ]
    exp_workflow_types = sorted(exp_workflow_types, key=lambda x: x["version"])

    # Run function
    with client_patch:
        seddy_registration.register_workflows(workflows, "spam", skip_existing=True)

    # Check registered workflows
    resp = client.list_workflow_types(domain="spam", registrationStatus="REGISTERED")
    workflow_types = [w["workflowType"] for w in resp["typeInfos"]]
    workflow_types = sorted(workflow_types, key=lambda x: x["version"])
    assert workflow_types == exp_workflow_types


def test_run_app(tmp_path, workflows):
    """Ensure workflow registration app is run correctly."""
    # Setup environment
    register_mock = mock.Mock()
    register_patch = mock.patch.object(
        seddy_registration, "register_workflows", register_mock
    )

    # Build input
    workflows_spec = {
        "version": "1.0",
        "workflows": [
            {
                "spec_type": "dag",
                "name": "spam",
                "version": "1.0",
                "tasks": [],
            },
            {
                "spec_type": "dag",
                "name": "foo",
                "version": "0.42",
                "tasks": [],
                "description": "The best workflow, bar none.",
                "registration_defaults": {
                    "task_timeout": "NONE",
                    "execution_timeout": 60,
                    "task_list": "eggs",
                    # "task_priority": 2,
                },
            },
        ],
    }
    workflows_spec_json = tmp_path / "workflows.json"
    workflows_spec_json.write_text(json.dumps(workflows_spec, indent=4))

    # Run function
    with register_patch:
        seddy_registration.run_app(workflows_spec_json, "spam", skip_existing=True)

    # Check workflow registration configuration
    register_mock.assert_called_once_with(mock.ANY, "spam", True)

    res_workflows = register_mock.call_args_list[0][0][0]
    assert len(workflows) == len(workflows)
    for res_workflow, workflow in zip(res_workflows, workflows):
        assert isinstance(res_workflow, type(workflow))
        assert res_workflow.name == workflow.name
        assert res_workflow.version == workflow.version
        assert res_workflow.description == workflow.description
        res_defaults = getattr(res_workflow, "registration_defaults", None)
        assert res_defaults == getattr(workflow, "registration_defaults", None)
