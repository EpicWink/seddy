"""Test ``seddy.app``."""

import json
from unittest import mock

from seddy import registration as seddy_registration
from seddy import decisions as seddy_decisions
import moto
import boto3
import pytest


@pytest.fixture
def workflows():
    """Example workflow type specifications."""
    workflows = [
        seddy_decisions.DAGWorkflow("spam", "1.0", []),
        seddy_decisions.DAGWorkflow("foo", "0.42", [], "The best workflow, bar none.",),
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
    assert res == {("foo", "1.0"): False, ("foo", "1.1"): True, ("bar", "0.42"): True}


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
def test_deprecate_workflow():
    """Test workflow deprecation."""
    # Setup environment
    client = boto3.client("swf", region_name="us-east-1")
    client.register_domain(name="spam", workflowExecutionRetentionPeriodInDays="2")
    client.register_workflow_type(domain="spam", name="foo", version="0.42")

    # Build input
    workflow = seddy_decisions.DAGWorkflow("foo", "0.42", [], "A workflow.")

    # Run function
    seddy_registration.deprecate_workflow(workflow, "spam", client)

    # Check registered workflow
    workflow_info = client.describe_workflow_type(
        domain="spam", workflowType={"name": "foo", "version": "0.42"}
    )
    assert workflow_info["typeInfo"]["status"] == "DEPRECATED"


@pytest.fixture
def patch_moto_swf():
    """Temporarily patch ``moto`` to fix workflow undeprecation."""
    from moto.swf import models as swf_models
    from moto.swf import responses as swf_responses
    from moto.swf import urls as swf_urls

    class SWFBackend(swf_models.SWFBackend):
        def undeprecate_type(self, kind, domain_name, name, version):
            domain = self._get_domain(domain_name)
            _type = domain.get_type(kind, name, version)
            if _type.status == "REGISTERED":
                raise swf_models.SWFTypeAlreadyExistsFault(_type)
            _type.status = "REGISTERED"

    class SWFResponse(swf_responses.SWFResponse):
        def undeprecate_workflow_type(self):
            domain = self._params["domain"]
            name = self._params["workflowType"]["name"]
            version = self._params["workflowType"]["version"]
            self._check_string(domain)
            self._check_string(name)
            self._check_string(version)
            self.swf_backend.undeprecate_type("workflow", domain, name, version)
            return ""

    swf_backends_patch = mock.patch.dict(
        swf_models.swf_backends, {r: SWFBackend(r) for r in swf_models.swf_backends}
    )
    url_paths_patch = mock.patch.dict(
        swf_urls.url_paths, {"{0}/$": SWFResponse.dispatch}
    )
    with swf_backends_patch, url_paths_patch:
        yield


@moto.mock_swf
def test_undeprecate_workflow(patch_moto_swf):
    """Test workflow undeprecation."""
    # Setup environment
    client = boto3.client("swf", region_name="us-east-1")
    client.register_domain(name="spam", workflowExecutionRetentionPeriodInDays="2")
    client.register_workflow_type(domain="spam", name="foo", version="0.42")
    client.deprecate_workflow_type(
        domain="spam", workflowType={"name": "foo", "version": "0.42"}
    )

    # Build input
    workflow = seddy_decisions.DAGWorkflow("foo", "0.42", [], "A workflow.")

    # Run function
    seddy_registration.undeprecate_workflow(workflow, "spam", client)

    # Check registered workflow
    workflow_info = client.describe_workflow_type(
        domain="spam", workflowType={"name": "foo", "version": "0.42"}
    )
    assert workflow_info["typeInfo"]["status"] == "REGISTERED"


@moto.mock_swf
def test_register_workflows(patch_moto_swf):
    """Test workflows registration syncing."""
    # Setup environment
    client = boto3.client("swf", region_name="us-east-1")
    client.register_domain(name="spam", workflowExecutionRetentionPeriodInDays="2")
    client.register_workflow_type(domain="spam", name="foo", version="1.0")
    client.register_workflow_type(domain="spam", name="foo", version="1.1")
    client.register_workflow_type(domain="spam", name="bar", version="0.42")
    client.register_workflow_type(domain="spam", name="bar", version="0.43")
    client.register_workflow_type(domain="spam", name="yay", version="0.17")
    client.register_workflow_type(domain="spam", name="yay", version="0.18")
    client.deprecate_workflow_type(
        domain="spam", workflowType={"name": "foo", "version": "1.0"}
    )
    client.deprecate_workflow_type(
        domain="spam", workflowType={"name": "bar", "version": "0.42"}
    )
    client.deprecate_workflow_type(
        domain="spam", workflowType={"name": "yay", "version": "0.17"}
    )

    client_patch = mock.patch.object(boto3, "client", lambda x, **_: {"swf": client}[x])

    # Build input
    workflows = [
        seddy_decisions.DAGWorkflow("foo", "1.0", []),
        seddy_decisions.DAGWorkflow("foo", "1.1", []),
        seddy_decisions.DAGWorkflow("foo", "1.2", []),
        seddy_decisions.DAGWorkflow("bar", "0.42", []),
        seddy_decisions.DAGWorkflow("bar", "0.43", []),
        seddy_decisions.DAGWorkflow("bar", "0.44", []),
    ]
    workflows[0].active = False
    workflows[1].active = False
    workflows[2].active = True
    workflows[3].active = True
    workflows[4].active = True
    workflows[5].active = False

    # Build expectation
    exp_registered_workflow_types = [
        {"name": "yay", "version": "0.18"},
        {"name": "bar", "version": "0.42"},
        {"name": "bar", "version": "0.43"},
        {"name": "foo", "version": "1.2"},
    ]
    exp_deprecated_workflow_types = [
        {"name": "yay", "version": "0.17"},
        {"name": "foo", "version": "1.0"},
        {"name": "foo", "version": "1.1"},
    ]

    # Run function
    with client_patch:
        seddy_registration.register_workflows(workflows, "spam")

    # Check registered workflows
    resp = client.list_workflow_types(domain="spam", registrationStatus="REGISTERED")
    workflow_types = [w["workflowType"] for w in resp["typeInfos"]]
    workflow_types = sorted(workflow_types, key=lambda x: x["version"])
    assert workflow_types == exp_registered_workflow_types

    resp = client.list_workflow_types(domain="spam", registrationStatus="DEPRECATED")
    workflow_types = [w["workflowType"] for w in resp["typeInfos"]]
    workflow_types = sorted(workflow_types, key=lambda x: x["version"])
    assert workflow_types == exp_deprecated_workflow_types


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
            {"spec_type": "dag", "name": "spam", "version": "1.0", "tasks": [],},
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
        seddy_registration.run_app(workflows_spec_json, "spam")

    # Check workflow registration configuration
    register_mock.assert_called_once_with(mock.ANY, "spam")

    res_workflows = register_mock.call_args_list[0][0][0]
    assert len(workflows) == len(workflows)
    for res_workflow, workflow in zip(res_workflows, workflows):
        assert isinstance(res_workflow, type(workflow))
        assert res_workflow.name == workflow.name
        assert res_workflow.version == workflow.version
        assert res_workflow.description == workflow.description
        res_defaults = getattr(res_workflow, "registration_defaults", None)
        assert res_defaults == getattr(workflow, "registration_defaults", None)
