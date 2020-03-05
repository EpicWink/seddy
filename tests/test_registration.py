"""Test ``seddy.app``."""

import json
from unittest import mock

from seddy import registration as seddy_registration
from seddy import _specs as seddy_decisions
import moto
import boto3
import pytest


class Workflow(seddy_decisions.Workflow):
    """Test workflow specification."""

    spec_type = "test"
    decisions_builder = None


seddy_decisions.WORKFLOW["test"] = Workflow


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


@pytest.fixture
def patch_moto_swf_register():
    """Temporarily patch ``moto`` to fix workflow undeprecation."""
    from moto.swf import models as swf_models
    from moto.swf import responses as swf_responses
    from moto.swf import urls as swf_urls

    class WorkflowType(swf_models.WorkflowType):
        @property
        def _configuration_keys(self):
            new_keys = ["defaultTaskPriority", "defaultLambdaRole"]
            return super()._configuration_keys + new_keys

    class SWFResponse(swf_responses.SWFResponse):
        def register_workflow_type(self):
            domain = self._params["domain"]
            name = self._params["name"]
            version = self._params["version"]
            task_list_d = self._params.get("defaultTaskList")
            task_list = task_list_d.get("name") if task_list_d else None
            child_policy = self._params.get("defaultChildPolicy")
            task_timeout = self._params.get("defaultTaskStartToCloseTimeout")
            execution_timeout = self._params.get("defaultExecutionStartToCloseTimeout")
            task_priority = self._params.get("defaultTaskPriority")
            lambda_role = self._params.get("defaultLambdaRole")
            description = self._params.get("description")

            self._check_string(domain)
            self._check_string(name)
            self._check_string(version)
            self._check_none_or_string(task_list)
            self._check_none_or_string(child_policy)
            self._check_none_or_string(task_timeout)
            self._check_none_or_string(execution_timeout)
            self._check_none_or_string(task_priority)
            self._check_none_or_string(lambda_role)
            self._check_none_or_string(description)

            self.swf_backend.register_type(
                "workflow",
                domain,
                name,
                version,
                task_list=task_list,
                default_child_policy=child_policy,
                default_task_start_to_close_timeout=task_timeout,
                default_execution_start_to_close_timeout=execution_timeout,
                default_task_priority=task_priority,
                default_lambda_role=lambda_role,
                description=description,
            )
            return ""

    swf_types_patch = mock.patch.dict(
        swf_models.KNOWN_SWF_TYPES, {"workflow": WorkflowType}
    )
    url_paths_patch = mock.patch.dict(
        swf_urls.url_paths, {"{0}/$": SWFResponse.dispatch}
    )
    with swf_types_patch, url_paths_patch:
        yield


@moto.mock_swf
def test_register_workflow(patch_moto_swf_register):
    """Test workflow registration."""
    # Setup environment
    client = boto3.client("swf", region_name="us-east-1")
    client.register_domain(name="spam", workflowExecutionRetentionPeriodInDays="2")

    # Build input
    registration = seddy_decisions.Registration(
        task_timeout="NONE",
        execution_timeout=60,
        task_list="eggs",
        task_priority=2,
        child_policy=seddy_decisions.ChildPolicy.TERMINATE,
        lambda_role="arn:aws:iam::spam:role/eggs",
    )
    workflow = Workflow("foo", "0.42", "A workflow.", registration)

    # Run function
    seddy_registration.register_workflow(workflow, "spam", client)

    # Check registered workflow
    workflow_info = client.describe_workflow_type(
        domain="spam", workflowType={"name": "foo", "version": "0.42"}
    )
    assert workflow_info["typeInfo"]["status"] == "REGISTERED"
    assert workflow_info["typeInfo"]["description"] == "A workflow."
    cfg = workflow_info["configuration"]
    assert cfg["defaultTaskStartToCloseTimeout"] == "NONE"
    assert cfg["defaultExecutionStartToCloseTimeout"] == "60"
    assert cfg["defaultTaskList"] == {"name": "eggs"}
    assert cfg["defaultTaskPriority"] == "2"
    assert cfg["defaultChildPolicy"] == "TERMINATE"
    assert cfg["defaultLambdaRole"] == "arn:aws:iam::spam:role/eggs"


@moto.mock_swf
def test_deprecate_workflow():
    """Test workflow deprecation."""
    # Setup environment
    client = boto3.client("swf", region_name="us-east-1")
    client.register_domain(name="spam", workflowExecutionRetentionPeriodInDays="2")
    client.register_workflow_type(domain="spam", name="foo", version="0.42")

    # Build input
    workflow = Workflow("foo", "0.42", "A workflow.")

    # Run function
    seddy_registration.deprecate_workflow(workflow, "spam", client)

    # Check registered workflow
    workflow_info = client.describe_workflow_type(
        domain="spam", workflowType={"name": "foo", "version": "0.42"}
    )
    assert workflow_info["typeInfo"]["status"] == "DEPRECATED"


@pytest.fixture
def patch_moto_swf_undeprecate():
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
def test_undeprecate_workflow(patch_moto_swf_undeprecate):
    """Test workflow undeprecation."""
    # Setup environment
    client = boto3.client("swf", region_name="us-east-1")
    client.register_domain(name="spam", workflowExecutionRetentionPeriodInDays="2")
    client.register_workflow_type(domain="spam", name="foo", version="0.42")
    client.deprecate_workflow_type(
        domain="spam", workflowType={"name": "foo", "version": "0.42"}
    )

    # Build input
    workflow = Workflow("foo", "0.42", "A workflow.")

    # Run function
    seddy_registration.undeprecate_workflow(workflow, "spam", client)

    # Check registered workflow
    workflow_info = client.describe_workflow_type(
        domain="spam", workflowType={"name": "foo", "version": "0.42"}
    )
    assert workflow_info["typeInfo"]["status"] == "REGISTERED"


@moto.mock_swf
def test_register_workflows(patch_moto_swf_undeprecate):
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
        Workflow("foo", "1.0", registration=seddy_decisions.Registration(active=False)),
        Workflow("foo", "1.1", registration=seddy_decisions.Registration(active=False)),
        Workflow("foo", "1.2", registration=seddy_decisions.Registration(active=True)),
        Workflow("bar", "0.42", registration=seddy_decisions.Registration(active=True)),
        Workflow("bar", "0.43", registration=seddy_decisions.Registration(active=True)),
        Workflow("bar", "0.44", "", seddy_decisions.Registration(active=False)),
    ]

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


def test_run_app(tmp_path):
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
            {"spec_type": "test", "name": "spam", "version": "1.0"},
            {
                "spec_type": "test",
                "name": "foo",
                "version": "0.42",
                "description": "The best workflow, bar none.",
                "registration": {
                    "task_timeout": "NONE",
                    "execution_timeout": 60,
                    "task_list": "eggs",
                    "task_priority": 2,
                    "child_policy": "TERMINATE",
                    "lambda_role": "arn:aws:iam::spam:role/eggs",
                },
            },
        ],
    }
    workflows_spec_json = tmp_path / "workflows.json"
    workflows_spec_json.write_text(json.dumps(workflows_spec, indent=4))

    # Build expectation
    registration1 = seddy_decisions.Registration(
        task_timeout="NONE",
        execution_timeout=60,
        task_list="eggs",
        task_priority=2,
        child_policy=seddy_decisions.ChildPolicy.TERMINATE,
        lambda_role="arn:aws:iam::spam:role/eggs",
    )
    workflows = [
        Workflow("spam", "1.0"),
        Workflow("foo", "0.42", "The best workflow, bar none.", registration1),
    ]

    # Run function
    with register_patch:
        seddy_registration.run_app(workflows_spec_json, "spam")

    # Check workflow registration configuration
    register_mock.assert_called_once_with(mock.ANY, "spam")

    res_workflows = register_mock.call_args_list[0][0][0]
    assert len(res_workflows) == len(workflows)
    for res_workflow, workflow in zip(res_workflows, workflows):
        assert isinstance(res_workflow, type(workflow))
        assert res_workflow.name == workflow.name
        assert res_workflow.version == workflow.version
        assert res_workflow.description == workflow.description
        if workflow.registration:
            registration = workflow.registration
            res_registration = res_workflow.registration
            assert res_registration.active == registration.active
            assert res_registration.task_timeout == registration.task_timeout
            assert res_registration.execution_timeout == registration.execution_timeout
            assert res_registration.task_list == registration.task_list
            assert res_registration.task_priority == registration.task_priority
            assert res_registration.child_policy == registration.child_policy
            assert res_registration.lambda_role == registration.lambda_role
