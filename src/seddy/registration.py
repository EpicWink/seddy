"""SWF workflow registration."""

import json
import pathlib
import typing as t
import logging as lg

import boto3

from . import _util as seddy_util
from . import decisions as seddy_decisions

logger = lg.getLogger(__name__)


def list_workflows(domain: str, client) -> t.List[t.Tuple[str, str]]:
    """List all workflows in SWF, including registered and deprecated.

    Args:
        domain: domain to list workflows of
        client (botocore.client.BaseClient): SWF client

    Returns:
        names and versions of workflows in SWF
    """

    logger.info("Listing workflows in '%s'", domain)
    resp_registered = seddy_util.list_paginated(
        client.list_workflow_types,
        "typeInfos",
        {"domain": domain, "registrationStatus": "REGISTERED"},
    )
    resp_deprecated = seddy_util.list_paginated(
        client.list_workflow_types,
        "typeInfos",
        {"domain": domain, "registrationStatus": "DEPRECATED"},
    )
    workflows = resp_registered["typeInfos"] + resp_deprecated["typeInfos"]
    existing = [w["workflowType"] for w in workflows]
    return [(w["name"], w["version"]) for w in existing]


def register_workflow(workflow: seddy_decisions.Workflow, domain: str, client):
    """Register a workflow with SWF.

    Args:
        workflow: specification of workflow to register
        domain: domain to register workflow in
        client (botocore.client.BaseClient): SWF client
    """

    logger.info(
        "Registering workflow '%s' (version %s) on domain '%s'",
        workflow.name,
        workflow.version,
        domain,
    )
    kwargs = {}
    if workflow.description is not None:
        kwargs["description"] = workflow.description
    if hasattr(workflow, "registration_defaults"):
        if "task_timeout" in workflow.registration_defaults:
            _default = workflow.registration_defaults["task_timeout"]
            kwargs["defaultTaskStartToCloseTimeout"] = str(_default)
        if "execution_timeout" in workflow.registration_defaults:
            _default = workflow.registration_defaults["execution_timeout"]
            kwargs["defaultExecutionStartToCloseTimeout"] = str(_default)
        if "task_list" in workflow.registration_defaults:
            _default = workflow.registration_defaults["task_list"]
            kwargs["defaultTaskList"] = {"name": _default}
        # if "task_priority" in workflow.registration_defaults:
        #     _default = workflow.registration_defaults["task_priority"]
        #     kwargs["defaultTaskPriority"] = str(_default)
    client.register_workflow_type(
        domain=domain, name=workflow.name, version=workflow.version, **kwargs,
    )


def register_workflows(
    workflows: t.List[seddy_decisions.Workflow],
    domain: str,
    skip_existing: bool = False,
):
    """Register workflows with SWF.

    Args:
        workflows: specifications of workflows to register
        domain: domain to register workflows in
        skip_existing: check for and skip existing workflows
    """

    client = boto3.client("swf")
    logger.log(25, "Registering workflows in '%s'", domain)
    existing = list_workflows(domain, client) if skip_existing else []
    logger.debug("Exising workflows: %s", existing)
    for workflow in workflows:
        if (workflow.name, workflow.version) in existing:
            logger.debug(
                "Skipping existing workflow '%s' (version %s)",
                workflow.name,
                workflow.version,
            )
            continue
        register_workflow(workflow, domain, client)


def run_app(
    workflows_spec_json: pathlib.Path, domain: str, skip_existing: bool = False
):
    """Run decider application.

    Arguments:
        workflows_spec_json: workflows specifications JSON
        domain: SWF domain
        skip_existing: check for and skip existing workflows
    """

    workflows_spec = json.loads(workflows_spec_json.read_text())
    workflows = seddy_util.construct_workflows(workflows_spec)
    register_workflows(workflows, domain, skip_existing)
