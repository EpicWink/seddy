"""SWF workflow registration."""

import pathlib
import typing as t
import logging as lg

import swf_typed

from . import _util
from . import _specs

logger = lg.getLogger(__name__)


def list_workflows(domain: str, client) -> t.Dict[t.Tuple[str, str], bool]:
    """List all workflows in SWF, including registered and deprecated.

    Args:
        domain: domain to list workflows of
        client (botocore.client.BaseClient): SWF client

    Returns:
        names, versions and registration status of workflows in SWF
    """

    logger.info("Listing workflows in '%s'", domain)
    registered = swf_typed.list_workflows(domain, deprecated=False, client=client)
    deprecated = swf_typed.list_workflows(domain, deprecated=True, client=client)
    existing = {(w.workflow.name, w.workflow.version): True for w in registered}
    existing.update({(w.workflow.name, w.workflow.version): False for w in deprecated})
    return existing


def register_workflow(workflow: _specs.Workflow, domain: str, client):
    """Register a workflow with SWF.

    Args:
        workflow: specification of workflow to register
        domain: domain to register workflow in
        client (botocore.client.BaseClient): SWF client
    """

    _fmt = "Registering workflow '%s' (version %s) on domain '%s'"
    logger.info(_fmt, workflow.name, workflow.version, domain)

    # Get registration options
    kwargs = {}
    if workflow.description is not None:
        kwargs["description"] = workflow.description
    config = None
    if workflow.registration:
        config = swf_typed.DefaultExecutionConfiguration()
        if workflow.registration.task_timeout is not None:
            config.decision_task_timeout = workflow.registration.task_timeout
        if workflow.registration.execution_timeout is not None:
            config.timeout = workflow.registration.execution_timeout
        if workflow.registration.task_list is not None:
            config.decision_task_list = workflow.registration.task_list
        if workflow.registration.task_priority is not None:
            config.decision_task_priority = workflow.registration.task_priority
        if workflow.registration.child_policy is not None:
            config.child_execution_policy_on_termination = (
                workflow.registration.child_policy.value
            )
        if workflow.registration.lambda_role is not None:
            config.lambda_iam_role_arn = workflow.registration.lambda_role

    # Register
    swf_typed.register_workflow(
        domain=domain,
        workflow=swf_typed.WorkflowId(name=workflow.name, version=workflow.version),
        default_execution_configuration=config,
        client=client,
    )


def deprecate_workflow(workflow: _specs.Workflow, domain: str, client):
    """Deprecate a workflow in SWF.

    Args:
        workflow: specification of workflow to deprecate
        domain: domain to deprecate workflow in
        client (botocore.client.BaseClient): SWF client
    """

    _fmt = "Deprecating workflow '%s' (version %s) in domain '%s'"
    logger.info(_fmt, workflow.name, workflow.version, domain)
    workflow_id = swf_typed.WorkflowId(name=workflow.name, version=workflow.version)
    swf_typed.deprecate_workflow(workflow=workflow_id, domain=domain, client=client)


def undeprecate_workflow(workflow: _specs.Workflow, domain: str, client):
    """Undeprecate a workflow in SWF.

    Args:
        workflow: specification of workflow to undeprecate
        domain: domain to undeprecate workflow in
        client (botocore.client.BaseClient): SWF client
    """

    _fmt = "Undeprecating workflow '%s' (version %s) in domain '%s'"
    logger.info(_fmt, workflow.name, workflow.version, domain)
    workflow_id = swf_typed.WorkflowId(name=workflow.name, version=workflow.version)
    swf_typed.undeprecate_workflow(workflow=workflow_id, domain=domain, client=client)


def _sync_workflow(
    workflow: _specs.Workflow,
    domain: str,
    existing: t.Dict[t.Tuple[str, str], bool],
    client,
):
    """Synchronise a workflow's registration with SWF.

    Args:
        workflow: specification of workflow to register
        domain: domain to register workflow in
        existing:
        client (botocore.client.BaseClient): SWF client
    """

    is_active = workflow.registration.active if workflow.registration else True
    key = (workflow.name, workflow.version)
    if key in existing:
        if existing[key] is is_active:
            _fmt = "Skipping up-to-date workflow '%s' (version %s, active: %s)"
            logger.debug(_fmt, workflow.name, workflow.version, is_active)
        elif is_active:
            undeprecate_workflow(workflow, domain, client)
        elif not is_active:
            deprecate_workflow(workflow, domain, client)
    elif is_active:  # don't register inactive workflows
        register_workflow(workflow, domain, client)


def register_workflows(workflows: t.List[_specs.Workflow], domain: str):
    """Synchronise workflow registration with SWF.

    Args:
        workflows: specifications of workflows to register
        domain: domain to register workflows in
    """

    client = _util.get_swf_client()
    logger.log(25, "Registering workflows in '%s'", domain)

    # Get existing workflows
    existing = list_workflows(domain, client)
    logger.debug("Exising workflows: %s", existing)

    # Register workflows
    for workflow in workflows:
        _sync_workflow(workflow, domain, existing, client)


def run_app(workflows_spec_file: pathlib.Path, domain: str):
    """Run registration synchronisation application.

    Arguments:
        workflows_spec_file: workflows specifications file path
        domain: SWF domain
    """

    workflows = _specs.load_workflows(workflows_spec_file)
    register_workflows(workflows, domain)
