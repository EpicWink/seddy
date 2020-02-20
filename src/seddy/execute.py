"""SWF workflow execution start."""

import sys
import json
import pathlib
import typing as t
import logging as lg

import boto3

logger = lg.getLogger(__name__)


def execute_workflow(
    workflow_name: str,
    workflow_version: str,
    execution_id: str,
    domain: str,
    input_: t.Union[t.Dict[str, t.Any], list, str, float, int, bool, None],
    task_list: str = None,
    execution_timeout: int = None,
    decision_timeout: t.Union[int, str] = None,
    child_policy: str = None,
) -> str:
    """Start a workflow execution.

    Args:
        workflow_name: name of workflow to run
        workflow_version: version of workflow to run
        execution_id: user-specified execution ID, for
        domain: SWF domain
        input_: execution input
        task_list: decision task task-list
        execution_timeout: execution time-out (seconds)
        decision_timeout: decision task time-out (seconds), or 'NONE' for
            unlimited
        child_policy: child execution close policy on termination

    Returns:
        started execution's run ID
    """

    client = boto3.client("swf")
    logger.log(
        25,
        "Executing workflow '%s' (version %s) with ID '%s' on domain '%s'",
        workflow_name,
        workflow_version,
        execution_id,
        domain,
    )
    logger.debug("Input: %s", input_)
    kwargs = {}
    if task_list is not None:
        kwargs["taskList"] = {"name": task_list}
    if execution_timeout is not None:
        kwargs["executionStartToCloseTimeout"] = str(execution_timeout)
    if decision_timeout is not None:
        kwargs["taskStartToCloseTimeout"] = str(decision_timeout)
    if child_policy is not None:
        kwargs["childPolicy"] = child_policy
    logger.debug("Extra arguments: %s", kwargs)
    resp = client.start_workflow_execution(
        domain=domain,
        workflowId=execution_id,
        workflowType={"name": workflow_name, "version": workflow_version},
        input=json.dumps(input_),
        **kwargs,
    )
    return resp["runId"]


def run_app(
    workflow_name: str,
    workflow_version: str,
    execution_id: str,
    domain: str,
    input_json: t.Union[pathlib.Path, int],
    task_list: str,
    execution_timeout: int,
    decision_timeout: int,
    child_policy: str,
):
    """Run workflow execution application.

    Arguments:
        workflow_name: name of workflow to run
        workflow_version: version of workflow to run
        execution_id: user-specified workflow execution ID
        domain: SWF domain
        input_json: execution input JSON, 0 for stdin
        task_list: decision task task-list
        execution_timeout: execution time-out (seconds)
        decision_timeout: decision task time-out (seconds)
        child_policy: child execution close policy on termination
    """

    input_ = json.loads(sys.stdin.read() if input_json == 0 else input_json.read_text())
    run_id = execute_workflow(
        workflow_name,
        workflow_version,
        execution_id,
        domain,
        input_,
        task_list,
        execution_timeout,
        decision_timeout,
        child_policy,
    )
    print(run_id)
