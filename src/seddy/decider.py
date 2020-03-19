"""SWF decider."""

import uuid
import socket
import pathlib
import typing as t
import logging as lg
from concurrent import futures as cf

from . import _util
from . import _specs

logger = lg.getLogger(__name__)
socket.setdefaulttimeout(70.0)


class UnsupportedWorkflow(LookupError):
    """Decider doesn't support workflow."""


class Decider:
    """SWF decider.

    Args:
        workflows_spec_file: workflows specifications file path
        domain: SWF domain to poll in
        task_list: SWF decider task-list
        identity: decider identity, default: automatically generated from
            fully-qualified domain-name and a UUID

    Attributes:
        client (botocore.client.BaseClient): SWF client
        identity (str): name of decider to poll as
    """

    def __init__(
        self,
        workflows_spec_file: pathlib.Path,
        domain: str,
        task_list: str,
        identity: str = None,
    ):
        self.workflows_spec_file = workflows_spec_file
        self.domain = domain
        self.task_list = task_list
        self.client = _util.get_swf_client()
        self.identity = identity or (socket.getfqdn() + "-" + str(uuid.uuid4())[:8])
        self._future = None

    def _poll_for_decision_task(self) -> t.Dict[str, t.Any]:
        """Poll for a decision task from SWF.

        See https://docs.aws.amazon.com/amazonswf/latest/apireference/API_PollForDecisionTask.html

        Returns:
            decision task
        """

        _kwargs = {
            "domain": self.domain,
            "identity": self.identity,
            "taskList": {"name": self.task_list},
        }
        return _util.list_paginated(
            self.client.poll_for_decision_task, "events", _kwargs
        )

    def _get_workflow(self, task: t.Dict[str, t.Any]) -> _specs.Workflow:
        """Get workflow specification for task.

        Args:
            task: decision task

        Returns:
            workflow specification
        """

        workflows = _specs.load_workflows(self.workflows_spec_file)
        workflow_ids = [(w.name, w.version) for w in workflows]
        task_id = (task["workflowType"]["name"], task["workflowType"]["version"])
        try:
            idx = workflow_ids.index(task_id)
        except ValueError:
            raise UnsupportedWorkflow(task["workflowType"]) from None
        return workflows[idx]

    def _respond_decision_task_completed(
        self, decisions: t.List[t.Dict[str, t.Any]], task: t.Dict[str, t.Any]
    ):
        """Send decisions to SWF.

        See https://docs.aws.amazon.com/amazonswf/latest/apireference/API_RespondDecisionTaskCompleted.html

        Args:
            decisions: workflow decisions
            task: decision task
        """

        logger.debug(
            "Sending %d decisions for task '%s'", len(decisions), task["taskToken"]
        )
        self.client.respond_decision_task_completed(
            taskToken=task["taskToken"], decisions=decisions
        )

    def _poll_and_run(self):
        """Perform poll, and possibly run decision task."""
        task = self._poll_for_decision_task()
        logger.debug("Decision task: %s", task)
        if not task["taskToken"]:
            return
        executor = cf.ThreadPoolExecutor(max_workers=1)
        self._future = executor.submit(self._decide_and_respond, task)
        self._future.result()

    def _decide_and_respond(self, task):
        """Make and respond with decisions."""
        logger.info(
            "Got decision task '%s' for workflow '%s-%s' execution '%s' (run '%s')",
            task["taskToken"],
            task["workflowType"]["name"],
            task["workflowType"]["version"],
            task["workflowExecution"]["workflowId"],
            task["workflowExecution"]["runId"],
        )
        try:
            workflow = self._get_workflow(task)
        except UnsupportedWorkflow:
            logger.error("Unsupported workflow type: %s" % task["workflowType"])
            raise
        workflow.setup()

        exc = None
        try:
            decisions = workflow.make_decisions(task)
        except Exception as e:
            decisions = _specs.make_decisions_on_error(e)
            exc = e
        self._respond_decision_task_completed(decisions, task)
        if exc:
            raise exc

    def _run_uncaught(self):
        """Run decider."""
        _fmt = "Polling for tasks in domain '%s' with task-list '%s' as '%s'"
        logger.log(25, _fmt, self.domain, self.task_list, self.identity)
        while True:
            self._poll_and_run()

    def run(self):
        """Run decider."""
        try:
            self._run_uncaught()
        except KeyboardInterrupt:
            logger.info("Quitting due to keyboard-interrupt")
        if self._future.running():
            logger.log(25, "Waiting on current decision task to be handled")
            self._future.result()


def run_app(
    workflows_spec_file: pathlib.Path, domain: str, task_list: str, identity: str = None
):
    """Run decider application.

    Arguments:
        workflows_spec_file: workflows specifications file path
        domain: SWF domain
        task_list: SWF decider task-list
        identity: decider identity, default: automatically generated
    """

    decider = Decider(workflows_spec_file, domain, task_list, identity)
    decider.run()
