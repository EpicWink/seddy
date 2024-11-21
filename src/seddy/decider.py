"""SWF decider."""

import uuid
import socket
import typing as t
import logging as lg
import pathlib
from concurrent import futures as cf

import swf_typed

from . import _specs, _util

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

    def _get_workflow(self, task: swf_typed.DecisionTask) -> _specs.Workflow:
        """Get workflow specification for task.

        Args:
            task: decision task

        Returns:
            workflow specification
        """

        name = task.workflow.name
        version = task.workflow.version
        try:
            return _specs.get_workflow(name, version, self.workflows_spec_file)
        except _specs.WorkflowNotFound as e:
            logger.error("Unsupported workflow type: %s" % task.workflow)
            raise UnsupportedWorkflow(task.workflow) from e

    def _respond_decision_task_completed(
        self, decisions: t.List[swf_typed.Decision], task: swf_typed.DecisionTask
    ):
        """Send decisions to SWF.

        Args:
            decisions: workflow decisions
            task: decision task
        """

        logger.debug("Sending %d decisions for task '%s'", len(decisions), task.token)
        swf_typed.send_decisions(task.token, decisions=decisions, client=self.client)

    def _poll_and_run(self):
        """Perform poll, and possibly run decision task."""
        task = swf_typed.request_decision_task(
            task_list=self.task_list,
            domain=self.domain,
            decider_identity=self.identity,
            client=self.client,
        )
        logger.debug("Decision task: %s", task)
        executor = cf.ThreadPoolExecutor(max_workers=1)
        self._future = executor.submit(self._decide_and_respond, task)
        self._future.result()

    def _decide_and_respond(self, task: swf_typed.DecisionTask):
        """Make and respond with decisions."""
        logger.info(
            "Got decision task '%s' for workflow '%s-%s' execution '%s' (run '%s')",
            task.token,
            task.workflow.name,
            task.workflow.version,
            task.execution.id,
            task.execution.run_id,
        )
        workflow = self._get_workflow(task)
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
        if self._future and self._future.running():
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
