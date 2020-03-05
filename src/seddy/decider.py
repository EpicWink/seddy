"""SWF decider."""

import uuid
import socket
import pathlib
import typing as t
import logging as lg

from . import _util
from . import _specs

logger = lg.getLogger(__name__)
socket.setdefaulttimeout(70.0)


class Decider:
    """SWF decider.

    Args:
        workflows: decider workflows
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
        workflows: t.List[_specs.Workflow],
        domain: str,
        task_list: str,
        identity: str = None,
    ):
        self.workflows = workflows
        self.domain = domain
        self.task_list = task_list
        self.client = _util.get_swf_client()
        self.identity = identity or (socket.getfqdn() + "-" + str(uuid.uuid4())[:8])

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

    def _make_decisions(self, task: t.Dict[str, t.Any]) -> t.List[t.Dict[str, t.Any]]:
        """Build decisions from workflow history.

        Args:
            task: decision task

        Returns:
            workflow decisions
        """

        workflow_ids = [(w.name, w.version) for w in self.workflows]
        task_id = (task["workflowType"]["name"], task["workflowType"]["version"])
        idx = workflow_ids.index(task_id)
        workflow = self.workflows[idx]
        return workflow.make_decisions(task)

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
        logger.info(
            "Got decision task '%s' for workflow '%s-%s' execution '%s' (run '%s')",
            task["taskToken"],
            task["workflowType"]["name"],
            task["workflowType"]["version"],
            task["workflowExecution"]["workflowId"],
            task["workflowExecution"]["runId"],
        )
        decisions = self._make_decisions(task)
        self._respond_decision_task_completed(decisions, task)

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

    decider_spec = _specs.load_workflows(workflows_spec_file)
    workflows = _specs.construct_workflows(decider_spec)
    _specs.setup_workflows(workflows)
    decider = Decider(workflows, domain, task_list, identity)
    decider.run()
