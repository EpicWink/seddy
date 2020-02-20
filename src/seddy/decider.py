"""SWF decider."""

import json
import uuid
import socket
import pathlib
import typing as t
import logging as lg

import boto3

from . import _util
from . import decisions as seddy_decisions

logger = lg.getLogger(__name__)
socket.setdefaulttimeout(70.0)


class Decider:
    """SWF decider.

    Args:
        workflows: decider workflows
        domain: SWF domain to poll in
        task_list: SWF decider task-list

    Attributes:
        client (botocore.client.BaseClient): SWF client
        identity: name of decider to poll as
    """

    def __init__(
        self, workflows: t.List[seddy_decisions.Workflow], domain: str, task_list: str,
    ):
        self.workflows = workflows
        self.domain = domain
        self.task_list = task_list
        self.client = boto3.client("swf")
        self.identity = socket.getfqdn() + "-" + str(uuid.uuid4())[:8]

    def _poll_for_decision_task(self) -> t.Dict[str, t.Any]:
        """Poll for a decision task from SWF.

        See https://docs.aws.amazon.com/amazonswf/latest/apireference/API_PollForDecisionTask.html

        Returns:
            decision task
        """

        return _util.list_paginated(
            self.client.poll_for_decision_task,
            "events",
            {
                "domain": self.domain,
                "identity": self.identity,
                "taskList": {"name": self.task_list},
            },
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
        logger.log(
            25,
            "Polling for tasks in domain '%s' with task-list '%s' as '%s'",
            self.domain,
            self.task_list,
            self.identity,
        )
        while True:
            self._poll_and_run()

    def run(self):
        """Run decider."""
        try:
            self._run_uncaught()
        except KeyboardInterrupt:
            logger.info("Quitting due to keyboard-interrupt")


def run_app(workflows_spec_json: pathlib.Path, domain: str, task_list: str):
    """Run decider application.

    Arguments:
        workflows_spec_json: workflows specifications JSON
        domain: SWF domain
        task_list: SWF decider task-list
    """

    decider_spec = json.loads(workflows_spec_json.read_text())
    workflows = _util.construct_workflows(decider_spec)
    _util.setup_workflows(workflows)
    decider = Decider(workflows, domain, task_list)
    decider.run()
