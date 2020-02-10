"""Multi-workflow SWF decider service."""

import json
import uuid
import socket
import pathlib
import argparse
import typing as t
import logging as lg

import boto3

logger = lg.getLogger(__name__)
socket.setdefaulttimeout(70.0)


def make_decisions_dag(  # TODO: unit-test
    task: t.Dict[str, t.Any], spec: t.Dict[str, t.Any],
) -> t.List[t.Dict[str, t.Any]]:
    """Build decisions from workflow history using DAG-type workflow.

    Args:
        task: decision task
        spec: DAG workflow specification

    Returns:
        workflow decisions
    """

    def get(item_id, items, id_key):
        return next(item for item in items if item[id_key] == item_id)

    def schedule_task():
        assert task["events"][-1]["eventType"] == "WorkflowExecutionStarted"
        decision_attributes = {
            "activityId": activity_task["id"],
            "activityType": activity_task["type"],
            "input": task["events"][-1]["input"][activity_task["id"]],
        }
        if "heartbeat" in activity_task:
            decision_attributes["heartbeatTimeout"] = activity_task["hearbeat"]
        if "timeout" in activity_task:
            decision_attributes["startToCloseTimeout"] = activity_task["timeout"]
        if "task_list" in activity_task:
            decision_attributes["taskList"] = activity_task["task_list"]
        if "priority" in activity_task:
            decision_attributes["taskPriority"] = activity_task["priority"]
        decisions.append(
            {
                "decisionType": "ScheduleActivityTask",
                "scheduleActivityTaskDecisionAttributes": decision_attributes,
            }
        )

    # Build each activities' dependants
    dependants = {}
    for activity_task in spec["tasks"]:
        dependants_task = []
        for other_activity_task in spec["tasks"]:
            if activity_task["id"] in other_activity_task.get("dependencies", []):
                dependants_task.append(other_activity_task["id"])
        dependants[activity_task["id"]] = dependants_task

    # Build activity task scheduled map
    scheduled = {}
    at_attr_keys = {
        "ActivityTaskCompleted": "ActivityTaskCompletedEventAttributes",
        "ActivityTaskFailed": "ActivityTaskFailedEventAttributes",
        "ActivityTaskTimedOut": "ActivityTaskTimedOutEventAttributes",
        "ActivityTaskScheduled": "ActivityTaskScheduledEventAttributes",
        "ActivityTaskStarted": "ActivityTaskStartedEventAttributes",
    }
    for event in task["events"]:
        if event["eventType"] in at_attr_keys:
            if event["eventType"] == "ActivityTaskScheduled":
                scheduled_event = event
            else:
                scheduled_event = get(
                    event[at_attr_keys[event["eventId"]]]["scheduledEventId"],
                    task["events"],
                    "eventId",
                )
            scheduled[event[at_attr_keys[event["eventId"]]]] = scheduled_event

    # Build workflow status from events
    activity_task_events = {at["id"]: [] for at in spec["tasks"]}
    for event in reversed(task["events"]):  # events are newest-first
        if event["eventType"] in at_attr_keys:
            scheduled_event = scheduled[event["eventId"]]
            activity_task_events[scheduled_event["activityId"]].append(event)

    # Process new events
    assert task["events"][0]["eventType"] == "DecisionTaskScheduled"
    decisions = []
    for event in task["events"][1:]:
        if event["eventType"] == "DecisionTaskStarted":
            break  # all older events are already processed
        elif event["eventType"] == "ActivityTaskCompleted":
            # Schedule dependants
            scheduled_event = scheduled[event["eventId"]]
            dependants_task = dependants[scheduled_event["activityId"]]
            for activity_task_id in dependants_task:
                assert not activity_task_events[activity_task_id]
                activity_task = get(activity_task_id, spec["tasks"], "id")
                # Check dependencies satisfied
                for dependency_activity_task_id in activity_task["dependencies"]:
                    events = activity_task_events[dependency_activity_task_id]
                    if not events or events[-1]["eventType"] != "ActivityTaskCompleted":
                        break
                else:
                    schedule_task()
            # Complete workflow
            for events in activity_task_events.values():
                if not events or events[-1]["eventType"] != "ActivityTaskCompleted":
                    break
            else:
                return [{"decisionType": "CompleteWorkflowExecution"}]
        elif event["eventType"] == "ActivityTaskFailed":
            return [
                {
                    "decisionType": "FailWorkflowExecution",
                    "failWorkflowExecutionDecisionAttributes": {
                        "reason": "activityFailure",
                        "details": "Activity '%s' failed" % (
                            scheduled[event["eventId"]]["activityId"]
                        ),
                    },
                },
            ]
        elif event["eventType"] == "ActivityTaskTimedOut":
            return [
                {
                    "decisionType": "FailWorkflowExecution",
                    "failWorkflowExecutionDecisionAttributes": {
                        "reason": "activityTimeOut",
                        "details": "Activity '%s' timed-out" % (
                            scheduled[event["eventId"]]["activityId"]
                        ),
                    },
                },
            ]
        elif event["eventType"] == "WorkflowExecutionCancelRequested":
            decisions = []
            for activity_task in spec["tasks"]:
                events = activity_task_events[activity_task["id"]]
                if events and events[-1]["eventType"] in (
                    "ActivityTaskScheduled", "ActivityTaskStarted"
                ):
                    decisions.append(
                        {
                            "decisionType": "RequestCancelActivityTask",
                            "RequestCancelActivityTaskDecisionAttributes": {
                                "activityId": activity_task["id"]
                            }
                        }
                    )
            decisions.append({"decisionType": "CancelWorkflowExecution"})
            return decisions
        elif event["eventType"] == "WorkflowExecutionStarted":
            assert event is task["events"][1]
            for activity_task in spec["tasks"]:  # schedule new activity tasks
                assert not activity_task_events[activity_task["id"]]
                if not activity_task["dependencies"]:
                    schedule_task()
    return decisions


class Decider:  # TODO: unit-test
    """SWF decider.

    Args:
        spec: decider specification
        domain: SWF domain to poll in
        task_list: SWF decider task-list

    Attributes:
        client (botocore.client.BaseClient): SWF client
        identity: name of decider to poll as
    """

    def __init__(self, spec: t.Dict[str, t.Any], domain: str, task_list: str):
        self.spec = spec
        self.domain = domain
        self.task_list = task_list
        self.client = boto3.client("swf")
        self.identity = socket.getfqdn() + "-" + str(uuid.uuid4())[:8]

    def _poll_for_decision_task(
        self, _next_page_token: str = None
    ) -> t.Dict[str, t.Any]:
        """Poll for a decision task from SWF.

        See https://docs.aws.amazon.com/amazonswf/latest/apireference/API_PollForDecisionTask.html

        Args:
            _next_page_token: events pagination token

        Returns:
            decision task
        """

        kwargs = {"nextPageToken": _next_page_token} if _next_page_token else {}
        resp = self.client.poll_for_decision_task(
            domain=self.domain,
            identity=self.identity,
            taskList={"name": self.task_list},
            **kwargs,
        )
        if resp.get("nextPageToken"):
            new_resp = self._poll_for_decision_task(resp.pop("nextPageToken"))
            resp["events"].extend(new_resp["events"])
        return resp

    def _make_decisions(self, task: t.Dict[str, t.Any]) -> t.List[t.Dict[str, t.Any]]:
        """Build decisions from workflow history.

        Args:
            task: decision task

        Returns:
            workflow decisions
        """

        assert tuple(map(int, self.spec["version"].split("."))) > (1,)
        workflow_ids = [(w["name"], w["version"]) for w in self.spec["workflows"]]
        task_id = (task["workflowType"]["name"], task["workflowType"]["version"])
        idx = workflow_ids.index(task_id)
        spec = self.spec["workflows"][idx]

        if spec["spec_type"] == "dag":
            return make_decisions_dag(task, spec)
        else:
            raise ValueError(self.spec)

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
        logger.info(
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


def run_app(decider_spec_json: pathlib.Path, domain: str, task_list: str):  # TODO: unit-test
    """Run decider application.

    Arguments:
        decider_spec_json: decider specification JSON
        domain: SWF domain
        task_list: SWF decider task-list
    """

    decider_spec = json.loads(decider_spec_json.read_text())
    decider = Decider(decider_spec, domain, task_list)
    decider.run()


def main():  # pragma: no cover
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "decider_json", type=pathlib.Path, help="decider specification JSON"
    )
    parser.add_argument("domain", help="SWF domain")
    parser.add_argument("task_list", help="SWF decider task-list")
    args = parser.parse_args()
    run_app(args.decider_json, args.domain, args.task_list)


if __name__ == "__main__":  # pragma: no cover
    main()
