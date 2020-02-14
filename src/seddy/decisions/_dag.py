"""SWF decisions making."""

import typing as t

from . import _base

_at_attr_keys = {
    "ActivityTaskCompleted": "ActivityTaskCompletedEventAttributes",
    "ActivityTaskFailed": "ActivityTaskFailedEventAttributes",
    "ActivityTaskTimedOut": "ActivityTaskTimedOutEventAttributes",
    "ActivityTaskScheduled": "ActivityTaskScheduledEventAttributes",
    "ActivityTaskStarted": "ActivityTaskStartedEventAttributes",
}


def _get(item_id, items, id_key):
    """Get item from list with given ID."""
    return next(item for item in items if item[id_key] == item_id)


class DAGBuilder(_base.DecisionsBuilder):  # TODO: unit-test
    """SWF decision builder from DAG-type workflow specification."""

    def __init__(self, workflow, task):
        super().__init__(workflow, task)
        self._scheduled = {}
        self._activity_task_events = {at["id"]: [] for at in workflow.spec["tasks"]}

    def _schedule_task(
        self,
        activity_task: t.Dict[str, t.Any],
        workflow_started_event: t.Dict[str, t.Any],
    ):
        assert workflow_started_event["eventType"] == "WorkflowExecutionStarted"
        decision_attributes = {
            "activityId": activity_task["id"],
            "activityType": activity_task["type"],
            "input": workflow_started_event["input"][activity_task["id"]],
        }
        if "heartbeat" in activity_task:
            decision_attributes["heartbeatTimeout"] = activity_task["hearbeat"]
        if "timeout" in activity_task:
            decision_attributes["startToCloseTimeout"] = activity_task["timeout"]
        if "task_list" in activity_task:
            decision_attributes["taskList"] = activity_task["task_list"]
        if "priority" in activity_task:
            decision_attributes["taskPriority"] = activity_task["priority"]
        self.decisions.append(
            {
                "decisionType": "ScheduleActivityTask",
                "scheduleActivityTaskDecisionAttributes": decision_attributes,
            }
        )

    def _get_scheduled_references(self):
        for event in self.task["events"]:
            if event["eventType"] in _at_attr_keys:
                if event["eventType"] == "ActivityTaskScheduled":
                    self._scheduled[event[_at_attr_keys[event["eventId"]]]] = event
                else:
                    self._scheduled[event[_at_attr_keys[event["eventId"]]]] = _get(
                        event[_at_attr_keys[event["eventId"]]]["scheduledEventId"],
                        self.task["events"],
                        "eventId",
                    )

    def _get_activity_task_events(self):
        for event in reversed(self.task["events"]):  # events are newest-first
            if event["eventType"] in _at_attr_keys:
                scheduled_event = self._scheduled[event["eventId"]]
                self._activity_task_events[scheduled_event["activityId"]].append(event)

    def _process_activity_task_completed_event(self, event: t.Dict[str, t.Any]):
        # Schedule dependants
        scheduled_event = self._scheduled[event["eventId"]]
        dependants_task = self.workflow.dependants[scheduled_event["activityId"]]
        for activity_task_id in dependants_task:
            assert not self._activity_task_events[activity_task_id]
            activity_task = _get(activity_task_id, self.workflow.spec["tasks"], "id")
            dependencies_satisfied = True
            for dependency_activity_task_id in activity_task["dependencies"]:
                events = self._activity_task_events[dependency_activity_task_id]
                if not events or events[-1]["eventType"] != "ActivityTaskCompleted":
                    dependencies_satisfied = False
                    break
            if dependencies_satisfied:
                self._schedule_task(activity_task, self.task["events"][-1])

        # Complete workflow
        tasks_complete = True
        for events in self._activity_task_events.values():
            if not events or events[-1]["eventType"] != "ActivityTaskCompleted":
                tasks_complete = False
                break
        if tasks_complete:
            result = {}
            for activity_id, events in self._activity_task_events.items():
                assert events and events[-1]["eventType"] == "ActivityTaskCompleted"
                result[activity_id] = events[-1][
                    "ActivityTaskCompletedEventAttributes"
                ]["result"]
            self.decisions = [
                {
                    "decisionType": "CompleteWorkflowExecution",
                    "CompleteWorkflowExecutionDecisionAttributes": {"result": result},
                },
            ]
            return True

    def _process_activity_task_failed_event(self, event: t.Dict[str, t.Any]):
        self.decisions = [
            {
                "decisionType": "FailWorkflowExecution",
                "failWorkflowExecutionDecisionAttributes": {
                    "reason": "activityFailure",
                    "details": "Activity '%s' failed"
                    % (self._scheduled[event["eventId"]]["activityId"]),
                },
            },
        ]

    def _process_activity_task_timed_out_event(self, event: t.Dict[str, t.Any]):
        self.decisions = [
            {
                "decisionType": "FailWorkflowExecution",
                "failWorkflowExecutionDecisionAttributes": {
                    "reason": "activityTimeOut",
                    "details": "Activity '%s' timed-out"
                    % (self._scheduled[event["eventId"]]["activityId"]),
                },
            },
        ]

    def _process_cancel_requested_event(self, event: t.Dict[str, t.Any]):
        decisions = []
        for activity_task in self.workflow.spec["tasks"]:
            events = self._activity_task_events[activity_task["id"]]
            if events and events[-1]["eventType"] in (
                "ActivityTaskScheduled",
                "ActivityTaskStarted",
            ):
                decisions.append(
                    {
                        "decisionType": "RequestCancelActivityTask",
                        "RequestCancelActivityTaskDecisionAttributes": {
                            "activityId": activity_task["id"]
                        },
                    }
                )
        decisions.append(
            {
                "decisionType": "CancelWorkflowExecution",
                "CancelWorkflowExecutionDecisionAttributes": {
                    "details": event["WorkflowExecutionCancelRequestedEventAttributes"][
                        "cause"
                    ]
                },
            },
        )
        self.decisions = decisions

    def _process_workflow_execution_started_event(self, event: t.Dict[str, t.Any]):
        assert event is self.task["events"][1]
        for activity_task in self.workflow.spec["tasks"]:
            assert not self._activity_task_events[activity_task["id"]]
            if not activity_task["dependencies"]:
                self._schedule_task(activity_task, event)

    def _process_new_events(self):
        assert self.task["events"][0]["eventType"] == "DecisionTaskScheduled"
        for event in self.task["events"][1:]:
            if event["eventType"] == "DecisionTaskStarted":
                break  # all older events are already processed
            elif event["eventType"] == "ActivityTaskCompleted":
                if self._process_activity_task_completed_event(event):
                    break
            elif event["eventType"] == "ActivityTaskFailed":
                self._process_activity_task_failed_event(event)
                break
            elif event["eventType"] == "ActivityTaskTimedOut":
                self._process_activity_task_timed_out_event(event)
                break
            elif event["eventType"] == "WorkflowExecutionCancelRequested":
                self._process_cancel_requested_event(event)
                break
            elif event["eventType"] == "WorkflowExecutionStarted":
                self._process_workflow_execution_started_event(event)

    def build_decisions(self):
        self._get_scheduled_references()
        self._get_activity_task_events()
        self._process_new_events()


class DAG(_base.Workflow):  # TODO: unit-test
    """Dag-type SWF workflow specification."""

    spec_type = "dag"
    decisions_builder = DAGBuilder

    def __init__(self, spec):
        super().__init__(spec)
        self.dependants = {}

    def _build_dependants(self):
        for activity_task in self.spec["tasks"]:
            dependants_task = []
            for other_activity_task in self.spec["tasks"]:
                if activity_task["id"] in other_activity_task.get("dependencies", []):
                    dependants_task.append(other_activity_task["id"])
            self.dependants[activity_task["id"]] = dependants_task

    def setup(self):
        self._build_dependants()


WORKFLOW = {
    DAG.spec_type: DAG,
}
