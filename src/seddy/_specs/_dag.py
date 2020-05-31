"""SWF decisions making."""

import json
import dataclasses
import typing as t
import logging as lg

from . import _base

logger = lg.getLogger(__name__)
_attr_keys = {
    "ActivityTaskCancelRequested": "activityTaskCancelRequestedEventAttributes",
    "ActivityTaskCanceled": "activityTaskCanceledEventAttributes",
    "ActivityTaskCompleted": "activityTaskCompletedEventAttributes",
    "ActivityTaskFailed": "activityTaskFailedEventAttributes",
    "ActivityTaskScheduled": "activityTaskScheduledEventAttributes",
    "ActivityTaskStarted": "activityTaskStartedEventAttributes",
    "ActivityTaskTimedOut": "activityTaskTimedOutEventAttributes",
    "CancelTimerFailed": "cancelTimerFailedEventAttributes",
    "CancelWorkflowExecutionFailed": "cancelWorkflowExecutionFailedEventAttributes",
    "ChildWorkflowExecutionCanceled": "childWorkflowExecutionCanceledEventAttributes",
    "ChildWorkflowExecutionCompleted": "childWorkflowExecutionCompletedEventAttributes",
    "ChildWorkflowExecutionFailed": "childWorkflowExecutionFailedEventAttributes",
    "ChildWorkflowExecutionStarted": "childWorkflowExecutionStartedEventAttributes",
    "ChildWorkflowExecutionTerminated": "childWorkflowExecutionTerminatedEventAttributes",
    "ChildWorkflowExecutionTimedOut": "childWorkflowExecutionTimedOutEventAttributes",
    "CompleteWorkflowExecutionFailed": "completeWorkflowExecutionFailedEventAttributes",
    "ContinueAsNewWorkflowExecutionFailed": "continueAsNewWorkflowExecutionFailedEventAttributes",
    "DecisionTaskCompleted": "decisionTaskCompletedEventAttributes",
    "DecisionTaskScheduled": "decisionTaskScheduledEventAttributes",
    "DecisionTaskStarted": "decisionTaskStartedEventAttributes",
    "DecisionTaskTimedOut": "decisionTaskTimedOutEventAttributes",
    "ExternalWorkflowExecutionCancelRequested": "externalWorkflowExecutionCancelRequestedEventAttributes",
    "ExternalWorkflowExecutionSignaled": "externalWorkflowExecutionSignaledEventAttributes",
    "FailWorkflowExecutionFailed": "failWorkflowExecutionFailedEventAttributes",
    "LambdaFunctionCompleted": "lambdaFunctionCompletedEventAttributes",
    "LambdaFunctionFailed": "lambdaFunctionFailedEventAttributes",
    "LambdaFunctionScheduled": "lambdaFunctionScheduledEventAttributes",
    "LambdaFunctionStarted": "lambdaFunctionStartedEventAttributes",
    "LambdaFunctionTimedOut": "lambdaFunctionTimedOutEventAttributes",
    "MarkerRecorded": "markerRecordedEventAttributes",
    "RecordMarkerFailed": "recordMarkerFailedEventAttributes",
    "RequestCancelActivityTaskFailed": "requestCancelActivityTaskFailedEventAttributes",
    "RequestCancelExternalWorkflowExecutionFailed": "requestCancelExternalWorkflowExecutionFailedEventAttributes",
    "RequestCancelExternalWorkflowExecutionInitiated": "requestCancelExternalWorkflowExecutionInitiatedEventAttributes",
    "ScheduleActivityTaskFailed": "scheduleActivityTaskFailedEventAttributes",
    "ScheduleLambdaFunctionFailed": "scheduleLambdaFunctionFailedEventAttributes",
    "SignalExternalWorkflowExecutionFailed": "signalExternalWorkflowExecutionFailedEventAttributes",
    "SignalExternalWorkflowExecutionInitiated": "signalExternalWorkflowExecutionInitiatedEventAttributes",
    "StartChildWorkflowExecutionFailed": "startChildWorkflowExecutionFailedEventAttributes",
    "StartChildWorkflowExecutionInitiated": "startChildWorkflowExecutionInitiatedEventAttributes",
    "StartLambdaFunctionFailed": "startLambdaFunctionFailedEventAttributes",
    "StartTimerFailed": "startTimerFailedEventAttributes",
    "TimerCanceled": "timerCanceledEventAttributes",
    "TimerFired": "timerFiredEventAttributes",
    "TimerStarted": "timerStartedEventAttributes",
    "WorkflowExecutionCancelRequested": "workflowExecutionCancelRequestedEventAttributes",
    "WorkflowExecutionCanceled": "workflowExecutionCanceledEventAttributes",
    "WorkflowExecutionCompleted": "workflowExecutionCompletedEventAttributes",
    "WorkflowExecutionContinuedAsNew": "workflowExecutionContinuedAsNewEventAttributes",
    "WorkflowExecutionFailed": "workflowExecutionFailedEventAttributes",
    "WorkflowExecutionSignaled": "workflowExecutionSignaledEventAttributes",
    "WorkflowExecutionStarted": "workflowExecutionStartedEventAttributes",
    "WorkflowExecutionTerminated": "workflowExecutionTerminatedEventAttributes",
    "WorkflowExecutionTimedOut": "workflowExecutionTimedOutEventAttributes",
}
_error_events = {
    "ActivityTaskFailed",
    "ActivityTaskTimedOut",
    "CancelTimerFailed",
    "CancelWorkflowExecutionFailed",
    "CompleteWorkflowExecutionFailed",
    "DecisionTaskTimedOut",
    "FailWorkflowExecutionFailed",
    "RecordMarkerFailed",
    "RequestCancelActivityTaskFailed",
    "ScheduleActivityTaskFailed",
    "StartTimerFailed",
    "WorkflowExecutionCancelRequested",
}
_activity_events = {
    "ActivityTaskCompleted",
    "ActivityTaskFailed",
    "ActivityTaskTimedOut",
    "ActivityTaskScheduled",
    "ActivityTaskStarted",
}
_decision_failed_events = {
    "ScheduleActivityTaskFailed",
    "RequestCancelActivityTaskFailed",
    "StartTimerFailed",
    "CancelTimerFailed",
    "StartChildWorkflowExecutionFailed",
    "SignalExternalWorkflowExecutionFailed",
    "RequestCancelExternalWorkflowExecutionFailed",
    "CancelWorkflowExecutionFailed",
    "CompleteWorkflowExecutionFailed",
    "ContinueAsNewWorkflowExecutionFailed",
    "FailWorkflowExecutionFailed",
}


@dataclasses.dataclass
class Task:  # TODO: unit-test
    """DAG-type workflow activity task specification.

    Args:
        id: task ID, must be unique within a workflow execution and
            without ':', '/', '|', 'arn' or any control character
        name: activity type name
        version: activity type version
        heartbeat: task heartbeat time-out (seconds), or "NONE" for
            unlimited
        timeout: task time-out (seconds), or "None" for unlimited
        task_list: task-list to schedule task on
        priority: task priority
        dependencies: IDs of task’s dependencies
    """

    id: str
    name: str
    version: str
    heartbeat: t.Union[int, str] = None
    timeout: t.Union[int, str] = None
    task_list: str = None
    priority: int = None
    dependencies: t.List[str] = None

    @property
    def type(self) -> t.Dict[str, str]:
        """Activity type."""
        return {"name": self.name, "version": self.version}

    @classmethod
    def from_spec(cls, spec: t.Dict[str, t.Any]) -> "Task":
        """Construct registration configuration from specification.

        Args:
            spec: workflow registration configuration specification
        """

        args = (spec["id"], spec["type"]["name"], spec["type"]["version"])
        kwargs = {}
        if "heartbeat" in spec:
            kwargs["heartbeat"] = spec["heartbeat"]
        if "timeout" in spec:
            kwargs["timeout"] = spec["timeout"]
        if "task_list" in spec:
            kwargs["task_list"] = spec["task_list"]
        if "priority" in spec:
            kwargs["priority"] = spec["priority"]
        if "dependencies" in spec:
            kwargs["dependencies"] = spec["dependencies"]
        return cls(*args, **kwargs)


def _get(item_id, items, id_key):
    """Get item from list with given ID."""
    return next(item for item in items if item[id_key] == item_id)


class DAGBuilder(_base.DecisionsBuilder):
    """SWF decision builder from DAG-type workflow specification."""

    def __init__(self, workflow: "DAGWorkflow", task):
        super().__init__(workflow, task)
        self.workflow = workflow
        self._scheduled = {}
        self._activity_task_events = {at.id: [] for at in workflow.task_specs}
        self._new_events = None
        self._error_events = []
        self._ready_activities = set()

    def _schedule_task(self, activity_task: Task):
        workflow_started_event = self.task["events"][0]
        assert workflow_started_event["eventType"] == "WorkflowExecutionStarted"
        attrs = workflow_started_event["workflowExecutionStartedEventAttributes"]
        decision_attributes = {
            "activityId": activity_task.id,
            "activityType": activity_task.type,
        }

        input_ = json.loads(attrs.get("input", "null"))
        if input_ and activity_task.id in input_:
            decision_attributes["input"] = json.dumps(input_[activity_task.id])
        if activity_task.heartbeat is not None:
            decision_attributes["heartbeatTimeout"] = str(activity_task.heartbeat)
        if activity_task.timeout is not None:
            decision_attributes["startToCloseTimeout"] = str(activity_task.timeout)
        if activity_task.task_list is not None:
            decision_attributes["taskList"] = {"name": activity_task.task_list}
        if activity_task.priority is not None:
            decision_attributes["taskPriority"] = str(activity_task.priority)

        decision = {
            "decisionType": "ScheduleActivityTask",
            "scheduleActivityTaskDecisionAttributes": decision_attributes,
        }
        self.decisions.append(decision)

    def _get_scheduled_references(self):
        for event in self.task["events"]:
            if event["eventType"] in _activity_events:
                if event["eventType"] == "ActivityTaskScheduled":
                    self._scheduled[event["eventId"]] = event
                else:
                    attrs = event[_attr_keys[event["eventType"]]]
                    self._scheduled[event["eventId"]] = _get(
                        attrs["scheduledEventId"], self.task["events"], "eventId"
                    )

    def _get_activity_task_events(self):
        for event in self.task["events"]:
            if event["eventType"] in _activity_events:
                scheduled_event = self._scheduled[event["eventId"]]
                attrs = scheduled_event["activityTaskScheduledEventAttributes"]
                self._activity_task_events[attrs["activityId"]].append(event)

    def _process_activity_task_completed_event(self, event: t.Dict[str, t.Any]):
        scheduled_event = self._scheduled[event["eventId"]]
        attrs = scheduled_event["activityTaskScheduledEventAttributes"]
        dependants_task = self.workflow.dependants[attrs["activityId"]]

        for activity_task_id in dependants_task:
            assert not self._activity_task_events[activity_task_id]
            task = next(a for a in self.workflow.task_specs if a.id == activity_task_id)

            dependencies_satisfied = True
            for dependency_activity_task_id in task.dependencies:
                events = self._activity_task_events[dependency_activity_task_id]
                if not events or events[-1]["eventType"] != "ActivityTaskCompleted":
                    dependencies_satisfied = False
                    break
            if dependencies_satisfied:
                self._ready_activities.add(task.id)

    def _complete_workflow(self):
        tasks_complete = True
        for events in self._activity_task_events.values():
            if not events or events[-1]["eventType"] != "ActivityTaskCompleted":
                tasks_complete = False
                break

        if tasks_complete:
            result = {}
            for activity_id, events in self._activity_task_events.items():
                assert events and events[-1]["eventType"] == "ActivityTaskCompleted"
                attrs = events[-1].get("activityTaskCompletedEventAttributes")
                if attrs and "result" in attrs:
                    result[activity_id] = json.loads(attrs["result"])

            decision = {"decisionType": "CompleteWorkflowExecution"}
            if result:
                decision_attrs = {"result": json.dumps(result)}
                decision["completeWorkflowExecutionDecisionAttributes"] = decision_attrs
            self.decisions = [decision]

    def _fail_workflow(self, reason=None, details=None):
        decision_attrs = {}
        if reason:
            decision_attrs["reason"] = reason
        if details:
            decision_attrs["details"] = details
        decision = {"decisionType": "FailWorkflowExecution"}
        if decision_attrs:
            decision["failWorkflowExecutionDecisionAttributes"] = decision_attrs
        self.decisions = [decision]

    def _process_decision_failed(self, event: t.Dict[str, t.Any]) -> bool:
        event_ids = [event["eventId"] for event in self.task["events"]]
        attrs = event[_attr_keys[event["eventType"]]]
        if attrs["cause"] == "OPERATION_NOT_PERMITTED":
            idx = event_ids.index(attrs["DecisionTaskCompletedEventId"])
            dc_event = self.task["events"][idx]
            dc_attrs = dc_event["decisionTaskCompletedEventAttributes"]
            idx = event_ids.index(dc_attrs["startedEventId"])
            ds_event = self.task["events"][idx]
            ds_attrs = ds_event["decisionTaskStartedEventAttributes"]
            this_ds_event = self.task["events"][-1]
            this_ds_attrs = this_ds_event["decisionTaskStartedEventAttributes"]
            if ds_attrs["identity"] == this_ds_attrs["identity"]:
                raise _base.DeciderError("Not permitted")
            else:
                return False
        elif attrs["cause"] != "UNHANDLED_DECISION":
            raise _base.DeciderError()

        if event["eventType"] == "CancelWorkflowExecutionFailed":
            self.decisions = [{"decisionType": "CancelWorkflowExecution"}]
            return True
        elif event["eventType"] == "FailWorkflowExecutionFailed":
            return False
        elif event["eventType"] == "CompleteWorkflowExecutionFailed":
            self._complete_workflow()
            return True

    def _schedule_initial_activity_tasks(self):
        for task_id in self.workflow.dependants[None]:
            self._ready_activities.add(task_id)

    def _process_error_events(self):
        if not self._error_events:
            return False
        activity_events = []
        decider_events = []
        time_out_events = []
        other_events = []
        for event in self._error_events:
            if event["eventType"] == "ActivityTaskFailed":
                activity_events.append(event)
            elif event["eventType"] == "ActivityTaskTimedOut":
                attr = event["activityTaskTimedOutEventAttributes"]
                if attr["timeoutType"] in ("START_TO_CLOSE", "HEARTBEAT"):
                    activity_events.append(event)
                elif attr["timeoutType"] in ("SCHEDULE_TO_START", "SCHEDULE_TO_CLOSE"):
                    time_out_events.append(event)
            elif event["eventType"] == "WorkflowExecutionCancelRequested":
                self.decisions = [{"decisionType": "CancelWorkflowExecution"}]
                return True
            elif event["eventType"] in _decision_failed_events:
                if self._process_decision_failed(event):
                    return True
                decider_events.append(event)
            elif event["eventType"] in (
                "DecisionTaskTimedOut",
                "WorkflowExecutionTimedOut",
            ):
                time_out_events.append(event)
            elif event["eventType"] == "RecordMarkerFailed":
                other_events.append(event)

        details = []
        if activity_events:
            details.append("%d activities failed" % len(activity_events))
        if decider_events:
            details.append("%d decisions failed" % len(decider_events))
        if time_out_events:
            details.append("%d actions timed-out" % len(time_out_events))
        if other_events:
            details.append("%d other actions failed" % len(other_events))
        details = ", ".join(details)
        self._fail_workflow(details=details)
        return True

    def _process_event(self, event: t.Dict[str, t.Any]):
        if event["eventType"] == "ActivityTaskCompleted":
            self._process_activity_task_completed_event(event)
        elif event["eventType"] == "WorkflowExecutionStarted":
            self._schedule_initial_activity_tasks()

    def _get_new_events(self):
        event_ids = [event["eventId"] for event in self.task["events"]]
        current_idx = event_ids.index(self.task["startedEventId"])
        previous_idx = -1
        if self.task["previousStartedEventId"] in event_ids:
            previous_idx = event_ids.index(self.task["previousStartedEventId"])
        events = self.task["events"][previous_idx + 1 : current_idx + 1]
        logger.debug(
            "Processing %d events from index %d (ID: %s) to %d (ID: %s)",
            len(events),
            previous_idx + 1,
            events[0]["eventId"],
            current_idx,
            events[-1]["eventId"],
        )
        self._new_events = events

    def _schedule_tasks(self):
        for task_id in self._ready_activities:
            task = next(ts for ts in self.workflow.task_specs if ts.id == task_id)
            assert not self._activity_task_events[task.id]
            self._schedule_task(task)

    def _process_new_events(self):
        assert self.task["events"][-1]["eventType"] == "DecisionTaskStarted"
        assert self.task["events"][-2]["eventType"] == "DecisionTaskScheduled"

        for event in self._new_events[:-2]:
            if event["eventType"] in _error_events:
                self._error_events.append(event)
        if self._process_error_events():
            return

        for event in self._new_events[:-2]:
            self._process_event(event)
        self._schedule_tasks()
        self._complete_workflow()

    def build_decisions(self):
        self._get_scheduled_references()
        self._get_activity_task_events()
        self._get_new_events()
        self._process_new_events()


class DAGWorkflow(_base.Workflow):
    """Dag-type SWF workflow specification.

    Args:
        name: workflow name
        version: workflow version
        task_specs: DAG task specifications
    """

    spec_type = "dag"
    decisions_builder = DAGBuilder
    _task_cls = Task

    def __init__(self, name, version, task_specs: t.List[Task], description=None):
        super().__init__(name, version, description)
        self.task_specs = task_specs
        self.dependants = {None: []}

    @classmethod
    def _args_from_spec(cls, spec):
        args, kwargs = super()._args_from_spec(spec)
        tasks = [cls._task_cls.from_spec(s) for s in spec["tasks"]]
        args += (tasks,)
        return args, kwargs

    def _build_dependants(self):
        for activity_task in self.task_specs:
            dependants_task = []
            for other_activity_task in self.task_specs:
                if activity_task.id in (other_activity_task.dependencies or []):
                    dependants_task.append(other_activity_task.id)
            self.dependants[activity_task.id] = dependants_task
            if not activity_task.dependencies:
                self.dependants[None].append(activity_task.id)

    def setup(self):
        self._build_dependants()
