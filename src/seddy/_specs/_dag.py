"""SWF decisions making."""

import abc
import json
import string
import dataclasses
import typing as t
import logging as lg

from . import _base

logger = lg.getLogger(__name__)
_jsonpath_characters = string.digits + string.ascii_letters + "_"
_sentinel = object()
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
class TaskInput:
    @staticmethod
    def from_spec(spec: t.Dict[str, t.Any]) -> "TaskInput":
        for cls in [NoInput, Constant, WorkflowInput, DependencyResult, Object]:
            if cls.type == spec["type"]:
                break
        else:  # TODO: unit-test
            raise ValueError(spec["type"])
        return cls.from_spec(spec)


@dataclasses.dataclass
class NoInput(TaskInput):
    type: t.ClassVar = "none"

    @classmethod
    def from_spec(cls, spec) -> "NoInput":
        return cls()


@dataclasses.dataclass
class Constant(TaskInput):
    type: t.ClassVar = "constant"
    value: t.Any

    @classmethod
    def from_spec(cls, spec) -> "Constant":
        return cls(spec["value"])


@dataclasses.dataclass
class WorkflowInput(TaskInput):
    type: t.ClassVar = "workflow-input"
    path: str = "$"
    default: t.Any = _sentinel

    @classmethod
    def from_spec(cls, spec) -> "WorkflowInput":
        kwargs = {}
        if "path" in spec:
            kwargs["path"] = spec["path"]
        if "default" in spec:
            kwargs["default"] = spec["default"]
        return cls(**kwargs)


@dataclasses.dataclass
class DependencyResult(TaskInput):
    type: t.ClassVar = "dependency-result"
    id: t.Any
    path: str = "$"
    default: t.Any = _sentinel

    @classmethod
    def from_spec(cls, spec) -> "DependencyResult":
        kwargs = {}
        if "path" in spec:
            kwargs["path"] = spec["path"]
        if "default" in spec:
            kwargs["default"] = spec["default"]
        return cls(spec["id"], **kwargs)


@dataclasses.dataclass
class Object(TaskInput):
    type: t.ClassVar = "object"
    items: t.Dict[str, TaskInput]

    @classmethod
    def from_spec(cls, spec) -> "Object":
        items = {}
        for key, subspec in spec["items"].items():
            items[key] = cls.from_spec(subspec)
        return cls(items)


@dataclasses.dataclass
class Condition(metaclass=abc.ABCMeta):  # TODO: unit-test
    @staticmethod
    def from_spec(spec: t.Dict[str, t.Any]) -> "Condition":
        for cls in [Equal, NotEqual, LessThan, LessThanOrEqual, In, And, Or, Not]:
            if cls.type == spec["type"]:
                return cls.from_spec(spec)
        raise ValueError(spec["type"])

    @abc.abstractmethod
    def is_true(
        self,
        workflow_input: t.Union[t.Dict[str, t.Any], None],
        activity_results: t.Dict[str, t.Any],
    ) -> bool:
        raise NotImplementedError


@dataclasses.dataclass
class ComparisonBase(Condition, metaclass=abc.ABCMeta):  # TODO: unit-test
    lhs: TaskInput
    rhs: TaskInput

    @classmethod
    def from_spec(cls, spec):
        lhs = TaskInput.from_spec(spec["lhs"])
        rhs = TaskInput.from_spec(spec["rhs"])
        return cls(lhs, rhs)

    def is_true(self, workflow_input, activity_results) -> bool:
        lhs = _build_activity_input(self.lhs, workflow_input, activity_results)
        rhs = _build_activity_input(self.rhs, workflow_input, activity_results)
        return self._compare(lhs, rhs)

    @staticmethod
    @abc.abstractmethod
    def _compare(lhs: t.Any, rhs: t.Any) -> bool:  # TODO: unit-test
        raise NotImplementedError


@dataclasses.dataclass
class Equal(ComparisonBase):  # TODO: unit-test
    type: t.ClassVar = "="
    _compare = staticmethod(lambda l, r: l == r)


@dataclasses.dataclass
class NotEqual(ComparisonBase):  # TODO: unit-test
    type: t.ClassVar = "!="
    _compare = staticmethod(lambda l, r: l != r)


@dataclasses.dataclass
class LessThan(ComparisonBase):  # TODO: unit-test
    type: t.ClassVar = "<"
    _compare = staticmethod(lambda l, r: l < r)


@dataclasses.dataclass
class LessThanOrEqual(ComparisonBase):  # TODO: unit-test
    type: t.ClassVar = "<="
    _compare = staticmethod(lambda l, r: l <= r)


@dataclasses.dataclass
class In(ComparisonBase):  # TODO: unit-test
    type: t.ClassVar = "in"
    _compare = staticmethod(lambda l, r: l in r)


@dataclasses.dataclass
class BinaryLogicalBase(Condition, metaclass=abc.ABCMeta):  # TODO: unit-test
    lhs: Condition
    rhs: Condition

    @classmethod
    def from_spec(cls, spec):
        lhs = Condition.from_spec(spec["lhs"])
        rhs = Condition.from_spec(spec["rhs"])
        return cls(lhs, rhs)

    def is_true(self, workflow_input, activity_results) -> bool:
        lhs = self.lhs.is_true(workflow_input, activity_results)
        rhs = self.rhs.is_true(workflow_input, activity_results)
        return self._combine(lhs, rhs)

    @staticmethod
    @abc.abstractmethod
    def _combine(lhs: t.Any, rhs: t.Any) -> bool:
        raise NotImplementedError


@dataclasses.dataclass
class And(BinaryLogicalBase):  # TODO: unit-test
    type: t.ClassVar = "and"
    _combine = staticmethod(lambda l, r: l and r)


@dataclasses.dataclass
class Or(BinaryLogicalBase):  # TODO: unit-test
    type: t.ClassVar = "or"
    _combine = staticmethod(lambda l, r: l or r)


@dataclasses.dataclass
class Not(Condition):  # TODO: unit-test
    type: t.ClassVar = "not"
    value: Condition

    @classmethod
    def from_spec(cls, spec):
        value = Condition.from_spec(spec["value"])
        return cls(value)

    def is_true(self, workflow_input, activity_results) -> bool:
        return not self.value.is_true(workflow_input, activity_results)


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
        skip_if: condition to skip task
    """

    id: str
    name: str
    version: str
    input: t.Union[NoInput, Constant, WorkflowInput, DependencyResult, Object] = None
    heartbeat: t.Union[int, str] = None
    timeout: t.Union[int, str] = None
    task_list: str = None
    priority: int = None
    dependencies: t.List[str] = None
    skip_if: Condition = None
    _input_cls: t.ClassVar = TaskInput
    _condition_cls: t.ClassVar = Condition

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
        if "input" in spec:
            kwargs["input"] = cls._input_cls.from_spec(spec["input"])
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
        if "skip_if" in spec:
            kwargs["skip_if"] = cls._condition_cls.from_spec(spec["skip_if"])
        return cls(*args, **kwargs)


def _get(item_id, items, id_key):
    """Get item from list with given ID."""
    return next(item for item in items if item[id_key] == item_id)


def _get_item_jsonpath(path: str, obj, default: t.Any = _sentinel) -> t.Any:
    """Get a child item from an object.

    Args:
        path: path to child item, using basic single-valued JSONPath
            syntax
        obj: object to get child item from
        default: default value if missing

    Returns:
        pointed-to child item

    Raises:
        ValueError: invalid path
    """

    if path[0] != "$":
        raise ValueError("invalid path (must start at root): %s" % path)

    indices = []
    chars = []
    state = None
    for char in path[1:]:
        if char in ".[":
            if state:
                if state != ".":
                    raise ValueError("invalid path (missing closing ']'): %s" % path)
                elif not chars:
                    raise ValueError("invalid path (empty key): %s" % path)
                indices.append("".join(chars))
            chars = []
            state = char
        elif char == "]":
            if state != "[":
                raise ValueError("invalid path (invalid key): %s" % path)
            elif not chars:
                raise ValueError("invalid path (empty key): %s" % path)
            index = int("".join(chars))
            indices.append(index)
            chars = []
            state = None
        elif char not in _jsonpath_characters:
            raise ValueError("invalid path (illegal characters): %s" % path)
        else:
            if not state:
                raise ValueError("invalid path (missing '.' or '['): %s" % path)
            chars.append(char)
    if state == "[":
        raise ValueError("invalid path (missing closing ']'): %s" % path)
    elif state == ".":
        indices.append("".join(chars))

    item = obj
    for index in indices:
        try:
            item = item[index]
        except (KeyError, IndexError):
            if default != _sentinel:
                return default
            raise
    return item


def _build_activity_input(
    input_spec: TaskInput,
    workflow_input: t.Union[t.Dict[str, t.Any], None],
    activity_results: t.Dict[str, t.Any],
) -> t.Any:
    """Build activity input.

    Args:
        input_spec: activity task input specification
        workflow_input: workflow input
        activity_results: activities' results

    Returns:
        activity task input
    """

    input_spec = input_spec or NoInput()
    if isinstance(input_spec, NoInput):
        return _sentinel
    if isinstance(input_spec, Constant):
        return input_spec.value
    if isinstance(input_spec, WorkflowInput):
        path = input_spec.path
        return _get_item_jsonpath(path, workflow_input, input_spec.default)
    if isinstance(input_spec, DependencyResult):
        path = input_spec.path
        dependency_result = activity_results[input_spec.id]
        return _get_item_jsonpath(path, dependency_result, input_spec.default)
    if isinstance(input_spec, Object):
        input_ = {}
        for key, subspec in input_spec.items.items():
            value = _build_activity_input(subspec, workflow_input, activity_results)
            if value is not _sentinel:
                input_[key] = value
        return input_
    else:
        raise TypeError(input_spec)


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
        # Get workflow-start event
        workflow_started_event = self.task["events"][0]
        assert workflow_started_event["eventType"] == "WorkflowExecutionStarted"
        attrs = workflow_started_event["workflowExecutionStartedEventAttributes"]
        decision_attributes = {
            "activityId": activity_task.id,
            "activityType": activity_task.type,
        }

        # Get workflow input and activity task results
        input_spec = activity_task.input
        workflow_input = json.loads(attrs.get("input", "null"))
        activity_results = {}
        for activity_task_id, events in self._activity_task_events.items():
            if activity_task_id in (activity_task.dependencies or []):
                assert events[-1]["eventType"] == "ActivityTaskCompleted"
            elif not events or events[-1]["eventType"] != "ActivityTaskCompleted":
                continue
            d_attrs = events[-1].get("activityTaskCompletedEventAttributes", {})
            if "result" in d_attrs:
                activity_results[activity_task_id] = json.loads(d_attrs["result"])

        # Skip if required
        if (
            activity_task.skip_if
            and activity_task.skip_if.is_true(workflow_input, activity_results)
        ):
            logger.info(f"Skipping activity task '{activity_task.id}', as requested")
            self._process_completed_activity_task(activity_task.id)
            return

        # Build input
        input_ = _build_activity_input(input_spec, workflow_input, activity_results)
        if input_ is not _sentinel:
            decision_attributes["input"] = json.dumps(input_)

        # Set other attributes
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
        self._process_completed_activity_task(attrs["activityId"])

    def _process_completed_activity_task(self, activity_id: str) -> None:
        dependants_task = self.workflow.dependants[activity_id]

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
    dependants: t.Dict[t.Union[None, str], t.List[str]]

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
