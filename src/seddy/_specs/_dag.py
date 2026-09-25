"""SWF decisions making."""

import json
import string
import typing as t
import logging as lg
import datetime
import dataclasses

import swf_typed

from . import _base

logger = lg.getLogger(__name__)
_jsonpath_characters = string.digits + string.ascii_letters + "_"
_sentinel = object()
_error_events = (
    swf_typed.ActivityTaskFailedEvent,
    swf_typed.ActivityTaskTimedOutEvent,
    swf_typed.CancelTimerFailedEvent,
    swf_typed.CancelWorkflowExecutionFailedEvent,
    swf_typed.CompleteWorkflowExecutionFailedEvent,
    swf_typed.DecisionTaskTimedOutEvent,
    swf_typed.FailWorkflowExecutionFailedEvent,
    swf_typed.RecordMarkerFailedEvent,
    swf_typed.RequestCancelActivityTaskFailedEvent,
    swf_typed.ScheduleActivityTaskFailedEvent,
    swf_typed.StartTimerFailedEvent,
    swf_typed.WorkflowExecutionCancelRequestedEvent,
)
_activity_events = (
    swf_typed.ActivityTaskCompletedEvent,
    swf_typed.ActivityTaskFailedEvent,
    swf_typed.ActivityTaskTimedOutEvent,
    swf_typed.ActivityTaskScheduledEvent,
    swf_typed.ActivityTaskStartedEvent,
)
_decision_failed_events = (
    swf_typed.ScheduleActivityTaskFailedEvent,
    swf_typed.RequestCancelActivityTaskFailedEvent,
    swf_typed.StartTimerFailedEvent,
    swf_typed.CancelTimerFailedEvent,
    swf_typed.StartChildWorkflowExecutionFailedEvent,
    swf_typed.SignalExternalWorkflowExecutionFailedEvent,
    swf_typed.RequestCancelExternalWorkflowExecutionFailedEvent,
    swf_typed.CancelWorkflowExecutionFailedEvent,
    swf_typed.CompleteWorkflowExecutionFailedEvent,
    swf_typed.ContinueAsNewWorkflowExecutionFailedEvent,
    swf_typed.FailWorkflowExecutionFailedEvent,
)
_execution_timeout_events = (
    swf_typed.DecisionTaskTimedOutEvent,
    swf_typed.WorkflowExecutionTimedOutEvent,
)


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
    input: t.Union[NoInput, Constant, WorkflowInput, DependencyResult, Object] = None
    heartbeat: t.Union[int, str] = None
    timeout: t.Union[int, str] = None
    task_list: str = None
    priority: int = None
    dependencies: t.List[str] = None
    _input_cls: t.ClassVar = TaskInput

    @property
    def type(self) -> swf_typed.ActivityId:
        """Activity type."""
        return swf_typed.ActivityId(name=self.name, version=self.version)

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
        return cls(*args, **kwargs)


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
        workflow_started_event = self.task.execution_history[0]
        assert isinstance(
            workflow_started_event, swf_typed.WorkflowExecutionStartedEvent
        )
        # attrs = workflow_started_event["workflowExecutionStartedEventAttributes"]
        decision = swf_typed.ScheduleActivityTaskDecision(
            activity=activity_task.type, task_id=activity_task.id
        )

        # Build input
        input_spec = activity_task.input
        workflow_input = json.loads(workflow_started_event.execution_input)
        activity_results = {}
        for activity_task_id, events in self._activity_task_events.items():
            if activity_task_id in (activity_task.dependencies or []):
                assert isinstance(events[-1], swf_typed.ActivityTaskCompletedEvent)
            elif not events or not isinstance(
                events[-1], swf_typed.ActivityTaskCompletedEvent
            ):
                continue
            if events[-1].task_result or events[-1].task_result == "":
                activity_results[activity_task_id] = json.loads(events[-1].task_result)
        input_ = _build_activity_input(input_spec, workflow_input, activity_results)
        if input_ is not _sentinel:
            decision.task_input = json.dumps(input_)

        # Set other attributes
        decision.task_configuration = swf_typed.PartialTaskConfiguration()
        if activity_task.heartbeat is not None:
            decision.task_configuration.heartbeat_timeout = datetime.timedelta(
                seconds=int(activity_task.heartbeat),
            )
        if activity_task.timeout is not None:
            decision.task_configuration.runtime_timeout = datetime.timedelta(
                seconds=int(activity_task.timeout),
            )
        if activity_task.task_list is not None:
            decision.task_configuration.task_list = activity_task.task_list
        if activity_task.priority is not None:
            decision.task_configuration.priority = activity_task.priority

        self.decisions.append(decision)

    def _get_scheduled_references(self):
        events_by_id = {e.id: e for e in self.task.execution_history}
        for event in self.task.execution_history:
            if isinstance(event, _activity_events):
                if isinstance(event, swf_typed.ActivityTaskScheduledEvent):
                    self._scheduled[event.id] = event
                else:
                    scheduled_id = event.task_scheduled_event_id
                    self._scheduled[event.id] = events_by_id[scheduled_id]

    def _get_activity_task_events(self):
        for event in self.task.execution_history:
            if isinstance(event, _activity_events):
                scheduled_event = self._scheduled[event.id]
                self._activity_task_events[scheduled_event.task_id].append(event)

    def _process_activity_task_completed_event(self, event: swf_typed.Event) -> None:
        scheduled_event = self._scheduled[event.id]
        dependants_task = self.workflow.dependants[scheduled_event.task_id]

        for activity_task_id in dependants_task:
            assert not self._activity_task_events[activity_task_id]
            task = next(a for a in self.workflow.task_specs if a.id == activity_task_id)

            dependencies_satisfied = True
            for dependency_activity_task_id in task.dependencies:
                events = self._activity_task_events[dependency_activity_task_id]
                if not events or not isinstance(
                    events[-1], swf_typed.ActivityTaskCompletedEvent
                ):
                    dependencies_satisfied = False
                    break
            if dependencies_satisfied:
                self._ready_activities.add(task.id)

    def _complete_workflow(self):
        tasks_complete = True
        for events in self._activity_task_events.values():
            if not events or not isinstance(
                events[-1], swf_typed.ActivityTaskCompletedEvent
            ):
                tasks_complete = False
                break

        if tasks_complete:
            result = {}
            for activity_id, events in self._activity_task_events.items():
                assert events and isinstance(
                    events[-1], swf_typed.ActivityTaskCompletedEvent
                )
                if events[-1].task_result or events[-1].task_result == "":
                    result[activity_id] = json.loads(events[-1].task_result)

            decision = swf_typed.CompleteWorkflowExecutionDecision()
            if result:
                decision.execution_result = json.dumps(result)
            self.decisions = [decision]

    def _fail_workflow(self, reason=None, details=None):
        decision = swf_typed.FailWorkflowExecutionDecision()
        if reason:
            decision.reason = reason
        if details:
            decision.details = details
        self.decisions = [decision]

    def _process_decision_failed(self, event: swf_typed.Event) -> bool:
        events_by_id = {e.id: e for e in self.task.execution_history}
        if event.cause == swf_typed.DecisionFailureCause.unauthorised:
            dc_event = events_by_id[event.decision_event_id]
            ds_event = events_by_id[dc_event.decision_task_started_event_id]
            this_ds_event = self.task.execution_history[-1]
            if ds_event.decider_identity == this_ds_event.decider_identity:
                raise _base.DeciderError("Not permitted")
            else:
                return False
        elif event.cause == swf_typed.DecisionFailureCause.unhandled_decision:
            raise _base.DeciderError()

        if isinstance(event, swf_typed.CancelWorkflowExecutionFailedEvent):
            self.decisions = [swf_typed.CancelWorkflowExecutionDecision()]
            return True
        elif isinstance(event, swf_typed.FailWorkflowExecutionFailedEvent):
            return False
        elif isinstance(event, swf_typed.CompleteWorkflowExecutionFailedEvent):
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
            if isinstance(event, swf_typed.ActivityTaskFailedEvent):
                activity_events.append(event)
            elif isinstance(event, swf_typed.ActivityTaskTimedOutEvent):
                if event.timeout_type in (
                    swf_typed.TimeoutType.runtime,
                    swf_typed.TimeoutType.heartbeat,
                ):
                    activity_events.append(event)
                elif event.timeout_type in (
                    swf_typed.TimeoutType.schedule,
                    swf_typed.TimeoutType.total,
                ):
                    time_out_events.append(event)
            elif isinstance(event, swf_typed.WorkflowExecutionCancelRequestedEvent):
                self.decisions = [swf_typed.CancelWorkflowExecutionDecision()]
                return True
            elif isinstance(event, _decision_failed_events):
                if self._process_decision_failed(event):
                    return True
                decider_events.append(event)
            elif isinstance(event, _execution_timeout_events):
                time_out_events.append(event)
            elif isinstance(event, swf_typed.RecordMarkerFailedEvent):
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

    def _process_event(self, event: swf_typed.Event) -> None:
        if isinstance(event, swf_typed.ActivityTaskCompletedEvent):
            self._process_activity_task_completed_event(event)
        elif isinstance(event, swf_typed.WorkflowExecutionStartedEvent):
            self._schedule_initial_activity_tasks()

    def _get_new_events(self):
        event_ids = [e.id for e in self.task.execution_history]
        current_idx = event_ids.index(
            self.task.decision_task_started_execution_history_event_id
        )
        previous_idx = -1
        prev_id = self.task.previous_decision_task_started_execution_history_event_id
        if prev_id in event_ids:
            previous_idx = event_ids.index(prev_id)
        events = self.task.execution_history[previous_idx + 1 : current_idx + 1]
        logger.debug(
            "Processing %d events from index %d (ID: %s) to %d (ID: %s)",
            len(events),
            previous_idx + 1,
            events[0].id,
            current_idx,
            events[-1].id,
        )
        self._new_events = events

    def _schedule_tasks(self):
        for task_id in self._ready_activities:
            task = next(ts for ts in self.workflow.task_specs if ts.id == task_id)
            assert not self._activity_task_events[task.id]
            self._schedule_task(task)

    def _process_new_events(self):
        assert isinstance(
            self.task.execution_history[-1], swf_typed.DecisionTaskStartedEvent
        )
        assert isinstance(
            self.task.execution_history[-2], swf_typed.DecisionTaskScheduledEvent
        )

        for event in self._new_events[:-2]:
            if isinstance(event, _error_events):
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
