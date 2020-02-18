"""Test ``seddy.decisions._dag``."""

from seddy import decisions as seddy_decisions
import pytest


class TestDAGBuilder:
    """Test ``seddy.decisions.DAGBuilder``."""

    @pytest.fixture
    def workflow(self):
        """Example DAG workflow specification."""
        workflow = seddy_decisions.DAGWorkflow(
            {
                "name": "foo",
                "version": "0.42",
                "tasks": [
                    {
                        "id": "foo",
                        "type": {"name": "spam-foo", "version": "0.3"},
                        "heartbeat": 60,
                        "timeout": 86400,
                        "task_list": "eggs",
                        "priority": 1,
                    },
                    {
                        "id": "bar",
                        "type": {"name": "spam-bar", "version": "0.1"},
                        "heartbeat": 60,
                        "timeout": 86400,
                        "dependencies": ["foo"],
                    },
                    {
                        "id": "yay",
                        "type": {"name": "spam-foo", "version": "0.3"},
                        "heartbeat": 60,
                        "timeout": 86400,
                        "dependencies": ["foo"],
                    },
                ],
                "type": "dag",
            }
        )
        workflow.setup()
        return workflow

    @pytest.fixture(
        params=[
            "workflow-start",
            "other-event",
            "foo-complete",
            "foo-complete-yay-unsatisfied",
            "yay-complete",
            "bar-complete",
            "bar-and-yay-complete",
            "foo-failed",
            "foo-timed-out",
            "workflow-cancelled",
            "workflow-cancelled-bar-yay",
        ]
    )
    def _task_and_expected_decisions(self, request, workflow):
        """Example task and expected associated decisions."""
        # Events sections
        workflow_start_events = [
            {
                "eventId": 1,
                "eventType": "WorkflowExecutionStarted",
                "workflowExecutionStartedEventAttributes": {
                    "input": (
                        "{\n"
                        '    "foo": {"spam": [42], "eggs": null},\n'
                        '    "bar": null,\n'
                        '    "yay": {"spam": [17], "eggs": [42]}\n'
                        "}"
                    )
                },
            },
            {"eventId": 2, "eventType": "DecisionTaskScheduled"},
            {"eventId": 3, "eventType": "DecisionTaskStarted"},
        ]
        other_event_events = [
            {"eventId": 4, "eventType": "DecisionTaskCompleted"},
            {
                "eventId": 5,
                "eventType": "WorkflowExecutionSignaled",
                "workflowExecutionSignaledEventAttributes": {"signalName": "blue"},
            },
            {"eventId": 6, "eventType": "DecisionTaskScheduled"},
            {"eventId": 7, "eventType": "DecisionTaskStarted"},
        ]
        foo_complete_events = [
            {"eventId": 4, "eventType": "DecisionTaskCompleted"},
            {
                "eventId": 5,
                "eventType": "ActivityTaskScheduled",
                "activityTaskScheduledEventAttributes": {
                    "activityId": "foo",
                    "activityType": {"name": "spam-foo", "version": "0.3"},
                    "decisionTaskCompletedEventId": 4,
                    "input": '{"spam": [42], "eggs": null}',
                },
            },
            {
                "eventId": 6,
                "eventType": "ActivityTaskStarted",
                "activityTaskStartedEventAttributes": {"scheduledEventId": 5},
            },
            {
                "eventId": 7,
                "eventType": "ActivityTaskCompleted",
                "activityTaskCompletedEventAttributes": {
                    "scheduledEventId": 5,
                    "result": "3",
                },
            },
            {"eventId": 8, "eventType": "DecisionTaskScheduled"},
            {"eventId": 9, "eventType": "DecisionTaskStarted"},
        ]
        yay_complete_events = [
            {"eventId": 10, "eventType": "DecisionTaskCompleted"},
            {
                "eventId": 11,
                "eventType": "ActivityTaskScheduled",
                "activityTaskScheduledEventAttributes": {
                    "activityId": "yay",
                    "activityType": {"name": "spam-foo", "version": "0.3"},
                    "decisionTaskCompletedEventId": 10,
                    "input": '{"spam": [17], "eggs": [42]}',
                },
            },
            {
                "eventId": 12,
                "eventType": "ActivityTaskScheduled",
                "activityTaskScheduledEventAttributes": {
                    "activityId": "bar",
                    "activityType": {"name": "spam-bar", "version": "0.1"},
                    "decisionTaskCompletedEventId": 10,
                    "input": "null",
                },
            },
            {
                "eventId": 13,
                "eventType": "ActivityTaskStarted",
                "activityTaskStartedEventAttributes": {"scheduledEventId": 11},
            },
            {
                "eventId": 14,
                "eventType": "ActivityTaskStarted",
                "activityTaskStartedEventAttributes": {"scheduledEventId": 12},
            },
            {
                "eventId": 15,
                "eventType": "ActivityTaskCompleted",
                "activityTaskCompletedEventAttributes": {
                    "scheduledEventId": 11,
                    "result": "5",
                },
            },
            {"eventId": 16, "eventType": "DecisionTaskScheduled"},
            {"eventId": 17, "eventType": "DecisionTaskStarted"},
        ]
        bar_complete_events = [
            {"eventId": 18, "eventType": "DecisionTaskCompleted"},
            {
                "eventId": 19,
                "eventType": "ActivityTaskCompleted",
                "activityTaskCompletedEventAttributes": {
                    "scheduledEventId": 12,
                    "result": '{"a": 9, "b": "red"}',
                },
            },
            {"eventId": 20, "eventType": "DecisionTaskScheduled"},
            {"eventId": 21, "eventType": "DecisionTaskStarted"},
        ]
        bar_and_yay_complete_events = [
            {"eventId": 10, "eventType": "DecisionTaskCompleted"},
            {
                "eventId": 11,
                "eventType": "ActivityTaskScheduled",
                "activityTaskScheduledEventAttributes": {
                    "activityId": "yay",
                    "activityType": {"name": "spam-foo", "version": "0.3"},
                    "decisionTaskCompletedEventId": 10,
                    "input": '{"spam": [17], "eggs": [42]}',
                },
            },
            {
                "eventId": 12,
                "eventType": "ActivityTaskScheduled",
                "activityTaskScheduledEventAttributes": {
                    "activityId": "bar",
                    "activityType": {"name": "spam-bar", "version": "0.1"},
                    "decisionTaskCompletedEventId": 10,
                    "input": "null",
                },
            },
            {
                "eventId": 13,
                "eventType": "ActivityTaskStarted",
                "activityTaskStartedEventAttributes": {"scheduledEventId": 11},
            },
            {
                "eventId": 14,
                "eventType": "ActivityTaskStarted",
                "activityTaskStartedEventAttributes": {"scheduledEventId": 12},
            },
            {
                "eventId": 15,
                "eventType": "ActivityTaskCompleted",
                "activityTaskCompletedEventAttributes": {
                    "scheduledEventId": 11,
                    "result": "5",
                },
            },
            {
                "eventId": 16,
                "eventType": "ActivityTaskCompleted",
                "activityTaskCompletedEventAttributes": {
                    "scheduledEventId": 12,
                    "result": '{"a": 9, "b": "red"}',
                },
            },
            {"eventId": 17, "eventType": "DecisionTaskScheduled"},
            {"eventId": 18, "eventType": "DecisionTaskStarted"},
        ]
        foo_failed_events = [
            {"eventId": 4, "eventType": "DecisionTaskCompleted"},
            {
                "eventId": 5,
                "eventType": "ActivityTaskScheduled",
                "activityTaskScheduledEventAttributes": {
                    "activityId": "foo",
                    "activityType": {"name": "spam-foo", "version": "0.3"},
                    "decisionTaskCompletedEventId": 4,
                    "input": '{"spam": [42], "eggs": null}',
                },
            },
            {
                "eventId": 6,
                "eventType": "ActivityTaskStarted",
                "activityTaskStartedEventAttributes": {"scheduledEventId": 5},
            },
            {
                "eventId": 7,
                "eventType": "ActivityTaskFailed",
                "activityTaskFailedEventAttributes": {
                    "scheduledEventId": 5,
                    "details": "The specified spam does not exist",
                    "reason": "spamError",
                },
            },
            {"eventId": 8, "eventType": "DecisionTaskScheduled"},
            {"eventId": 9, "eventType": "DecisionTaskStarted"},
        ]
        foo_timed_out_events = [
            {"eventId": 4, "eventType": "DecisionTaskCompleted"},
            {
                "eventId": 5,
                "eventType": "ActivityTaskScheduled",
                "activityTaskScheduledEventAttributes": {
                    "activityId": "foo",
                    "activityType": {"name": "spam-foo", "version": "0.3"},
                    "decisionTaskCompletedEventId": 4,
                    "input": '{"spam": [42], "eggs": null}',
                },
            },
            {
                "eventId": 6,
                "eventType": "ActivityTaskStarted",
                "activityTaskStartedEventAttributes": {"scheduledEventId": 5},
            },
            {
                "eventId": 7,
                "eventType": "ActivityTaskTimedOut",
                "activityTaskTimedOutEventAttributes": {
                    "scheduledEventId": 5,
                    "details": "42 / 50",
                    "timeoutType": "HEARTBEAT",
                },
            },
            {"eventId": 8, "eventType": "DecisionTaskScheduled"},
            {"eventId": 9, "eventType": "DecisionTaskStarted"},
        ]
        workflow_cancelled_events = [
            {"eventId": 4, "eventType": "DecisionTaskCompleted"},
            {
                "eventId": 5,
                "eventType": "ActivityTaskScheduled",
                "activityTaskScheduledEventAttributes": {
                    "activityId": "foo",
                    "activityType": {"name": "spam-foo", "version": "0.3"},
                    "decisionTaskCompletedEventId": 4,
                    "input": '{"spam": [42], "eggs": null}',
                },
            },
            {
                "eventId": 6,
                "eventType": "ActivityTaskStarted",
                "activityTaskStartedEventAttributes": {"scheduledEventId": 5},
            },
            {"eventId": 7, "eventType": "WorkflowExecutionCancelRequested"},
            {"eventId": 8, "eventType": "DecisionTaskScheduled"},
            {"eventId": 9, "eventType": "DecisionTaskStarted"},
        ]
        workflow_cancelled_bar_yay_events = [
            {"eventId": 10, "eventType": "DecisionTaskCompleted"},
            {
                "eventId": 11,
                "eventType": "ActivityTaskScheduled",
                "activityTaskScheduledEventAttributes": {
                    "activityId": "yay",
                    "activityType": {"name": "spam-foo", "version": "0.3"},
                    "decisionTaskCompletedEventId": 10,
                    "input": '{"spam": [17], "eggs": [42]}',
                },
            },
            {
                "eventId": 12,
                "eventType": "ActivityTaskScheduled",
                "activityTaskScheduledEventAttributes": {
                    "activityId": "bar",
                    "activityType": {"name": "spam-bar", "version": "0.1"},
                    "decisionTaskCompletedEventId": 10,
                    "input": "null",
                },
            },
            {
                "eventId": 13,
                "eventType": "ActivityTaskStarted",
                "activityTaskStartedEventAttributes": {"scheduledEventId": 11},
            },
            {"eventId": 14, "eventType": "WorkflowExecutionCancelRequested"},
            {"eventId": 15, "eventType": "DecisionTaskScheduled"},
            {"eventId": 16, "eventType": "DecisionTaskStarted"},
        ]

        # Scenarios
        task = None
        expected_decisions = []
        if request.param == "workflow-start":
            task = {
                "taskToken": "spam",
                "previousStartedEventId": 0,
                "startedEventId": workflow_start_events[-1]["eventId"],
                "events": workflow_start_events,
            }
            expected_decisions = [
                {
                    "decisionType": "ScheduleActivityTask",
                    "scheduleActivityTaskDecisionAttributes": {
                        "activityId": "foo",
                        "activityType": {"name": "spam-foo", "version": "0.3"},
                        "input": '{"spam": [42], "eggs": null}',
                        "heartbeatTimeout": "60",
                        "startToCloseTimeout": "86400",
                        "taskPriority": "1",
                        "taskList": "eggs",
                    },
                },
            ]
        if request.param == "other-event":
            task = {
                "taskToken": "spam",
                "previousStartedEventId": workflow_start_events[-1]["eventId"],
                "startedEventId": other_event_events[-1]["eventId"],
                "events": workflow_start_events + other_event_events,
            }
            expected_decisions = []
        elif request.param == "foo-complete":
            task = {
                "taskToken": "spam",
                "previousStartedEventId": workflow_start_events[-1]["eventId"],
                "startedEventId": foo_complete_events[-1]["eventId"],
                "events": workflow_start_events + foo_complete_events,
            }
            expected_decisions = [
                {
                    "decisionType": "ScheduleActivityTask",
                    "scheduleActivityTaskDecisionAttributes": {
                        "activityId": "bar",
                        "activityType": {"name": "spam-bar", "version": "0.1"},
                        "input": "null",
                        "heartbeatTimeout": "60",
                        "startToCloseTimeout": "86400",
                    },
                },
                {
                    "decisionType": "ScheduleActivityTask",
                    "scheduleActivityTaskDecisionAttributes": {
                        "activityId": "yay",
                        "activityType": {"name": "spam-foo", "version": "0.3"},
                        "input": '{"spam": [17], "eggs": [42]}',
                        "heartbeatTimeout": "60",
                        "startToCloseTimeout": "86400",
                    },
                },
            ]
        elif request.param == "foo-complete-yay-unsatisfied":
            workflow.dependants["bar"] = ["yay"]
            assert workflow.spec["tasks"][2]["id"] == "yay"
            workflow.spec["tasks"][2]["dependencies"] = ["foo", "bar"]

            task = {
                "taskToken": "spam",
                "previousStartedEventId": workflow_start_events[-1]["eventId"],
                "startedEventId": foo_complete_events[-1]["eventId"],
                "events": workflow_start_events + foo_complete_events,
            }
            expected_decisions = [
                {
                    "decisionType": "ScheduleActivityTask",
                    "scheduleActivityTaskDecisionAttributes": {
                        "activityId": "bar",
                        "activityType": {"name": "spam-bar", "version": "0.1"},
                        "input": "null",
                        "heartbeatTimeout": "60",
                        "startToCloseTimeout": "86400",
                    },
                },
            ]
        elif request.param == "yay-complete":
            task = {
                "taskToken": "spam",
                "previousStartedEventId": foo_complete_events[-1]["eventId"],
                "startedEventId": yay_complete_events[-1]["eventId"],
                "events": (
                    workflow_start_events + foo_complete_events + yay_complete_events
                ),
            }
            expected_decisions = []
        elif request.param == "bar-complete":
            task = {
                "taskToken": "spam",
                "previousStartedEventId": yay_complete_events[-1]["eventId"],
                "startedEventId": bar_complete_events[-1]["eventId"],
                "events": (
                    workflow_start_events
                    + foo_complete_events
                    + yay_complete_events
                    + bar_complete_events
                ),
            }
            expected_decisions = [
                {
                    "decisionType": "CompleteWorkflowExecution",
                    "completeWorkflowExecutionDecisionAttributes": {
                        "result": '{"foo": 3, "bar": {"a": 9, "b": "red"}, "yay": 5}'
                    },
                }
            ]
        elif request.param == "bar-and-yay-complete":
            task = {
                "taskToken": "spam",
                "previousStartedEventId": foo_complete_events[-1]["eventId"],
                "startedEventId": bar_and_yay_complete_events[-1]["eventId"],
                "events": (
                    workflow_start_events
                    + foo_complete_events
                    + bar_and_yay_complete_events
                ),
            }
            expected_decisions = [
                {
                    "decisionType": "CompleteWorkflowExecution",
                    "completeWorkflowExecutionDecisionAttributes": {
                        "result": '{"foo": 3, "bar": {"a": 9, "b": "red"}, "yay": 5}'
                    },
                }
            ]
        elif request.param == "foo-failed":
            task = {
                "taskToken": "spam",
                "previousStartedEventId": workflow_start_events[-1]["eventId"],
                "startedEventId": foo_failed_events[-1]["eventId"],
                "events": workflow_start_events + foo_failed_events,
            }
            expected_decisions = [
                {
                    "decisionType": "FailWorkflowExecution",
                    "failWorkflowExecutionDecisionAttributes": {
                        "reason": "activityFailure",
                        "details": "Activity 'foo' failed",
                    },
                }
            ]
        elif request.param == "foo-timed-out":
            task = {
                "taskToken": "spam",
                "previousStartedEventId": workflow_start_events[-1]["eventId"],
                "startedEventId": foo_timed_out_events[-1]["eventId"],
                "events": workflow_start_events + foo_timed_out_events,
            }
            expected_decisions = [
                {
                    "decisionType": "FailWorkflowExecution",
                    "failWorkflowExecutionDecisionAttributes": {
                        "reason": "activityTimeOut",
                        "details": "Activity 'foo' timed-out",
                    },
                }
            ]
        elif request.param == "workflow-cancelled":
            task = {
                "taskToken": "spam",
                "previousStartedEventId": workflow_start_events[-1]["eventId"],
                "startedEventId": workflow_cancelled_events[-1]["eventId"],
                "events": workflow_start_events + workflow_cancelled_events,
            }
            expected_decisions = [
                {
                    "decisionType": "RequestCancelActivityTask",
                    "requestCancelActivityTaskDecisionAttributes": {
                        "activityId": "foo"
                    },
                },
                {"decisionType": "CancelWorkflowExecution"},
            ]
        elif request.param == "workflow-cancelled-bar-yay":
            task = {
                "taskToken": "spam",
                "previousStartedEventId": foo_complete_events[-1]["eventId"],
                "startedEventId": workflow_cancelled_bar_yay_events[-1]["eventId"],
                "events": (
                    workflow_start_events
                    + foo_complete_events
                    + workflow_cancelled_bar_yay_events
                ),
            }
            expected_decisions = [
                {
                    "decisionType": "RequestCancelActivityTask",
                    "requestCancelActivityTaskDecisionAttributes": {
                        "activityId": "bar"
                    },
                },
                {
                    "decisionType": "RequestCancelActivityTask",
                    "requestCancelActivityTaskDecisionAttributes": {
                        "activityId": "yay"
                    },
                },
                {"decisionType": "CancelWorkflowExecution"},
            ]
        return task, expected_decisions

    @pytest.fixture
    def task(self, _task_and_expected_decisions):
        """Example task."""
        return _task_and_expected_decisions[0]

    @pytest.fixture
    def expected_decisions(self, _task_and_expected_decisions):
        """Example expected decisions."""
        return _task_and_expected_decisions[1]

    @pytest.fixture
    def instance(self, workflow, task):
        """DAG decisions builder instance."""
        return seddy_decisions.DAGBuilder(workflow, task)

    def test_build_decisions(self, instance, expected_decisions):
        """Test DAG decisions building."""
        instance.build_decisions()
        assert instance.decisions == expected_decisions


class TestWorkflow:
    """Test ``seddy.decisions.DAGWorkflow``."""

    @pytest.fixture
    def spec(self):
        """Example DAG-type workflow specification."""
        return {
            "name": "foo",
            "version": "0.42",
            "tasks": [
                {
                    "id": "foo",
                    "type": {"name": "spam-foo", "version": "0.3"},
                    "heartbeat": "60",
                    "timeout": "86400",
                    "task_list": "eggs",
                    "priority": "1",
                },
                {
                    "id": "bar",
                    "type": {"name": "spam-bar", "version": "0.1"},
                    "heartbeat": "60",
                    "timeout": "86400",
                    "dependencies": "foo",
                },
                {
                    "id": "yay",
                    "type": {"name": "spam-foo", "version": "0.3"},
                    "heartbeat": "60",
                    "timeout": "86400",
                    "dependencies": "foo",
                },
            ],
            "type": "dag",
        }

    @pytest.fixture
    def instance(self, spec):
        """DAG-type workflow specification instance."""
        return seddy_decisions.DAGWorkflow(spec)

    def test_init(self, instance, spec):
        """Test DAG-type workflow specification initialisation."""
        assert instance.spec is spec
        assert instance.spec_type == "dag"
        assert instance.decisions_builder is seddy_decisions.DAGBuilder
        assert not instance.dependants

    def test_setup(self, instance):
        """Test DAG-type workflow specification pre-computation."""
        instance.setup()
        assert instance.dependants == {"foo": ["bar", "yay"], "bar": [], "yay": []}
