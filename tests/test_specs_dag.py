"""Test ``seddy._specs._dag``."""

from seddy import _specs as seddy_specs
import pytest


class TestDAGBuilder:
    """Test ``seddy._specs.DAGBuilder``."""

    @pytest.fixture
    def workflow(self):
        """Example DAG workflow specification."""
        workflow = seddy_specs.DAGWorkflow.from_spec(
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
                    {
                        "id": "tin",
                        "type": {"name": "spam-tin", "version": "1.2"},
                        "heartbeat": 30,
                        "timeout": 43200,
                        "dependencies": ["yay"],
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
            "tin-complete",
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
                "eventType": "ActivityTaskScheduled",
                "activityTaskScheduledEventAttributes": {
                    "activityId": "tin",
                    "activityType": {"name": "spam-tin", "version": "1.2"},
                    "decisionTaskCompletedEventId": 18,
                },
            },
            {
                "eventId": 20,
                "eventType": "ActivityTaskStarted",
                "activityTaskStartedEventAttributes": {"scheduledEventId": 19},
            },
            {
                "eventId": 21,
                "eventType": "ActivityTaskCompleted",
                "activityTaskCompletedEventAttributes": {
                    "scheduledEventId": 12,
                    "result": '{"a": 9, "b": "red"}',
                },
            },
            {"eventId": 22, "eventType": "DecisionTaskScheduled"},
            {"eventId": 23, "eventType": "DecisionTaskStarted"},
        ]
        tin_complete_events = [
            {"eventId": 24, "eventType": "DecisionTaskCompleted"},
            {
                "eventId": 25,
                "eventType": "ActivityTaskCompleted",
                "activityTaskCompletedEventAttributes": {"scheduledEventId": 19},
            },
            {"eventId": 26, "eventType": "DecisionTaskScheduled"},
            {"eventId": 27, "eventType": "DecisionTaskStarted"},
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
                        "taskList": {"name": "eggs"},
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
            assert workflow.task_specs[2]["id"] == "yay"
            workflow.task_specs[2]["dependencies"] = ["foo", "bar"]

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
        elif request.param in "yay-complete":
            task = {
                "taskToken": "spam",
                "previousStartedEventId": foo_complete_events[-1]["eventId"],
                "startedEventId": yay_complete_events[-1]["eventId"],
                "events": (
                    workflow_start_events + foo_complete_events + yay_complete_events
                ),
            }
            expected_decisions = [
                {
                    "decisionType": "ScheduleActivityTask",
                    "scheduleActivityTaskDecisionAttributes": {
                        "activityId": "tin",
                        "activityType": {"name": "spam-tin", "version": "1.2"},
                        "heartbeatTimeout": "30",
                        "startToCloseTimeout": "43200",
                    },
                },
            ]
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
            expected_decisions = []
        elif request.param == "tin-complete":
            task = {
                "taskToken": "spam",
                "previousStartedEventId": bar_complete_events[-1]["eventId"],
                "startedEventId": tin_complete_events[-1]["eventId"],
                "events": (
                    workflow_start_events
                    + foo_complete_events
                    + yay_complete_events
                    + bar_complete_events
                    + tin_complete_events
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
                    "decisionType": "ScheduleActivityTask",
                    "scheduleActivityTaskDecisionAttributes": {
                        "activityId": "tin",
                        "activityType": {"name": "spam-tin", "version": "1.2"},
                        "heartbeatTimeout": "30",
                        "startToCloseTimeout": "43200",
                    },
                },
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
        return seddy_specs.DAGBuilder(workflow, task)

    def test_build_decisions(self, instance, expected_decisions):
        """Test DAG decisions building."""
        instance.build_decisions()
        assert instance.decisions == expected_decisions

    @pytest.mark.parametrize(
        ("cause", "identity", "event_type", "exp"),
        [
            pytest.param(
                "OPERATION_NOT_PERMITTED",
                "spam-1234",
                "ScheduleActivityTaskFailed",
                seddy_specs._base.DeciderError("Not permitted"),
                id="permissions-this",
            ),
            pytest.param(
                "OPERATION_NOT_PERMITTED",
                "spam-1235",
                "ScheduleActivityTaskFailed",
                [
                    {
                        "decisionType": "FailWorkflowExecution",
                        "failWorkflowExecutionDecisionAttributes": {
                            "reason": "DeciderError",
                        },
                    }
                ],
                id="permissions-other",
            ),
            pytest.param(
                "INVALID_INPUT",
                "spam-1234",
                "CancelWorkflowExecutionFailed",
                seddy_specs._base.DeciderError(),
                id="error",
            ),
            pytest.param(
                "UNHANDLED_DECISION",
                "spam-1234",
                "CancelWorkflowExecutionFailed",
                [{"decisionType": "CancelWorkflowExecution"}],
                id="unhandled-cancel",
            ),
            pytest.param(
                "UNHANDLED_DECISION",
                "spam-1234",
                "FailWorkflowExecutionFailed",
                [
                    {
                        "decisionType": "FailWorkflowExecution",
                        "failWorkflowExecutionDecisionAttributes": {
                            "reason": "FailRetry",
                        },
                    }
                ],
                id="unhandled-fail",
            ),
            pytest.param(
                "UNHANDLED_DECISION",
                "spam-1234",
                "CompleteWorkflowExecutionFailed",
                [],
                id="unhandled-complete",
            ),
        ],
    )
    def test_decision_failure(self, workflow, cause, identity, event_type, exp):
        """Test decision failure handling."""
        # Build input
        event_attr_key = event_type[0].lower() + event_type[1:] + "EventAttributes"
        task = {
            "taskToken": "spam",
            "previousStartedEventId": 3,
            "startedEventId": 7,
            "events": [
                {"eventId": 1, "eventType": "WorkflowExecutionStarted"},
                {"eventId": 2, "eventType": "DecisionTaskScheduled"},
                {
                    "eventId": 3,
                    "eventType": "DecisionTaskStarted",
                    "decisionTaskStartedEventAttributes": {"identity": "spam-1234"},
                },
                {
                    "eventId": 4,
                    "eventType": "DecisionTaskCompleted",
                    "decisionTaskCompletedEventAttributes": {"startedEventId": 3},
                },
                {
                    "eventId": 5,
                    "eventType": event_type,
                    event_attr_key: {"cause": cause, "DecisionTaskCompletedEventId": 4},
                },
                {"eventId": 6, "eventType": "DecisionTaskScheduled"},
                {
                    "eventId": 7,
                    "eventType": "DecisionTaskStarted",
                    "decisionTaskStartedEventAttributes": {"identity": identity},
                },
            ],
        }
        instance = seddy_specs.DAGBuilder(workflow, task)

        # Run function
        if isinstance(exp, Exception):
            with pytest.raises(exp.__class__) as e:
                instance.build_decisions()
            assert str(e.value) == str(exp)
        else:
            instance.build_decisions()
            assert instance.decisions == exp


class TestWorkflow:
    """Test ``seddy._specs.DAGWorkflow``."""

    @pytest.fixture
    def task_specs(self):
        """Example DAG-type workflow tasks specifications."""
        return [
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
        ]

    @pytest.fixture
    def spec(self, task_specs):
        """Example DAG-type workflow specification."""
        return {
            "name": "foo",
            "version": "0.42",
            "description": "A DAGflow",
            "tasks": task_specs,
            "type": "dag",
        }

    @pytest.fixture
    def instance(self, task_specs):
        """DAG-type workflow specification instance."""
        return seddy_specs.DAGWorkflow("foo", "0.42", task_specs, "A DAGflow")

    def test_init(self, instance, task_specs):
        """Test DAG-type workflow specification initialisation."""
        assert instance.name == "foo"
        assert instance.version == "0.42"
        assert instance.description == "A DAGflow"
        assert instance.task_specs is task_specs
        assert instance.spec_type == "dag"
        assert instance.decisions_builder is seddy_specs.DAGBuilder
        assert not instance.dependants

    def test_from_spec(self, spec, task_specs):
        """Test construction from specification."""
        res = seddy_specs.DAGWorkflow.from_spec(spec)
        assert isinstance(res, seddy_specs.DAGWorkflow)
        assert res.name == "foo"
        assert res.version == "0.42"
        assert res.description == "A DAGflow"
        assert res.task_specs is task_specs

    def test_setup(self, instance):
        """Test DAG-type workflow specification pre-computation."""
        instance.setup()
        assert instance.dependants == {"foo": ["bar", "yay"], "bar": [], "yay": []}
