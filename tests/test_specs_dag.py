"""Test ``seddy._specs._dag``."""

import logging as lg

from seddy import _specs as seddy_specs
import pytest

lg.root.setLevel(lg.DEBUG)


class TestDAGDecisionsBuilding:
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

    def test_workflow_start(self, workflow):
        """Test DAG decisions building on workflow start."""
        task = {
            "taskToken": "spam",
            "previousStartedEventId": 0,
            "startedEventId": 3,
            "events": [
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
            ],
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
        instance = seddy_specs.DAGBuilder(workflow, task)
        instance.build_decisions()
        assert instance.decisions == expected_decisions

    def test_other_event(self, workflow):
        """Test DAG decisions building after other event."""
        task = {
            "taskToken": "spam",
            "previousStartedEventId": 3,
            "startedEventId": 7,
            "events": [
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
                {"eventId": 4, "eventType": "DecisionTaskCompleted"},
                {
                    "eventId": 5,
                    "eventType": "WorkflowExecutionSignaled",
                    "workflowExecutionSignaledEventAttributes": {"signalName": "blue"},
                },
                {"eventId": 6, "eventType": "DecisionTaskScheduled"},
                {"eventId": 7, "eventType": "DecisionTaskStarted"},
            ]
        }
        instance = seddy_specs.DAGBuilder(workflow, task)
        instance.build_decisions()
        assert instance.decisions == []

    def test_foo_complete(self, workflow):
        """Test DAG decisions building after foo activity completes."""
        task = {
            "taskToken": "spam",
            "previousStartedEventId": 3,
            "startedEventId": 9,
            "events": [
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
        instance = seddy_specs.DAGBuilder(workflow, task)
        instance.build_decisions()
        assert instance.decisions == expected_decisions

    def test_foo_complete_yay_unsatisfied(self, workflow):
        """Test DAG decisions building after foo completes yet yay not ready."""
        workflow.dependants["bar"] = ["yay"]
        assert workflow.task_specs[2]["id"] == "yay"
        workflow.task_specs[2]["dependencies"] = ["foo", "bar"]
        task = {
            "taskToken": "spam",
            "previousStartedEventId": 3,
            "startedEventId": 9,
            "events": [
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
            ],
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
        instance = seddy_specs.DAGBuilder(workflow, task)
        instance.build_decisions()
        assert instance.decisions == expected_decisions

    def test_yay_complete(self, workflow):
        """Test DAG decisions building after yay activity completes."""
        task = {
            "taskToken": "spam",
            "previousStartedEventId": 9,
            "startedEventId": 17,
            "events": [
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
            ],
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
        instance = seddy_specs.DAGBuilder(workflow, task)
        instance.build_decisions()
        assert instance.decisions == expected_decisions

    def test_bar_complete(self, workflow):
        """Test DAG decisions building after bar activity completes."""
        task = {
            "taskToken": "spam",
            "previousStartedEventId": 17,
            "startedEventId": 23,
            "events": [
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
            ],
        }
        instance = seddy_specs.DAGBuilder(workflow, task)
        instance.build_decisions()
        assert instance.decisions == []

    def test_tin_complete(self, workflow):
        """Test DAG decisions building after tin activity completes."""
        task = {
            "taskToken": "spam",
            "previousStartedEventId": 23,
            "startedEventId": 27,
            "events": [
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
                {"eventId": 24, "eventType": "DecisionTaskCompleted"},
                {
                    "eventId": 25,
                    "eventType": "ActivityTaskCompleted",
                    "activityTaskCompletedEventAttributes": {"scheduledEventId": 19},
                },
                {"eventId": 26, "eventType": "DecisionTaskScheduled"},
                {"eventId": 27, "eventType": "DecisionTaskStarted"},
            ],
        }
        expected_decisions = [
            {
                "decisionType": "CompleteWorkflowExecution",
                "completeWorkflowExecutionDecisionAttributes": {
                    "result": '{"foo": 3, "bar": {"a": 9, "b": "red"}, "yay": 5}'
                },
            }
        ]
        instance = seddy_specs.DAGBuilder(workflow, task)
        instance.build_decisions()
        assert instance.decisions == expected_decisions

    def test_bar_and_yay_complete(self, workflow):
        """Test DAG decisions building after bar and yay activities complete."""
        task = {
            "taskToken": "spam",
            "previousStartedEventId": 9,
            "startedEventId": 18,
            "events": [
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
            ],
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
        instance = seddy_specs.DAGBuilder(workflow, task)
        instance.build_decisions()
        assert instance.decisions == expected_decisions

    def test_foo_failed(self, workflow):
        """Test DAG decisions building after foo activity fails."""
        task = {
            "taskToken": "spam",
            "previousStartedEventId": 3,
            "startedEventId": 9,
            "events": [
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
            ],
        }
        expected_decisions = [
            {
                "decisionType": "FailWorkflowExecution",
                "failWorkflowExecutionDecisionAttributes": {
                    "reason": "activityFailure",
                },
            }
        ]
        instance = seddy_specs.DAGBuilder(workflow, task)
        instance.build_decisions()
        assert instance.decisions == expected_decisions

    def test_foo_timed_out(self, workflow):
        """Test DAG decisions building after foo activity times-out."""
        # Events sections
        task = {
            "taskToken": "spam",
            "previousStartedEventId": 3,
            "startedEventId": 9,
            "events": [
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
            ],
        }
        expected_decisions = [
            {
                "decisionType": "FailWorkflowExecution",
                "failWorkflowExecutionDecisionAttributes": {
                    "reason": "activityFailure",
                },
            }
        ]
        instance = seddy_specs.DAGBuilder(workflow, task)
        instance.build_decisions()
        assert instance.decisions == expected_decisions

    def test_foo_start_timed_out(self, workflow):
        """Test DAG decisions building after foo activity start times-out."""
        # Events sections
        task = {
            "taskToken": "spam",
            "previousStartedEventId": 3,
            "startedEventId": 9,
            "events": [
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
                        "timeoutType": "SCHEDULE_TO_START",
                    },
                },
                {"eventId": 8, "eventType": "DecisionTaskScheduled"},
                {"eventId": 9, "eventType": "DecisionTaskStarted"},
            ],
        }
        expected_decisions = [
            {
                "decisionType": "FailWorkflowExecution",
                "failWorkflowExecutionDecisionAttributes": {"reason": "timeOut"},
            }
        ]
        instance = seddy_specs.DAGBuilder(workflow, task)
        instance.build_decisions()
        assert instance.decisions == expected_decisions

    def test_workflow_timed_out(self, workflow):
        """Test DAG decisions building after foo activity start times-out."""
        # Events sections
        task = {
            "taskToken": "spam",
            "previousStartedEventId": 3,
            "startedEventId": 9,
            "events": [
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
                    "eventType": "WorkflowExecutionTimedOut",
                    "workflowExecutionTimedOutTimedOutEventAttributes": {
                        "childPolicy": "ABANDON",
                        "timeoutType": "START_TO_CLOSE",
                    },
                },
                {"eventId": 8, "eventType": "DecisionTaskScheduled"},
                {"eventId": 9, "eventType": "DecisionTaskStarted"},
            ],
        }
        expected_decisions = [
            {
                "decisionType": "FailWorkflowExecution",
                "failWorkflowExecutionDecisionAttributes": {"reason": "timeOut"},
            }
        ]
        instance = seddy_specs.DAGBuilder(workflow, task)
        instance.build_decisions()
        assert instance.decisions == expected_decisions

    def test_workflow_cancel(self, workflow):
        """Test DAG decisions building after workflow is cancelled."""
        task = {
            "taskToken": "spam",
            "previousStartedEventId": 3,
            "startedEventId": 9,
            "events": [
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
            ],
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
        instance = seddy_specs.DAGBuilder(workflow, task)
        instance.build_decisions()
        assert instance.decisions == expected_decisions

    def test_workflow_cancel_after_bar_yay(self, workflow):
        """Test DAG decisions building: workflow cancelled after bar and yay."""
        task = {
            "taskToken": "spam",
            "previousStartedEventId": 9,
            "startedEventId": 16,
            "events": [
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
            ],
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
        instance = seddy_specs.DAGBuilder(workflow, task)
        instance.build_decisions()
        assert instance.decisions == expected_decisions

    @pytest.mark.parametrize(
        ("cause", "identity", "event_type", "exp"),
        [
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
        instance.build_decisions()
        assert instance.decisions == exp

    @pytest.mark.parametrize(
        ("cause", "exception_str"),
        [
            pytest.param("OPERATION_NOT_PERMITTED", "Not permitted", id="permissions"),
            pytest.param("INVALID_INPUT", "", id="error"),
        ],
    )
    def test_decision_failure_raises(self, workflow, cause, exception_str):
        """Test decision failure handling raises on configuration issue."""
        # Build input
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
                    "eventType": "CancelWorkflowExecutionFailed",
                    "cancelWorkflowExecutionFailedEventAttributes": {
                        "cause": cause,
                        "DecisionTaskCompletedEventId": 4,
                    },
                },
                {"eventId": 6, "eventType": "DecisionTaskScheduled"},
                {
                    "eventId": 7,
                    "eventType": "DecisionTaskStarted",
                    "decisionTaskStartedEventAttributes": {"identity": "spam-1234"},
                },
            ],
        }
        instance = seddy_specs.DAGBuilder(workflow, task)

        # Run function
        with pytest.raises(seddy_specs._base.DeciderError) as e:
            instance.build_decisions()
        assert str(e.value) == exception_str

    def test_workflow_complete_decision_failure(self):
        """Test workflow-complete decision failure handling."""
        # Build input
        workflow = seddy_specs.DAGWorkflow.from_spec(
            {
                "name": "foo",
                "version": "0.44",
                "tasks": [],
                "type": "dag",
            }
        )
        workflow.setup()
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
                    "eventType": "CompleteWorkflowExecutionFailed",
                    "completeWorkflowExecutionFailedEventAttributes": {
                        "cause": "UNHANDLED_DECISION",
                        "DecisionTaskCompletedEventId": 4,
                    },
                },
                {"eventId": 6, "eventType": "DecisionTaskScheduled"},
                {
                    "eventId": 7,
                    "eventType": "DecisionTaskStarted",
                    "decisionTaskStartedEventAttributes": {"identity": "spam-1234"},
                },
            ],
        }
        instance = seddy_specs.DAGBuilder(workflow, task)

        # Run function
        instance.build_decisions()
        assert instance.decisions == [{"decisionType": "CompleteWorkflowExecution"}]


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
