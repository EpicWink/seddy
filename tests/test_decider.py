"""Test ``seddy.decider``."""

import os
import socket
from unittest import mock

from seddy import decider as seddy_decider
from seddy import decisions as seddy_decisions
import moto
import pytest
from botocore import client as botocore_client


def test_socket_timeout():
    assert socket.getdefaulttimeout() >= 70.0


class TestDecider:
    @pytest.fixture
    def workflow_mocks(self):
        workflows = [
            mock.Mock(spec=seddy_decisions.Workflow),
            mock.Mock(spec=seddy_decisions.Workflow),
            mock.Mock(spec=seddy_decisions.Workflow),
        ]
        workflows[0].spec = {"name": "spam", "version": "1.0"}
        workflows[1].spec = {"name": "bar", "version": "0.42"}
        workflows[2].spec = {"name": "spam", "version": "1.1"}
        return workflows

    @pytest.fixture
    def instance(self, workflow_mocks):
        env_update = {
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": "id",
            "AWS_SECRET_ACCESS_KEY": "key",
        }
        env_patch = mock.patch.dict(os.environ, env_update)
        with env_patch:
            yield seddy_decider.Decider(workflow_mocks, "spam", "eggs")

    def test_init(self, instance, workflow_mocks):
        assert instance.workflows == workflow_mocks
        assert instance.domain == "spam"
        assert instance.task_list == "eggs"
        assert isinstance(instance.client, botocore_client.BaseClient)
        assert instance.identity

    @moto.mock_swf
    def test_poll_for_decision_task(self, instance):
        # Setup environment
        instance.client.register_domain(
            name="spam", workflowExecutionRetentionPeriodInDays="2"
        )
        instance.client.register_workflow_type(
            domain="spam", name="bar", version="0.42"
        )
        resp = instance.client.start_workflow_execution(
            domain="spam",
            workflowId="1234",
            workflowType={"name": "bar", "version": "0.42"},
            executionStartToCloseTimeout="60",
            taskList={"name": "eggs"},
            taskStartToCloseTimeout="10",
            childPolicy="REQUEST_CANCEL",
        )

        # Run function
        res = instance._poll_for_decision_task()

        # Check result
        assert res == {
            "ResponseMetadata": mock.ANY,
            "events": [
                {
                    "eventId": 1,
                    "eventTimestamp": mock.ANY,
                    "eventType": "WorkflowExecutionStarted",
                    "workflowExecutionStartedEventAttributes": {
                        "workflowType": {"name": "bar", "version": "0.42"},
                        "executionStartToCloseTimeout": "60",
                        "taskList": {"name": "eggs"},
                        "taskStartToCloseTimeout": "10",
                        "childPolicy": "REQUEST_CANCEL",
                    },
                },
                {
                    "eventId": 2,
                    "eventTimestamp": mock.ANY,
                    "eventType": "DecisionTaskScheduled",
                    "decisionTaskScheduledEventAttributes": {
                        "startToCloseTimeout": "10",
                        "taskList": {"name": "eggs"},
                    },
                },
                {
                    "eventId": 3,
                    "eventTimestamp": mock.ANY,
                    "eventType": "DecisionTaskStarted",
                    "decisionTaskStartedEventAttributes": {
                        "identity": instance.identity,
                        "scheduledEventId": 2,
                    },
                },
            ],
            "previousStartedEventId": 0,
            "startedEventId": 3,
            "taskToken": mock.ANY,
            "workflowExecution": {"runId": resp["runId"], "workflowId": "1234"},
            "workflowType": {"name": "bar", "version": "0.42"},
        }

    def test_make_decisions(self, instance, workflow_mocks):
        # Build input
        task = {
            "ResponseMetadata": mock.ANY,
            "events": [
                {
                    "eventId": 1,
                    "eventTimestamp": mock.ANY,
                    "eventType": "WorkflowExecutionStarted",
                    "workflowExecutionStartedEventAttributes": {
                        "workflowType": {"name": "bar", "version": "0.42"},
                        "executionStartToCloseTimeout": "60",
                        "taskList": {"name": "eggs"},
                        "taskStartToCloseTimeout": "10",
                        "childPolicy": "REQUEST_CANCEL",
                    },
                },
                {
                    "eventId": 2,
                    "eventTimestamp": mock.ANY,
                    "eventType": "DecisionTaskScheduled",
                    "decisionTaskScheduledEventAttributes": {
                        "startToCloseTimeout": "10",
                        "taskList": {"name": "eggs"},
                    },
                },
                {
                    "eventId": 3,
                    "eventTimestamp": mock.ANY,
                    "eventType": "DecisionTaskStarted",
                    "decisionTaskStartedEventAttributes": {
                        "identity": instance.identity,
                        "scheduledEventId": 2,
                    },
                },
            ],
            "previousStartedEventId": 0,
            "startedEventId": 3,
            "taskToken": mock.ANY,
            "workflowExecution": {"runId": mock.ANY, "workflowId": "1234"},
            "workflowType": {"name": "bar", "version": "0.42"},
        }

        # Run function
        res = instance._make_decisions(task)

        # Check result
        assert res is workflow_mocks[1].make_decisions.return_value

    @moto.mock_swf
    def test_respond_decision_task_completed(self, instance):
        # Setup environment
        instance.client.register_domain(
            name="spam", workflowExecutionRetentionPeriodInDays="2"
        )
        instance.client.register_workflow_type(
            domain="spam", name="bar", version="0.42"
        )
        resp = instance.client.start_workflow_execution(
            domain="spam",
            workflowId="1234",
            workflowType={"name": "bar", "version": "0.42"},
            executionStartToCloseTimeout="60",
            taskList={"name": "eggs"},
            taskStartToCloseTimeout="10",
            childPolicy="REQUEST_CANCEL",
        )
        task = instance.client.poll_for_decision_task(
            domain="spam", identity=instance.identity, taskList={"name": "eggs"}
        )

        # Build input
        decisions = [{"decisionType": "CompleteWorkflowExecution"}]

        # Run function
        instance._respond_decision_task_completed(decisions, task)

        # Check result
        execution_info = instance.client.describe_workflow_execution(
            domain="spam", execution={"workflowId": "1234", "runId": resp["runId"]}
        )
        assert execution_info["executionInfo"]["executionStatus"] == "CLOSED"
        assert execution_info["executionInfo"]["closeStatus"] == "COMPLETED"

    def test_poll_and_run(self, workflow_mocks):
        # Setup environment
        task = {
            "taskToken": "spam",
            "workflowType": {"name": "bar", "version": "0.42"},
            "workflowExecution": {"workflowId": "1234", "runId": "9abc"},
        }

        class Decider(seddy_decider.Decider):
            _poll_for_decision_task = mock.Mock(return_value=task)
            _make_decisions = mock.Mock(
                return_value=[{"decisionType": "CompleteWorkflowExecution"}]
            )
            _respond_decision_task_completed = mock.Mock()

        instance = Decider(workflow_mocks, "spam", "eggs")

        # Run function
        instance._poll_and_run()

        # Check calls
        instance._poll_for_decision_task.assert_called_once_with()
        instance._make_decisions.assert_called_once_with(task)
        instance._respond_decision_task_completed.assert_called_once_with(
            [{"decisionType": "CompleteWorkflowExecution"}], task
        )

    def test_poll_and_run_no_result(self, workflow_mocks):
        # Setup environment
        class Decider(seddy_decider.Decider):
            _poll_for_decision_task = mock.Mock(return_value={"taskToken": ""})
            _make_decisions = mock.Mock()
            _respond_decision_task_completed = mock.Mock()

        instance = Decider(workflow_mocks, "spam", "eggs")

        # Run function
        instance._poll_and_run()

        # Check calls
        instance._poll_for_decision_task.assert_called_once_with()
        instance._make_decisions.assert_not_called()
        instance._respond_decision_task_completed.assert_not_called()

    def test_run_uncaught(self, workflow_mocks):
        # Setup environment
        class Decider(seddy_decider.Decider):
            _poll_and_run = mock.Mock(side_effect=[None, None, None, KeyboardInterrupt])

        instance = Decider(workflow_mocks, "spam", "eggs")

        # Run function
        with pytest.raises(KeyboardInterrupt):
            instance._run_uncaught()

        # Check calls
        assert instance._poll_and_run.call_args_list == [mock.call()] * 4

    def test_run(self, workflow_mocks):
        # Setup environment
        class Decider(seddy_decider.Decider):
            _run_uncaught = mock.Mock(side_effect=KeyboardInterrupt)

        instance = Decider(workflow_mocks, "spam", "eggs")

        # Run function
        instance.run()

        # Check calls
        instance._run_uncaught.assert_called_once_with()
