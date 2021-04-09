"""Test ``seddy.decider``."""

import os
import socket
from unittest import mock
from concurrent import futures as cf

from seddy import decider as seddy_decider
from seddy import _specs as seddy_specs
import moto
import pytest
from botocore import client as botocore_client


def test_socket_timeout():
    assert socket.getdefaulttimeout() >= 70.0


class TestDecider:
    @pytest.fixture
    def workflow_mocks(self):
        workflows = [
            mock.Mock(spec=seddy_specs.Workflow),
            mock.Mock(spec=seddy_specs.Workflow),
            mock.Mock(spec=seddy_specs.Workflow),
        ]
        workflows[0].name = "spam"
        workflows[0].version = "1.0"
        workflows[1].name = "bar"
        workflows[1].version = "0.42"
        workflows[2].name = "spam"
        workflows[2].version = "1.1"
        return workflows

    @pytest.fixture
    def aws_environment(self):
        env_update = {
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": "id",
            "AWS_SECRET_ACCESS_KEY": "key",
        }
        with mock.patch.dict(os.environ, env_update):
            yield env_update

    @pytest.fixture
    def workflows_spec_file(self, tmp_path):
        return tmp_path / "workflows.json"

    @pytest.fixture
    def instance(self, workflows_spec_file, aws_environment):
        return seddy_decider.Decider(workflows_spec_file, "spam", "eggs", "abcd1234")

    def test_init(self, instance, workflows_spec_file):
        assert instance.workflows_spec_file == workflows_spec_file
        assert instance.domain == "spam"
        assert instance.task_list == "eggs"
        assert isinstance(instance.client, botocore_client.BaseClient)
        assert instance.identity == "abcd1234"

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
            "startedEventId": 3,
            "taskToken": mock.ANY,
            "workflowExecution": {"runId": resp["runId"], "workflowId": "1234"},
            "workflowType": {"name": "bar", "version": "0.42"},
        }

    def test_get_workflow(self, instance, workflow_mocks):
        # Setup environment
        load_mock = mock.Mock(return_value=workflow_mocks)
        load_patch = mock.patch.object(seddy_specs, "load_workflows", load_mock)

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
        with load_patch:
            res = instance._get_workflow(task)

        # Check result
        assert res is workflow_mocks[1]

    def test_get_workflow_unsupported(self, instance, workflow_mocks):
        """Check workflow-get raises for unsupported workflows."""
        # Setup environment
        load_mock = mock.Mock(return_value=workflow_mocks)
        load_patch = mock.patch.object(seddy_specs, "load_workflows", load_mock)

        # Build input
        task = {
            "ResponseMetadata": mock.ANY,
            "taskToken": mock.ANY,
            "workflowExecution": {"runId": mock.ANY, "workflowId": "1234"},
            "workflowType": {"name": "bar", "version": "0.43"},
        }

        # Run function
        with pytest.raises(seddy_decider.UnsupportedWorkflow) as e:
            with load_patch:
                instance._get_workflow(task)

        # Check result
        assert e.value.args[0] == {"name": "bar", "version": "0.43"}

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

    def test_poll_and_run(self, workflow_mocks, aws_environment):
        # Setup environment
        task = {
            "taskToken": "spam",
            "workflowType": {"name": "bar", "version": "0.42"},
            "workflowExecution": {"workflowId": "1234", "runId": "9abc"},
        }

        class Decider(seddy_decider.Decider):
            _poll_for_decision_task = mock.Mock(return_value=task)
            _get_workflow = mock.Mock(return_value=workflow_mocks[1])
            _respond_decision_task_completed = mock.Mock()

        workflow_mocks[1].make_decisions.return_value = [
            {"decisionType": "CompleteWorkflowExecution"}
        ]

        instance = Decider(workflow_mocks, "spam", "eggs")

        # Run function
        instance._poll_and_run()

        # Check calls
        instance._poll_for_decision_task.assert_called_once_with()
        instance._get_workflow.assert_called_once_with(task)
        instance._respond_decision_task_completed.assert_called_once_with(
            [{"decisionType": "CompleteWorkflowExecution"}], task
        )

    def test_poll_and_run_no_result(self, workflow_mocks, aws_environment):
        # Setup environment
        class Decider(seddy_decider.Decider):
            _poll_for_decision_task = mock.Mock(return_value={"taskToken": ""})
            _get_workflow = mock.Mock()
            _respond_decision_task_completed = mock.Mock()

        instance = Decider(workflow_mocks, "spam", "eggs")

        # Run function
        instance._poll_and_run()

        # Check calls
        instance._poll_for_decision_task.assert_called_once_with()
        instance._get_workflow.assert_not_called()
        instance._respond_decision_task_completed.assert_not_called()

    def test_poll_and_run_unsupported(self, workflow_mocks, aws_environment):
        """Task for workflow not in specifications file."""
        # Setup environment
        task = {
            "taskToken": "spam",
            "workflowType": {"name": "bar", "version": "0.43"},
            "workflowExecution": {"workflowId": "1234", "runId": "9abc"},
        }

        class Decider(seddy_decider.Decider):
            _poll_for_decision_task = mock.Mock(return_value=task)
            _get_workflow = mock.Mock(
                side_effect=seddy_decider.UnsupportedWorkflow(task["workflowType"])
            )
            _respond_decision_task_completed = mock.Mock()

        instance = Decider(workflow_mocks, "spam", "eggs")

        # Run function
        with pytest.raises(seddy_decider.UnsupportedWorkflow) as e:
            instance._poll_and_run()
        assert e.value.args[0] == task["workflowType"]

        # Check calls
        instance._poll_for_decision_task.assert_called_once_with()
        instance._get_workflow.assert_called_once_with(task)
        instance._respond_decision_task_completed.assert_not_called()

    def test_poll_and_run_decider_error(self, workflow_mocks, aws_environment):
        """Decision-building raises."""
        # Setup environment
        task = {
            "taskToken": "spam",
            "workflowType": {"name": "bar", "version": "0.42"},
            "workflowExecution": {"workflowId": "1234", "runId": "9abc"},
        }

        class Decider(seddy_decider.Decider):
            _poll_for_decision_task = mock.Mock(return_value=task)
            _get_workflow = mock.Mock(return_value=workflow_mocks[1])
            _respond_decision_task_completed = mock.Mock()

        workflow_mocks[1].make_decisions.side_effect = RuntimeError("malformed specs")

        instance = Decider(workflow_mocks, "spam", "eggs")

        # Build expectation
        exp_decision = {
            "decisionType": "FailWorkflowExecution",
            "failWorkflowExecutionDecisionAttributes": {
                "reason": "RuntimeError",
                "details": "malformed specs",
            },
        }

        # Run function
        with pytest.raises(RuntimeError) as e:
            instance._poll_and_run()
        assert str(e.value) == "malformed specs"

        # Check calls
        instance._poll_for_decision_task.assert_called_once_with()
        instance._get_workflow.assert_called_once_with(task)
        instance._respond_decision_task_completed.assert_called_once_with(
            [exp_decision], task
        )

    def test_run_uncaught(self, workflow_mocks, aws_environment):
        # Setup environment
        class Decider(seddy_decider.Decider):
            _poll_and_run = mock.Mock(side_effect=[None, None, None, KeyboardInterrupt])

        instance = Decider(workflow_mocks, "spam", "eggs")

        # Run function
        with pytest.raises(KeyboardInterrupt):
            instance._run_uncaught()

        # Check calls
        assert instance._poll_and_run.call_args_list == [mock.call()] * 4

    def test_run(self, workflow_mocks, aws_environment):
        # Setup environment
        class Decider(seddy_decider.Decider):
            _run_uncaught = mock.Mock(side_effect=KeyboardInterrupt)

        instance = Decider(workflow_mocks, "spam", "eggs")
        instance._future = mock.Mock(spec=cf.Future)
        instance._future.running.return_value = False

        # Run function
        instance.run()

        # Check calls
        instance._run_uncaught.assert_called_once_with()
        instance._future.result.assert_not_called()

    def test_run_handling_decision(self, workflow_mocks, aws_environment):
        # Setup environment
        class Decider(seddy_decider.Decider):
            _run_uncaught = mock.Mock(side_effect=KeyboardInterrupt)

        instance = Decider(workflow_mocks, "spam", "eggs")
        instance._future = mock.Mock(spec=cf.Future)
        instance._future.running.return_value = True

        # Run function
        instance.run()

        # Check calls
        instance._run_uncaught.assert_called_once_with()
        instance._future.result.assert_called_once_with()


def test_run_app(tmp_path):
    """Ensure decider is run with the correct configuration."""
    # Setup environment
    decider_mock = mock.Mock(spec=seddy_decider.Decider)
    decider_class_mock = mock.Mock(return_value=decider_mock)
    decider_class_patch = mock.patch.object(
        seddy_decider, "Decider", decider_class_mock
    )

    # Build input
    workflows_spec_json = tmp_path / "workflows.json"

    # Run function
    with decider_class_patch:
        seddy_decider.run_app(workflows_spec_json, "spam", "eggs", "abcd1234")

    # Check decider configuration
    decider_class_mock.assert_called_once_with(
        workflows_spec_json, "spam", "eggs", "abcd1234"
    )
    decider_class_mock.return_value.run.assert_called_once_with()
