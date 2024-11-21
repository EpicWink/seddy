"""Test ``seddy._specs``."""

from unittest import mock

import pytest

from seddy import _specs as seddy_decisions


def test_workflow_map():
    """Test workflow class map."""
    assert seddy_decisions.WORKFLOW["dag"] == seddy_decisions.DAGWorkflow


class TestDecisionsBuilder:
    """Test ``seddy._specs.DecisionsBuilder``."""

    class DecisionsBuilder(seddy_decisions.DecisionsBuilder):
        def build_decisions(self):
            self.decisions = [{"decisionType": "spam"}]

    @pytest.fixture
    def workflow_mock(self):
        """Workflow specification mock."""
        return mock.Mock(spec=seddy_decisions.Workflow)

    @pytest.fixture
    def task(self):
        """Example decision task."""
        return {"taskToken": ""}

    @pytest.fixture
    def instance(self, workflow_mock, task):
        """Decisions builder instance."""
        return self.DecisionsBuilder(workflow_mock, task)

    def test_init(self, instance, workflow_mock, task):
        """Test decisions builder initialisation."""
        assert instance.workflow is workflow_mock
        assert instance.task is task
        assert not instance.decisions


class TestWorkflow:
    """Test ``seddy._specs.Workflow``."""

    class Workflow(seddy_decisions.Workflow):
        class DecisionsBuilder(seddy_decisions.DecisionsBuilder):
            def build_decisions(self):
                self.decisions = [{"decisionType": "spam"}]

        decisions_builder = DecisionsBuilder
        spec_type = "eggs"

    @pytest.fixture
    def spec(self):
        """Example workflow specification."""
        return {
            "name": "foo",
            "version": "0.42",
            "description": "Sunny-side up workflow.",
            "spec_type": "eggs",
            "registration": {
                "active": False,
                "task_timeout": "NONE",
                "execution_timeout": 60,
                "task_list": "spam",
                "task_priority": 2,
                "child_policy": "TERMINATE",
                "lambda_role": "arn:aws:iam::spam:role/eggs",
            },
        }

    @pytest.fixture
    def instance(self):
        """Workflow specification instance."""
        return self.Workflow("foo", "0.42", "Sunny-side up workflow.")

    def test_init(self, instance, spec):
        """Test workflow specification initialisation."""
        assert instance.name == "foo"
        assert instance.version == "0.42"
        assert instance.description == "Sunny-side up workflow."
        assert instance.spec_type == "eggs"
        assert instance.decisions_builder is self.Workflow.DecisionsBuilder

    def test_from_spec(self, spec):
        """Test workflow specification construction from specification."""
        res = self.Workflow.from_spec(spec)
        assert isinstance(res, self.Workflow)
        assert res.name == "foo"
        assert res.version == "0.42"
        assert res.description == "Sunny-side up workflow."
        assert res.registration.active is False
        assert res.registration.task_timeout == "NONE"
        assert res.registration.execution_timeout == 60
        assert res.registration.task_list == "spam"
        assert res.registration.task_priority == 2
        assert res.registration.child_policy == seddy_decisions.ChildPolicy.TERMINATE
        assert res.registration.lambda_role == "arn:aws:iam::spam:role/eggs"

    def test_setup(self, instance):
        """Test workflow specification pre-computation."""
        instance.setup()

    def test_make_decisions(self, instance):
        """Test workflow decision making."""
        task = {"taskToken": ""}
        res = instance.make_decisions(task)
        assert res == [{"decisionType": "spam"}]
