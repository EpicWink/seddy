"""Test ``seddy.decisions``."""

from unittest import mock

from seddy import decisions as seddy_decisions
import pytest


def test_workflow_map():
    """Test workflow class map."""
    assert seddy_decisions.WORKFLOW["dag"] == seddy_decisions.DAG


class TestDecisionsBuilder:
    """Test ``seddy.decisions.DecisionsBuilder``."""

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
    """Test ``seddy.decisions.Workflow``."""

    class Workflow(seddy_decisions.Workflow):
        class DecisionsBuilder(seddy_decisions.DecisionsBuilder):
            def build_decisions(self):
                self.decisions = [{"decisionType": "spam"}]

        decisions_builder = DecisionsBuilder
        spec_type = "eggs"

    @pytest.fixture
    def spec(self):
        """Example workflow specification."""
        return {"name": "foo", "version": "0.42", "tasks": [], "type": "eggs"}

    @pytest.fixture
    def instance(self, spec):
        """Workflow specification instance."""
        return self.Workflow(spec)

    def test_init(self, instance, spec):
        """Test workflow specification initialisation."""
        assert instance.spec is spec
        assert instance.spec_type == "eggs"
        assert instance.decisions_builder is self.Workflow.DecisionsBuilder

    def test_setup(self, instance):
        """Test workflow specification pre-computation."""
        instance.setup()

    def test_make_decisions(self, instance):
        """Test workflow decision making."""
        task = {"taskToken": ""}
        res = instance.make_decisions(task)
        assert res == [{"decisionType": "spam"}]
