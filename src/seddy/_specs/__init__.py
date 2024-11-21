"""SWF workflows specifications."""

__all__ = [
    "ChildPolicy",
    "Registration",
    "DecisionsBuilder",
    "Workflow",
    "make_decisions_on_error",
    "DAGBuilder",
    "DAGWorkflow",
    "WorkflowNotFound",
    "get_workflow",
    "load_workflows",
    "WORKFLOW",
]

from ._base import (
    ChildPolicy,
    DecisionsBuilder,
    Registration,
    Workflow,
    make_decisions_on_error,
)
from ._dag import DAGBuilder, DAGWorkflow
from ._io import WorkflowNotFound, get_workflow, load_workflows

WORKFLOW = {
    DAGWorkflow.spec_type: DAGWorkflow,
}
