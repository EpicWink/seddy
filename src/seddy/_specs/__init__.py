"""SWF workflows specifications."""

__all__ = [
    "ChildPolicy",
    "Registration",
    "DecisionsBuilder",
    "Workflow",
    "DAGBuilder",
    "DAGWorkflow",
    "load_workflows",
    "setup_workflows",
    "WORKFLOW",
]

from ._base import ChildPolicy
from ._base import Registration
from ._base import DecisionsBuilder
from ._base import Workflow
from ._dag import DAGBuilder
from ._dag import DAGWorkflow
from ._io import load_workflows
from ._io import setup_workflows

WORKFLOW = {
    DAGWorkflow.spec_type: DAGWorkflow,
}
