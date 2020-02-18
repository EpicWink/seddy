"""SWF decisions building."""

__all__ = ["DecisionsBuilder", "Workflow", "DAGBuilder", "DAGWorkflow"]

from ._base import DecisionsBuilder
from ._base import Workflow
from ._dag import DAGBuilder
from ._dag import DAGWorkflow

WORKFLOW = {
    DAGWorkflow.spec_type: DAGWorkflow,
}
