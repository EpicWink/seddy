"""SWF decider application."""

import json
import pathlib
import typing as t

from . import decider as seddy_decider
from . import decisions as seddy_decisions


def setup_workflows(  # TODO: unit-test
    decider_spec: t.Dict[str, t.Any]
) -> t.List[seddy_decisions.Workflow]:
    """Set-up decider workflows.

    Args:
        decider_spec: decider specification

    Returns:
        decider initialised workflows
    """

    assert (1,) < tuple(map(int, decider_spec["version"].split("."))) < (2,)
    workflows = []
    for workflow_spec in decider_spec["workflows"]:
        workflow = seddy_decisions.WORKFLOW[workflow_spec["spec_type"]](workflow_spec)
        workflow.setup()
        workflows.append(workflow)
    return workflows


def run_app(decider_spec_json: pathlib.Path, domain: str, task_list: str):  # TODO: unit-test
    """Run decider application.

    Arguments:
        decider_spec_json: decider specification JSON
        domain: SWF domain
        task_list: SWF decider task-list
    """

    decider_spec = json.loads(decider_spec_json.read_text())
    workflows = setup_workflows(decider_spec)
    decider = seddy_decider.Decider(workflows, domain, task_list)
    decider.run()
