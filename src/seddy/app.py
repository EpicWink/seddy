"""SWF decider application."""

import json
import pathlib

from . import decider as seddy_decider


def run_app(decider_spec_json: pathlib.Path, domain: str, task_list: str):  # TODO: unit-test
    """Run decider application.

    Arguments:
        decider_spec_json: decider specification JSON
        domain: SWF domain
        task_list: SWF decider task-list
    """

    decider_spec = json.loads(decider_spec_json.read_text())
    decider = seddy_decider.Decider(decider_spec, domain, task_list)
    decider.run()
