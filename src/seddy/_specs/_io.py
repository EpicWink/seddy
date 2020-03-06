"""Workflows specs serialisation and desieralisation."""

import json
import pathlib
import typing as t
import logging as lg

from . import Workflow

logger = lg.getLogger(__package__)


def _construct_workflows(workflows_spec: t.Dict[str, t.Any]) -> t.List[Workflow]:
    """Construct workflows from specification.

    Args:
        workflows_spec: workflows specifications

    Returns:
        workflow type specifications
    """

    from . import WORKFLOW

    assert (1,) < tuple(map(int, workflows_spec["version"].split("."))) < (2,)
    workflows = []
    for workflow_spec in workflows_spec["workflows"]:
        workflow_cls = WORKFLOW[workflow_spec["spec_type"]]
        workflow = workflow_cls.from_spec(workflow_spec)
        workflows.append(workflow)
    return workflows


def _load_specs(workflows_file: pathlib.Path) -> t.Dict[str, t.Any]:
    """Load workflows specifications file.

    Determines load method from the file suffix. Supported file types:

    * JSON
    * YAML

    Args:
        workflows_file: workflows specifications file path

    Returns:
        workflows specifications
    """

    logger.info("Loading workflows specifictions from '%s'", workflows_file)
    workflows_text = workflows_file.read_text()
    if workflows_file.suffix == ".json":
        return json.loads(workflows_text)
    elif workflows_file.suffix in (".yml", ".yaml"):
        try:
            import yaml
        except ImportError as e:
            try:
                from ruamel import yaml
            except ImportError as er:
                raise e from er
        return yaml.safe_load(workflows_text)
    raise ValueError("Unknown extension: %s" % workflows_file.suffix)


def load_workflows(workflows_file: pathlib.Path) -> t.List[Workflow]:
    """Load workflows specifications file.

    Determines load method from the file suffix. Supported file types:

    * JSON
    * YAML

    Args:
        workflows_file: workflows specifications file path

    Returns:
        workflows specifications
    """

    workflows_specs = _load_specs(workflows_file)
    return _construct_workflows(workflows_specs)
