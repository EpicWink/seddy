"""Workflows specs serialisation and desieralisation."""

import json
import pathlib
import typing as t
import logging as lg
import urllib.parse

from . import Workflow

logger = lg.getLogger(__package__)


class WorkflowNotFound(LookupError):
    """Workflow not found."""


def _construct_workflows(
    workflow_specs: t.List[t.Dict[str, t.Any]],
) -> t.List[Workflow]:
    """Construct workflows from specification.

    Args:
        workflow_specs: workflows specifications

    Returns:
        workflow type specifications
    """

    from . import WORKFLOW

    workflows = []
    for workflow_spec in workflow_specs:
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


def _load_specs_from_uri(  # TODO: unit-test
    workflows_spec_uri: str
) -> t.List[t.Dict[str, t.Any]]:
    """Load workflows specifications.

    Supported URI types:

    * file:// (or none): local file path (absolute or relative)

    Args:
        workflows_spec_uri: workflows specifications file URI

    Returns:
        workflows specifications
    """

    logger.debug("Workflows specifications URI '%s'", workflows_spec_uri)
    url_parts = urllib.parse.urlparse(workflows_spec_uri)
    if not url_parts.scheme or url_parts.scheme == "file":
        if url_parts.scheme == "file":
            workflows_spec_uri = workflows_spec_uri[7:]
        workflows_spec = _load_specs(pathlib.Path(workflows_spec_uri))
        if not (1,) < tuple(map(int, workflows_spec["version"].split("."))) < (2,):
            raise ValueError(
                "Unknown workflows file version: %s" % workflows_spec["version"]
            )
        return workflows_spec["workflows"]
    else:
        raise ValueError(
            "Unknown URI scheme '%s': %s" % (url_parts.scheme, workflows_spec_uri)
        )


def load_workflows(workflows_spec_uri: str) -> t.List[Workflow]:
    """Load workflows specifications.

    Determines load method from the file suffix. Supported file types:

    * JSON
    * YAML

    Supported URI types:

    * file:// (or none): local file path (absolute or relative)

    Args:
        workflows_spec_uri: workflows specifications URI

    Returns:
        workflows specifications
    """

    workflows_specs = _load_specs_from_uri(workflows_spec_uri)
    return _construct_workflows(workflows_specs)


def get_workflow(
    name: str,
    version: str,
    workflows_spec_uri: str,
) -> Workflow:
    """Get workflow specification.

    Args:
        name: workflow name
        version: workflow version
        workflows_spec_uri: workflows specifications URI

    Returns:
        workflow specification

    Raises:
        WorkflowNotFound: if workflow with given name and version not found
    """

    workflows = load_workflows(workflows_spec_uri)
    try:
        return next(w for w in workflows if w.name == name and w.version == version)
    except StopIteration:
        raise WorkflowNotFound("name=%s, version=%s" % (name, version)) from None
