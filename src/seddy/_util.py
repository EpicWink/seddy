"""SWF decider service utilities."""

import typing as t
import logging as lg

from . import decisions as seddy_decisions

LOGGING_LEVELS = {
    -2: lg.ERROR,
    -1: lg.WARNING,
    0: 25,
    1: lg.INFO,
    2: lg.DEBUG,
}


def setup_logging(verbose: int):
    """Setup logging.

    Args:
        verbose: logging verbosity
    """

    lg.addLevelName(25, "NOTICE")
    level = LOGGING_LEVELS.get(verbose, lg.CRITICAL if verbose < 0 else lg.NOTSET)
    fmt = "%(asctime)s [%(levelname)8s] %(name)s: %(message)s"

    try:
        import coloredlogs
    except ImportError:
        lg.basicConfig(level=level, format=fmt)
        return

    coloredlogs.install(
        level=level,
        fmt=fmt,
        field_styles={
            "asctime": {"faint": True, "color": "white"},
            "levelname": {"bold": True, "color": "blue"},
            "name": {"bold": True, "color": "yellow"},
        },
        level_styles={
            **coloredlogs.DEFAULT_LEVEL_STYLES,
            "notice": {},
            "info": {"color": "white"},
        },
        milliseconds=True,
    )
    lg.root.setLevel(level)


def list_paginated(
    fn: t.Callable[..., t.Dict[str, t.Any]],
    list_key: str,
    kwargs: t.Dict[str, t.Any] = None,
    next_key: str = "nextPageToken",
    next_arg: str = None,
) -> t.Dict[str, t.Any]:
    """List AWS resources, consuming pagination.

    Args:
        fn: resource listing function
        list_key: key of paginated list in response
        kwargs: keyword arguments to ``fn``
        next_key: key of next-page token in response
        next_arg: argument name of next-page token in ``fn``, default: same
            as ``next_key``

    Returns:
        collected response of ``fn``
    """

    next_arg = next_key if next_arg is None else next_arg
    kwargs = kwargs or {}
    resp = fn(**kwargs)
    if resp.get(next_key):
        kwargs = kwargs.copy()
        kwargs[next_arg] = resp.pop(next_key)
        new_resp = list_paginated(fn, list_key, kwargs, next_key, next_arg)
        resp[list_key].extend(new_resp[list_key])
    return resp


def construct_workflows(
    workflows_spec: t.Dict[str, t.Any]
) -> t.List[seddy_decisions.Workflow]:
    """Construct workflows from specification.

    Args:
        workflows_spec: workflows specifications

    Returns:
        workflow type specifications
    """

    assert (1,) < tuple(map(int, workflows_spec["version"].split("."))) < (2,)
    workflows = []
    for workflow_spec in workflows_spec["workflows"]:
        workflow_cls = seddy_decisions.WORKFLOW[workflow_spec["spec_type"]]
        workflow = workflow_cls.from_spec(workflow_spec)
        if "registration_defaults" in workflow_spec:
            workflow.registration_defaults = workflow_spec["registration_defaults"]
        workflows.append(workflow)
    return workflows


def setup_workflows(workflows: t.List[seddy_decisions.Workflow]):
    """Set-up decider workflows.

    Args:
        workflows: workflow type specifications

    Returns:
        decider initialised workflows
    """

    for workflow in workflows:
        workflow.setup()
