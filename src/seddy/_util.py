"""SWF decider service utilities."""

import typing as t

from . import decisions as seddy_decisions


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
        new_resp = list_paginated(
            fn, list_key, kwargs, next_key, next_arg,
        )
        resp[list_key].extend(new_resp[list_key])
    return resp


def setup_workflows(
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
        workflow_cls = seddy_decisions.WORKFLOW[workflow_spec["spec_type"]]
        workflow = workflow_cls.from_spec(workflow_spec)
        workflow.setup()
        workflows.append(workflow)
    return workflows
