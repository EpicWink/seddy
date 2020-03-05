"""SWF decider service utilities."""

import os
import sys
import typing as t
import logging as lg

import boto3

logger = lg.getLogger(__package__)
AWS_SWF_ENDPOINT_URL = os.environ.get("AWS_SWF_ENDPOINT_URL")
LOGGING_LEVELS = {
    -2: lg.ERROR,
    -1: lg.WARNING,
    0: 25,
    1: lg.INFO,
    2: lg.DEBUG,
}


def setup_logging(verbose: int, json_logging: bool = False):
    """Setup logging.

    Args:
        verbose: logging verbosity
        json_logging: JSON-format logs
    """

    lg.addLevelName(25, "NOTICE")
    level = LOGGING_LEVELS.get(verbose, lg.CRITICAL if verbose < 0 else lg.NOTSET)
    fmt = "%(asctime)s [%(levelname)8s] %(name)s: %(message)s"

    if json_logging:
        from pythonjsonlogger import jsonlogger

        handler = lg.StreamHandler()
        formatter = jsonlogger.JsonFormatter(
            "%(levelname)s %(name)s %(message)s", timestamp=True
        )
        handler.setFormatter(formatter)
        lg.basicConfig(level=level, handlers=[handler])
        return

    if level > lg.DEBUG:
        sys.tracebacklimit = 0

    try:
        import coloredlogs
    except ImportError:
        lg.basicConfig(level=level, format=fmt)
        return

    field_styles = {
        "asctime": {"faint": True, "color": "white"},
        "levelname": {"bold": True, "color": "blue"},
        "name": {"bold": True, "color": "yellow"},
    }
    level_styles = {
        **coloredlogs.DEFAULT_LEVEL_STYLES,
        "notice": {},
        "info": {"color": "white"},
    }
    coloredlogs.install(
        level=level,
        fmt=fmt,
        field_styles=field_styles,
        level_styles=level_styles,
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


def get_swf_client():
    """Create an SWF client.

    Uses ``AWS_SWF_ENDPOINT_URL`` from environment for the endpoint URL.

    Returns:
        botocore.client.BaseClient: SWF client
    """

    logger.debug(
        "Creating SWF client with endpoint URL: %s", AWS_SWF_ENDPOINT_URL or "<default>"
    )
    return boto3.client("swf", endpoint_url=AWS_SWF_ENDPOINT_URL)
