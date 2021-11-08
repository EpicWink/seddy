"""SWF decider service utilities."""

import os
import sys
import logging as lg

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


def get_swf_client():
    """Create an SWF client.

    Uses ``AWS_SWF_ENDPOINT_URL`` from environment for the endpoint URL.

    Returns:
        botocore.client.BaseClient: SWF client
    """

    import boto3

    logger.debug(
        "Creating SWF client with endpoint URL: %s", AWS_SWF_ENDPOINT_URL or "<default>"
    )
    return boto3.client("swf", endpoint_url=AWS_SWF_ENDPOINT_URL)
