"""SWF workflow management service."""

import pathlib
import argparse
import logging as lg

import pkg_resources

try:
    version = pkg_resources.get_distribution("seddy").version
except pkg_resources.DistributionNotFound:  # pragma: no cover
    version = None


def setup_logging(verbose: int):
    lg.addLevelName(25, "NOTICE")
    levels = {
        -2: lg.ERROR,
        -1: lg.WARNING,
        0: 25,
        1: lg.INFO,
        2: lg.DEBUG,
    }
    level = levels.get(verbose, lg.CRITICAL if verbose < 0 else lg.NOTSET)
    fmt = "%(asctime)s [%(levelname)8s] %(name)s: %(message)s"
    try:
        import coloredlogs
    except ImportError:
        lg.basicConfig(level=level, format=fmt)
    else:
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


def run_app(args: argparse.Namespace):
    setup_logging(args.verbose - args.quiet)
    if args.command == "decider":
        from . import decider

        decider.run_app(args.decider_json, args.domain, args.task_list)
    else:
        raise ValueError(args.command)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="increase logging verbosity"
    )
    parser.add_argument(
        "-q", "--quiet", action="count", default=0, help="decrease logging verbosity"
    )
    parser.add_argument("-V", "--version", action="version", version=version)
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")
    subparsers.required = True

    # Decider
    decider_parser = subparsers.add_parser(
        "decider", help="run SWF decider", description="Run SWF decider."
    )
    decider_parser.add_argument(
        "decider_json", type=pathlib.Path, help="decider specification JSON"
    )
    decider_parser.add_argument("domain", help="SWF domain")
    decider_parser.add_argument("task_list", help="SWF decider task-list")

    return parser


def main():  # pragma: no cover
    parser = build_parser()
    args = parser.parse_args()
    run_app(args)


if __name__ == "__main__":  # pragma: no cover
    main()
