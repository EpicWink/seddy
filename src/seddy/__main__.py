"""SWF workflow management service."""

import pathlib
import argparse

import pkg_resources

try:
    version = pkg_resources.get_distribution("seddy").version
except pkg_resources.DistributionNotFound:  # pragma: no cover
    version = None


def run_app(args: argparse.Namespace):
    """Run application from parsed command-line arguments."""
    from . import _util

    _util.setup_logging(args.verbose - args.quiet)

    if args.command == "decider":
        from . import decider

        decider.run_app(args.workflows_json, args.domain, args.task_list)
    elif args.command == "register":
        from . import registration

        registration.run_app(args.workflows_json, args.domain, args.skip_existing)
    else:  # pragma: no cover
        raise ValueError(args.command)


def build_parser() -> argparse.ArgumentParser:
    """Build command-line argument parser."""
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
        "workflows_json", type=pathlib.Path, help="workflows specifications JSON"
    )
    decider_parser.add_argument("domain", help="SWF domain")
    decider_parser.add_argument("task_list", help="SWF decider task-list")

    # Workflows registration
    register_parser = subparsers.add_parser(
        "register",
        help="register workflows with SWF",
        description="Register workflows with SWF.",
    )
    register_parser.add_argument(
        "workflows_json", type=pathlib.Path, help="workflows specifications JSON"
    )
    register_parser.add_argument("domain", help="SWF domain")
    register_parser.add_argument(
        "-s",
        "--skip-existing",
        action="store_true",
        help="check for and skip existing workflows with the same name and version",
    )

    return parser


def main():  # pragma: no cover
    parser = build_parser()
    args = parser.parse_args()
    run_app(args)


if __name__ == "__main__":  # pragma: no cover
    main()
