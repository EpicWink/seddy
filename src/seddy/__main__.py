"""SWF decider application."""

import pathlib
import argparse


def _run_app(args: argparse.Namespace):  # pragma: no cover
    from . import app
    app.run_app(args.decider_json, args.domain, args.task_list)


def main():  # pragma: no cover
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "decider_json", type=pathlib.Path, help="decider specification JSON"
    )
    parser.add_argument("domain", help="SWF domain")
    parser.add_argument("task_list", help="SWF decider task-list")
    args = parser.parse_args()
    _run_app(args)


if __name__ == "__main__":  # pragma: no cover
    main()
