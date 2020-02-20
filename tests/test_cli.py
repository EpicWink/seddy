"""Test ``seddy`` command-line application."""

import sys
import logging as lg
from unittest import mock

from seddy import __main__ as seddy_main
from seddy import decider as seddy_decider
from seddy import registration as seddy_registration
import pytest
import coloredlogs
import pkg_resources


@pytest.fixture
def decider_mock():
    """Decider application mock."""
    run_app_mock = mock.Mock()
    with mock.patch.object(seddy_decider, "run_app", run_app_mock):
        yield run_app_mock


@pytest.mark.parametrize(
    ("verbosity_flags", "exp_logging_level"),
    [
        pytest.param([], 25, id='""'),
        pytest.param(["-v"], lg.INFO, id='"-v"'),
        pytest.param(["-vv"], lg.DEBUG, id='"-vv"'),
        pytest.param(["-vvv"], lg.NOTSET, id='"-vvv"'),
        pytest.param(["-vvvv"], lg.NOTSET, id='"-vvvv"'),
        pytest.param(["-q"], lg.WARNING, id='"-q"'),
        pytest.param(["-qq"], lg.ERROR, id='"-qq"'),
        pytest.param(["-qqq"], lg.CRITICAL, id='"-qqq"'),
        pytest.param(["-qqqq"], lg.CRITICAL, id='"-qqqq"'),
        pytest.param(["-vq"], 25, id='"-vq"'),
        pytest.param(["-vvqq"], 25, id='"-vvqq"'),
        pytest.param(["-vvq"], lg.INFO, id='"-vvq"'),
        pytest.param(["-vqq"], lg.WARNING, id='"-vqq"'),
        pytest.param(["-v", "-v"], lg.DEBUG, id='"-v -v"'),
        pytest.param(["-v", "-q"], 25, id='"-v -q"'),
        pytest.param(["-q", "-q"], lg.ERROR, id='"-q -q"'),
    ],
)
@pytest.mark.parametrize(
    "coloredlogs_module",
    [pytest.param(None, id="logging"), pytest.param(coloredlogs, id="coloredlogs")],
)
def test_logging(
    decider_mock,
    tmp_path,
    verbosity_flags,
    exp_logging_level,
    coloredlogs_module,
    capsys,
):
    """Ensure logging configuration is set up correctly."""
    # Setup environment
    coloredlogs_patch = mock.patch.dict(
        sys.modules, {"coloredlogs": coloredlogs_module}
    )

    root_logger = lg.RootLogger("WARNING")
    root_logger_patch = mock.patch.object(lg, "root", root_logger)

    # Run function
    parser = seddy_main.build_parser()
    args = parser.parse_args(
        verbosity_flags + ["decider", str(tmp_path / "workflows.json"), "spam", "eggs"]
    )
    with root_logger_patch, coloredlogs_patch:
        seddy_main.run_app(args)

    # Check logging configuration
    assert root_logger.level == exp_logging_level

    root_logger.critical("spam")
    assert capsys.readouterr().err[24:] == "[CRITICAL] root: spam\n"


@pytest.mark.parametrize(
    ("command_line_args", "description"),
    [
        pytest.param(["-h"], "SWF workflow management service.", id='"-h"'),
        pytest.param(["--help"], "SWF workflow management service.", id='"--help"'),
        pytest.param(["decider", "-h"], "Run SWF decider.", id='"decider -h"'),
        pytest.param(
            ["decider", "a.json", "spam", "eggs", "-h"],
            "Run SWF decider.",
            id='"decider a.json spam eggs -h"',
        ),
        pytest.param(
            ["register", "-h"], "Register workflows with SWF.", id='"register -h"'
        ),
    ],
)
def test_usage(decider_mock, command_line_args, capsys, description):
    """Ensure usage is displayed."""
    # Run function
    parser = seddy_main.build_parser()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(command_line_args)
    assert e.value.code == 0

    # Check output
    res_out = capsys.readouterr().out
    assert res_out[:6] == "usage:"
    assert res_out.splitlines()[2] == description


@pytest.mark.parametrize(
    "command_line_args",
    [
        pytest.param(["-V"], id='"-V"'),
        pytest.param(["--version"], id='"--version"'),
        pytest.param(
            ["-V", "decider", "a.json", "spam", "eggs"],
            id='"-V decider a.json spam eggs"',
        ),
    ],
)
def test_version(decider_mock, command_line_args, capsys):
    """Ensure version is displayed."""
    # Run function
    parser = seddy_main.build_parser()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(command_line_args)
    assert e.value.code == 0

    # Check output
    res_out = capsys.readouterr().out
    assert res_out.strip() == pkg_resources.get_distribution("seddy").version


def test_decider(decider_mock, tmp_path):
    """Ensure decider application is run with the correct input."""
    # Run function
    parser = seddy_main.build_parser()
    args = parser.parse_args(
        ["decider", str(tmp_path / "workflows.json"), "spam", "eggs"]
    )
    seddy_main.run_app(args)

    # Check application input
    decider_mock.assert_called_once_with(tmp_path / "workflows.json", "spam", "eggs")


def test_register(tmp_path):
    """Ensure workflow registration application is run correctly."""
    # Setup environment
    run_app_mock = mock.Mock()
    run_app_patch = mock.patch.object(seddy_registration, "run_app", run_app_mock)

    # Run function
    parser = seddy_main.build_parser()
    args = parser.parse_args(
        ["register", str(tmp_path / "workflows.json"), "spam", "-s"]
    )
    with run_app_patch:
        seddy_main.run_app(args)

    # Check application input
    run_app_mock.assert_called_once_with(tmp_path / "workflows.json", "spam", True)
