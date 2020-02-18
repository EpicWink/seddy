# seddy
[![Build status](
https://github.com/EpicWink/seddy/workflows/test/badge.svg?branch=master)](
https://github.com/EpicWink/seddy/actions?query=branch%3Amaster+workflow%3Atest)
[![codecov](https://codecov.io/gh/EpicWink/seddy/branch/master/graph/badge.svg)](
https://codecov.io/gh/EpicWink/seddy)
[![Documentation Status](https://readthedocs.org/projects/seddy/badge/?version=latest)](
https://seddy.readthedocs.io/en/latest/?badge=latest)

Multi-workflow SWF decider and workflow management service.

Features:
* Start a decider on many workflows
* Specify a directed graph (aka DAG) of activity (via dependencies) tasks in the
  workflow
* Supports coloured logging
* Extensible decision-building: just subclass `seddy.decisions.DecisionsBuilder`
* Register workflows

## Installation
```bash
pip3 install seddy
```

For coloured logging
```bash
pip3 install coloredlogs
```

## Usage
Get the CLI usage
```bash
seddy -h
```

API documentation
```bash
pydoc3 seddy
```

See [the example DAG workflow definition](tests/data/dag.json).
