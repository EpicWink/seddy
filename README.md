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
* Extensible decision-building: just subclass `seddy.DecisionsBuilder`
* Register workflows

What `seddy` doesn't do:
* Activity workers
* Anything [AWS CLI](https://aws.amazon.com/cli/) can
  * Workflows listing and detailing
  * Workflow execution management and history detailing
  * Tag management
  * Domain management
  * Activities management
* Validate workflow execution input
* Manage workflows definition file (`seddy` just uses it)

## Installation
```bash
pip3 install seddy
```

Install extra packages for further functionality
* Coloured logging: ``coloredlogs``
* YAML workflows specs file: ``pyyaml`` or ``ruamel.yaml``
* JSON-format logging: ``python-json-logger``

## Usage
Get the CLI usage
```bash
seddy -h
```

API documentation
```bash
pydoc3 seddy
```

## Docker
Instead of installing `seddy` locally, you can use our pre-built Docker image
```bash
docker run -v /path/to/workflow/file/parent:/seddy-data seddy -h
```
