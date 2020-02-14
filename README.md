# seddy
Multi-workflow SWF decider service.

## Getting Started
### Prerequisites
* [Python 3](https://python.org/) for pythonic reasons
* AWS credentials

### Installation
```bash
pip3 install sitesee-seddy
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

## Running tests
```bash
pip3 install -r tests/requirements.txt
pytest -vvra --cov=sitesegment
```
