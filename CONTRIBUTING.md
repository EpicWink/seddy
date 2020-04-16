# seddy contribution giude
Thanks for wanting to help out!

## Environment installation
```bash
pip3 install -e .
pip3 install -r tests/requirements.txt
```

## Testing
```bash
pytest --cov seddy
```

## Style-guide
Follow [PEP-8](https://www.python.org/dev/peps/pep-0008/?), hanging-indent style, with 4
spaces for indentation, 88-character lines. Lint with [`black`](
https://black.readthedocs.io/en/stable/)

## TODO
See the [issues page](https://github.com/EpicWink/seddy/issues) for the current
discussions on improvements, features and bugs.

## Design goal
`seddy` is designed to be a decider and workflow definition manager for multiple
workflows. All other interaction with SWF is either already provided by the AWS CLI
[`awscli`](https://aws.amazon.com/cli/), or is part of the activity worker. Therefore,
`seddy` won't be replicating functionality from `awscli`, so it will focus on being a
multi-workflow decider and registration manager.

## Generating documentation
A simple Makefile handles the pre-requisites installation, documentation markup
generation and documentation generation.
```bash
make -C docs
```

Output documentation is in "docs/build/html".

## Code of conduct
Please note that this project is released with a [Contributor Code of Conduct](
CODE_OF_CONDUCT.md).
By participating in this project you agree to abide by its terms.

Breaches of the code of conduct will be treated with according to severity as
deemed by project maintainers. This can include warnings, bannings from project
interaction and reports to GitHub. Correspondence will be made via email.
