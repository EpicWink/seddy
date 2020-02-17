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
Follow [PEP-8](https://www.python.org/dev/peps/pep-0008/?), then Google Python
style-guide (for the most part). In particular, use Google-style docstrings.
Use hanging-indent style, with 4 spaces for indentation. No lines with just a
closing bracket! 80-character lines.

## TODO
See the [issues page](https://github.com/EpicWink/swf-decider/issues) for the current
discussions on improvements, features and bugs.

## Generating documentation
When the package structure is changed (moved/deleted/new packages/modules), the
documentation configuration must be regenerated:
```bash
sphinx-apidoc -ef -o docs/src/ src/seddy/ --ext-autodoc
```

To build the documentation:
```bash
cd docs/
pip3 install -r requirements.txt
make
```
