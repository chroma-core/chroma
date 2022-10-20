# Development Instructions

This project uses the testing, build and release standards specified
by the PyPA organization and documented at
https://packaging.python.org.

## Environment

To set up an environment allowing you to test, build or distribute the
project, you will need to set up and activate a virtual environment
specific to this library. For example:

```
python3 -m venv venv      # Only need to do this once
source venv/bin/activate  # Do this each time you use a new shell for the project
```

Then, install the development dependencies by running

```
pip install -r dev_requirements.txt
```

## Testing

Unit tests are in the `/tests` directory.

To run unit tests using your current environment, run `pytest`.

## Manual Build

To manually build a distribution, run `python -m build`.

The project's source and wheel distributions will be placed in the `dist` directory.

## Manual Release

Not yet implemented.

## Versioning

This project uses PyPA's `setuptools_scm` module to determine the
version number for build artifacts, meaning the version number is
derived from Git rather than hardcoded in the repository. For full
details, see the
[documentation for setuptools_scm](https://github.com/pypa/setuptools_scm/).

In brief, version numbers are generated as follows:

- If the current git head is tagged, the version number is exactly the
  tag (e.g, `0.0.1`).
- If the the current git head is a clean checkout, but is not tagged,
  the version number is a patch version increment of the most recent
  tag, plus `devN` where N is the number of commits since the most
  recent tag. For example, if there have been 5 commits since the
  `0.0.1` tag, the generated version will be `0.0.2-dev5`.
- If the current head is not a clean checkout, a `+dirty` local
  version will be appended to the version number. For example,
  `0.0.2-dev5+dirty`.

At any point, you can manually run `python -m setuptools_scm` to see
what version would be assigned given your current state.

## Continuous Integration

This project uses Github Actions to run unit tests automatically upon
every commit to the main branch. See the documentation for Github
Actions and the flow definitions in `.github/workflows` for details.

## Continuous Delivery

Not yet implemented.
