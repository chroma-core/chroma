# Development Instructions

This project uses the testing, build and release standards specified
by the PyPA organization and documented at
<https://packaging.python.org>.

## Setup

Set up a virtual environment and install the project's requirements
and dev requirements:

```bash
python3 -m venv venv      # Only need to do this once
source venv/bin/activate  # Do this each time you use a new shell for the project
pip install -r requirements.txt
pip install -r requirements_dev.txt
pre-commit install # install the precommit hooks
```

Install protobuf:
for MacOS `brew install protobuf`

You can also install `chromadb` the `pypi` package locally and in editable mode with `pip install -e .`.

## Local dev setup for distributed chroma

We use tilt for providing local dev setup. Tilt is an open source project

### Requirement

- Docker
- Local Kubernetes cluster (Recommended: [OrbStack](https://orbstack.dev/) for mac, [Kind](https://kind.sigs.k8s.io/) for linux)
- [Tilt](https://docs.tilt.dev/)
- [Helm](https://helm.sh)

1. Start Kubernetes. If you're using OrbStack, navigate to `Kubernetes - Pods`, and select `Turn On`
2. Start a distributed Chroma cluster by running `tilt up` from the root of the repository.
3. Once done, it will expose Chroma on port 8000. You can also visit the Tilt dashboard UI at `http://localhost:10350/`.
4. To clean and remove all the resources created by Tilt, use `tilt down`.

## Testing

Unit tests are in the `/chromadb/test` directory.

To run unit tests using your current environment, run `pytest`.

Make sure to have `tilt up` running for these tests otherwise some distributed Chroma tests will fail.

## Manual Build

Make sure the following is only done in the virtual environment created in the [Setup](#setup) section above.

To manually build the rust codebase and bindings for type safety, run `maturin dev`.

To manually build a distribution, run `python -m build`.

The project's source and wheel distributions will be placed in the `dist` directory.

If you have `tilt up` running, saving changes to your files will automatically rebuild new binaries with your changes and deploy to the local cluster `tilt` has running.

## IDE Recommendations

If you are developing with VSCode or its derivatives (Windsurf/Cursor etc), make sure to install the `rust-analyzer` extension. It helps with auto-formatting, Intellisense and code navigation.

For debugging it is recommended to install the `CodeLLDB` extension.

You should be able to run and debug the rust tests by clicking on the 'Run Test' or 'Debug' button found above the test method definitions.

![rust-analyzer extension](https://github.com/user-attachments/assets/a7779e4d-9d64-4511-9271-b790bed7b68b)

## Setting breakpoints in Distributed Chroma

Debugging binaries in the Kubernetes pods that `tilt up` spins up is a bit more involved. Right now the only reliable way to set a breakpoint in this scenario is to log in to the pod, install lldb/gdb and set a breakpoint that way. For example after running `tilt up` you can set a breakpoint in the query-service-0 pod as follows:

```bash
kubectl exec -it query-service-0 -n chroma -- /bin/sh
apt-get update && apt-get install gdb
gdb
(gdb) b <relative_file_path>:<lineno>
```

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
- If the current git head is a clean checkout, but is not tagged,
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
