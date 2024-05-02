GitHub *still* does not support organizing workflows into directories, so instead we use some notation:

- A workflow starting with `_` is a reusable workflow and should exclusively have a `workflow_call` trigger.
- Any other workflow is expected to have standard triggers (e.g. `push`, `pull_request`, etc.) and should not be called by other workflows.

All workflows should be prefixed by their language name, e.g. `python-test.yml`.
