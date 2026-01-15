## Release Process

This guide covers how to release the Chroma clients and the CLI.

If you want to release changes that affect only a client, choose the release path for that client. If changes include server-side changes or CLI functionality changes, release the CLI. This also triggers a release for all clients as the CLI is bundled in them.

### Step 1: Create a Release Branch

In the root directory of the Chroma repo, run 

```shell
uv run release.py
```

This script will ask you what release path you want to take (either client, or the CLI), and input new versions for the files necessary for a new release.

When done, this script will create a local branch with all the changes committed. The branch name will reflect the type of release you chose. For example, when releasing the Python client with version `1.2.3`, the branch will be called `release/python-1.2.3`.

Push the branch:
```shell
git push origin release/python-1.2.3
```

### Step 2: Create a Release PR

On GitHub, create a PR with the branch you pushed, and assign it the appropriate label:
* For Python - `release-python`
* For JS - `release-js`
* For Rust - `release-rust`
* For CLI (and clients) - `release-all`

### Step 3: Review the PR

Ask another Chroma engineer to review and approve the PR. Note that no tests are run on a release PR.

