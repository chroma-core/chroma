## Release Process

This guide covers how to release the Chroma clients and the CLI.

If you want to release changes that affect only a client, choose the release path for that client. If changes include server-side changes or CLI functionality changes, release the CLI. This also triggers a release for all clients as the CLI is bundled in them.

### Step 1: Create a Release Branch

In the root directory of the Chroma repo, run 

```shell
uv run release.py
```

This script will ask you what release path you want to take (either client, or the CLI), and input new versions for the files necessary for a new release.

The `release` script will create a local branch with all the changes in release-related files committed. The branch name will reflect the type of release you chose. For example, when releasing 
* the Python client with version `1.2.3`, the branch will be `release/python-1.2.3`.
* the JS client with version `1.2.3`, the branch will be `release/js-1.2.3`.
* the Rust client with version `1.2.3`, the branch will be `release/rust-1.2.3`.
* the CLI with version `1.2.3`, and a result also the Python and JS clients, say with versions `4.5.6` and `7.8.9` respectively, the branch will be `release/cli-1.2.3-python-4.5.6-js-7.8.9`. 

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

Once the PR is reviewed and merged, monitor the `release` CI workflow on `main`. For `release/` branches, this workflow also performs the appropriate release once tests have passed:

* For Python releases - builds the wheels and publishes to PyPi (we publish to Test PyPi for all PRs, not only releases).
* For JS - builds the `chromadb` package and publishes to NPM.
* For Rust - publishes `chroma-error`, `chroma-types`, `chroma-api-types`, and `chroma` to crates.io.
* For CLI - in addition to the Python and JS releases above, builds the CLI and publishes as a GitHub release, builds the JS-bindings to bundle the CLI in the JS client, and publishes them to NPM.

### Step 4: Upload AWS CF Template

After a **Python or CLI** release, upload the updated AWS CloudFormation template to our public S3 bucket, under the `cloudformation/latest` directory:

```
https://s3.amazonaws.com/public.trychroma.com/cloudformation/latest/
```

### Files to Review

The `release` script takes care of all the following file updates. When reviewing a release PR make sure the following files have changed with the correct version.

#### Python Client Release

1. `chromadb/__init__.py` contains the version of the Python client:

```python
__version__ = "1.2.3"
```

2. `deployments/aws/chroma.cf.json` is the AWS Cloud Formation template for deploying single-node Chroma with the latest version:

```json
{
  // ...
  "ChromaVersion": {
    "Description": "Chroma version to install",
    "Type": "String",
    "Default": "1.2.3"
  }
}
```

3. `deployments/azure/main.tf` is the Terraform template for deploying single-node Chroma on Azure with the latest version:

```terraform
variable "chroma_version" {
  description = "Chroma version to install"
  default     = "1.2.3"
}
```

4. `deployments/gcp/main.tf` is the Terraform template for deploying single-node Chroma on GCP with the latest version:

```terraform
variable "chroma_version" {
description = "Chroma version to install"
default     = "1.2.3"
}
```

#### JS Client Release

Update `clients/new-js/packages/chromadb/package.json` with the new version:

```json
{
  "name": "chromadb", 
  "version": "1.2.3",
  // ...
}
```

#### Rust Client Release

1. Update the version in `rust/error/Cargo.toml`

```toml
[package]
name = "chroma-error"
version = "1.2.3"
# ...
```

2. Update the version in `rust/types/Cargo.toml`

```toml
[package]
name = "chroma-types"
version = "1.2.3"
# ...
```

3. Update the version in `rust/api-types/Cargo.toml`

```toml
[package]
name = "chroma-api-types"
version = "1.2.3"
# ...
```

4. Update the version in `rust/chroma/Cargo.toml`

```toml
[package]
name = "chroma"
version = "1.2.3"
# ...
```

5. Update the version in the root `Cargo.toml` with the version you set in steps 1-4:

```toml
# ...

[workspace.dependencies]
# ...
chroma = { path = "rust/chroma", version = "1.2.3" }
chroma-api-types = { path = "rust/api-types", version = "1.2.3" }
chroma-error = { path = "rust/error", version = "1.2.3" }
chroma-types = { path = "rust/types", version = "1.2.3" }
# ...

```

#### CLI Release

1. Follow the steps for the Python Client release.
2. Follow the steps for the JS Client release.
3. Update the version for the CLI crate in `rust/cli/Cargo.toml`:

```toml
[package]
name = "chroma-cli"
version = "1.2.3"
# ...
```

4. Update the version in the Unix installation script in `rust/cli/install/install.sh`:

```shell
RELEASE="cli-1.2.3"
```

5. Update the version in the PowerShell (Windows) installation script in `rust/cli/install/install.ps1`:

```powershell
$release = "cli-1.2.3"
```

6. Update the CLI version in `rust/cli/src/lib.rs`:

```rust
#[command(version = "1.2.3")]
struct Cli {
    // ...
}
```

7. Update the version of the JS-bindings NPM packages in `rust/js_bindings/package.json`:

```json
{
  "name": "chromadb-js-bindings",
  "version": "1.2.3",
  // ...
}
```

8. Use the same version from step 7 to update the versions for the published packages for each platform:
* `rust/js_bindings/npm/darwin-arm64/package.json`
* `rust/js_bindings/npm/darwin-x64/package.json`
* `rust/js_bindings/npm/linux-arm64-gnu/package.json`
* `rust/js_bindings/npm/linux-x64-gnu/package.json`
* `rust/js_bindings/npm/win32-arm64-msvc/package.json`
* `rust/js_bindings/npm/win32-x64-msvc/package.json`

For example, in `rust/js_bindings/npm/darwin-arm64/package.json`:

```json
{
  "name": "chromadb-js-bindings-darwin-arm64",
  "version": "1.2.3",
  // ...
}
```

9. Use the same version from step 7 to update the optional dependencies for the JS client in `clients/new-js/packages/chromadb/package.json`:

```json
{
  // ...
  "optionalDependencies": {
    "chromadb-js-bindings-darwin-arm64": "^1.2.3",
    "chromadb-js-bindings-darwin-x64": "^1.2.3",
    "chromadb-js-bindings-linux-arm64-gnu": "^1.2.3",
    "chromadb-js-bindings-linux-x64-gnu": "^1.2.3",
    "chromadb-js-bindings-win32-x64-msvc": "^1.2.3"
  },
  // ...
}
```