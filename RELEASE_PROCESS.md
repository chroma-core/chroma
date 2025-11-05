## Release Process

This guide covers how to release chroma to PyPi, NPM, as well as releasing the standalone Chroma CLI.

### CLI

**Note:** Use this path for releasing any Chroma server updates, or any CLI functionality updates. The CLI is bundled in both our Python and JS/TS packages, so this path also includes releasing both clients, with the updated CLI.

1. Create a new branch for the release that upgrades the versions in code. The PR will upgrade the versions of the CLI and the clients. Name it `release/cli-[A.B.C]-python-[D.E.F]-js-[X.Y.Z]`. For example, for releasing CLI version `1.2.3`, Python client version `4.5.6`, and JS/TS client version `7.8.9`, you'd make a new branch:

```shell
git checkout -b release/cli-1.2.3-python-4.5.6-js-7.8.9
```

2. Update the CLI version in the [`rust/cli/Cargo.toml`](https://github.com/chroma-core/chroma/blob/main/rust/cli/Cargo.toml):

```toml
version = "1.2.3"
```

3. Update the CLI version for `clap` in [`rust/cli/src/lib.rs`](https://github.com/chroma-core/chroma/blob/main/rust/cli/src/lib.rs):

```rust
#[command(version = "1.2.3")]
```

4. Update the CLI version in the installation script ([`rust/cli/install/install.sh`](https://github.com/chroma-core/chroma/blob/main/rust/cli/install/install.sh)):

```shell
RELEASE="cli-1.2.3"
```

5. Update the CLI version in the Windows installation script ([`rust/cli/install/install.ps1`](https://github.com/chroma-core/chroma/blob/main/rust/cli/install/install.ps1))

```powershell
$release = "cli-1.2.1"
```

6. Upgrade the version of the JS binding packages. These are NPM packages for various platforms containing CLI binaries. These packages have their own versions. Update the following `package.json` files. They should all be updated to the same version:
* [`rust/js_bindings/package.json`](https://github.com/chroma-core/chroma/blob/main/rust/js_bindings/package.json)
* [`rust/js_bindings/npm/darwin-arm64/package.json`](https://github.com/chroma-core/chroma/tree/main/rust/js_bindings/npm/darwin-arm64)
* [`rust/js_bindings/npm/darwin-x64/package.json`](https://github.com/chroma-core/chroma/tree/main/rust/js_bindings/npm/darwin-x64)
* [`rust/js_bindings/npm/linux-arm64-gnu/package.json`](https://github.com/chroma-core/chroma/tree/main/rust/js_bindings/npm/linux-arm64-gnu)
* [`rust/js_bindings/npm/linux-x64-gnu/package.json`](https://github.com/chroma-core/chroma/tree/main/rust/js_bindings/npm/linux-x64-gnu)
* [`rust/js_bindings/npm/win32-arm64-msvc/package.json`](https://github.com/chroma-core/chroma/tree/main/rust/js_bindings/npm/win32-arm64-msvc)
* [`rust/js_bindings/npm/win32-x64-msvc/package.json`](https://github.com/chroma-core/chroma/tree/main/rust/js_bindings/npm/win32-x64-msvc)

```json
"version": "1.1.1"
```

7. Update the dependencies of the JS/TS client for the updated JS-bindings in [`clients/new-js/packages/chromadb/package.json`](https://github.com/chroma-core/chroma/blob/main/clients/new-js/packages/chromadb/package.json):

```json
"optionalDependencies": {
    "chromadb-js-bindings-darwin-arm64": "^1.1.1",
    "chromadb-js-bindings-darwin-x64": "^1.1.1",
    "chromadb-js-bindings-linux-arm64-gnu": "^1.1.1",
    "chromadb-js-bindings-linux-x64-gnu": "^1.1.1",
    "chromadb-js-bindings-win32-x64-msvc": "^1.1.1"
},
```

8. In the same file ([`clients/new-js/packages/chromadb/package.json`](https://github.com/chroma-core/chroma/blob/main/clients/new-js/packages/chromadb/package.json)), update the JS/TS client version:

```json
"name": "chromadb",
"version": "7.8.9",
```

9. Update the version for the Python client in [`chromadb/__init__.py`](https://github.com/chroma-core/chroma/blob/main/chromadb/__init__.py):

```python
__version__ = "4.5.6"
```

10. The AWS CloudFormation template, as well as the TF Azure and GCP templates use the Python package to run a Chroma server via the CLI. Update these templates to download the correct version.

[`deployments/aws/chroma.cf.json`](https://github.com/chroma-core/chroma/blob/main/deployments/aws/chroma.cf.json):

```json
"Default": "4.5.6"
```

[`deployments/azure/chroma.tfvars.tf`](https://github.com/chroma-core/chroma/blob/main/deployments/azure/chroma.tfvars.tf):

```yaml
chroma_version                  = "4.5.6"
```

[`deployments/azure/main.tf`](https://github.com/chroma-core/chroma/blob/main/deployments/azure/main.tf):

```yaml
variable "chroma_version" {
  description = "Chroma version to install"
  default     = "4.5.6"
}
```

[`deployments/gcp/chroma.tfvars.tf`](https://github.com/chroma-core/chroma/blob/main/deployments/gcp/chroma.tfvars.tf):

```yaml
chroma_version                  = "4.5.6"
```

[`deployments/gcp/main.tf`](https://github.com/chroma-core/chroma/blob/main/deployments/gcp/main.tf):

```yaml
variable "chroma_version" {
  description = "Chroma version to install"
  default     = "4.5.6"
}
```

11. Make a new PR with your branch, and label it with the `release` label.
12. Once the PR is merged, and the GitHub actions pass on `main`, tag your commit SHA with the **CLI version name**, `cli_<version>`, and push it. This will trigger a workflow that builds and releases the CLI and its JS bindings, as well as releasing both clients to PyPi and NPM.

```shell
git tag cli_<version> <SHA>
git push origin cli_<version>
```

Once the release workflow is done, you should see the new CLI and Python releases on the right-hand side on the Chroma repo under "Releases".

13. Upload the AWS CloudFormation template to our public S3 bucket to be the "latest" template: `s3://public.trychroma.com/cloudformation/latest/`

### Python

**Note:** use this path for releasing python **client** side changes.

1. Create a new branch for the release that upgrades the version in code. Name it `release/python-[A.B.C]`. For example, for releasing version `1.2.3` you'd make a new branch:

```shell
git checkout -b release/python-1.2.3
```

2. Update the version in [`chromadb/__init__.py`](https://github.com/chroma-core/chroma/blob/main/chromadb/__init__.py). For example, when releasing version `1.2.3`, set:

```python
__version__ = "1.2.3"
```

3. The AWS CloudFormation template, as well as the TF Azure and GCP templates use the Python package to run a Chroma server via the CLI. Update these templates to download the correct version.

[`deployments/aws/chroma.cf.json`](https://github.com/chroma-core/chroma/blob/main/deployments/aws/chroma.cf.json):

```json
"Default": "1.2.3"
```

[`deployments/azure/chroma.tfvars.tf`](https://github.com/chroma-core/chroma/blob/main/deployments/azure/chroma.tfvars.tf):

```yaml
chroma_version                  = "1.2.3"
```

[`deployments/azure/main.tf`](https://github.com/chroma-core/chroma/blob/main/deployments/azure/main.tf):

```yaml
variable "chroma_version" {
  description = "Chroma version to install"
  default     = "1.2.3"
}
```

[`deployments/gcp/chroma.tfvars.tf`](https://github.com/chroma-core/chroma/blob/main/deployments/gcp/chroma.tfvars.tf):

```yaml
chroma_version                  = "1.2.3"
```

[`deployments/gcp/main.tf`](https://github.com/chroma-core/chroma/blob/main/deployments/gcp/main.tf):

```yaml
variable "chroma_version" {
  description = "Chroma version to install"
  default     = "1.2.3"
}
```

4. Make a new PR with your branch, and label it with the `release` label.
5. Once the PR is merged, and the GitHub actions pass on `main`, tag your commit SHA with the version name and push it. This will trigger a workflow that performs the release to PyPi.

```shell
git tag <version> <SHA>
git push origin <version>
```

Once the release workflow is done, you should see the new release on the right-hand side on the Chroma repo under "Releases".

6. Upload the AWS CloudFormation template to our public S3 bucket to be the "latest" template: `s3://public.trychroma.com/cloudformation/latest/` 

### Javascript/Typescript

**Note:** use this path for releasing JS/TS **client** side changes.

1. Create a new branch for the release that upgrades the version in code. Name it `release/js-[A.B.C]`. For example, for releasing version `1.2.3` you'd make a new branch:

```shell
git checkout -b release/js-1.2.3
```

2. Update the version in [`clients/new-js/packages/chromadb/package.json`](https://github.com/chroma-core/chroma/blob/main/clients/new-js/packages/chromadb/package.json). For example, when releasing version `1.2.3`, set:

```json
"version": "1.2.3"
```

3. Make a new PR with your branch, and label it with the `release` label.
4. Once the PR is merged, and the GitHub actions pass on `main`, tag your commit SHA with `js_release_<version>` and push it. This will trigger a workflow that performs the release to NPM.

```shell
git tag <version> <SHA>
git push origin <version>
```

