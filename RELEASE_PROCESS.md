## Release Process

This guide covers how to release chroma to PyPi, NPM, as well as releasing the standalone Chroma CLI.  

### Python

1. Create a new PR for the release that upgrades the version in code. Name it `release/python-[A.B.C]`. For example, for releasing version `1.2.3` you'd make a new branch:

```shell
git checkout -b release/python-1.2.3
```

2. Update the version on in [`chromadb/__init__.py`](https://github.com/chroma-core/chroma/blob/main/chromadb/__init__.py). For example, when releasing version `1.2.3`, set:

```python
__version__ = "1.2.3"
```

3. The AWS CloudFormation template, as well as the TF Azure and GCP templates use the Python package to run a Chroma server via the CLI. Update these templates to download the correct version.

In [`deployments/aws/chroma.cf.json`](https://github.com/chroma-core/chroma/blob/main/deployments/aws/chroma.cf.json):

```json
"Default": "1.2.3"
```

In [`deployments/azure/chroma.tfvars.tf`](https://github.com/chroma-core/chroma/blob/main/deployments/azure/chroma.tfvars.tf):

```yaml
chroma_version                  = "1.2.3"
```

In [`deployments/azure/main.tf`](https://github.com/chroma-core/chroma/blob/main/deployments/azure/main.tf):

```yaml
variable "chroma_version" {
  description = "Chroma version to install"
  default     = "1.2.3"
}
```

In [`deployments/gcp/chroma.tfvars.tf`](https://github.com/chroma-core/chroma/blob/main/deployments/gcp/chroma.tfvars.tf):

```yaml
chroma_version                  = "1.2.3"
```

In [`deployments/gcp/main.tf`](https://github.com/chroma-core/chroma/blob/main/deployments/gcp/main.tf):

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

Once the release workflow is done, you should see the new release in on the right-hand side on the Chroma repo under "Releases".

6. Upload the AWS CloudFormation template to our public S3 bucket to be the "latest" template: `s3://public.trychroma.com/cloudformation/latest/` 

### Javascript/Typescript

1. Create a new PR for the release that upgrades the version in code. Name it `release/js-[A.B.C]`. For example, for releasing version `1.2.3` you'd make a new branch:

```shell
git checkout -b release/js-1.2.3
```

2. Update the version in `clients/new-js/packages/chromadb/package.json`

