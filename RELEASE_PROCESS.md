## Release Process

This guide covers how to release chroma to PyPi, NPM, as well as releasing the standalone Chroma CLI.  

### Python

1. Create a new PR for the release that upgrades the version in code. Name it `release/python-[A.B.C]`. For example, for releasing version `1.2.3` you'd make a new branch:

```shell
git checkout -b release/python-1.2.3
```

2. Update hte version on in [`chromadb/__init__.py`](https://github.com/chroma-core/chroma/blob/main/chromadb/__init__.py). For example, when releasing version `1.2.3`, set:

```python
__version__ = "1.2.3"
```

3. The AWS CloudFormation template, as well as the TF Azure and GCP templates use the Python package to run a Chroma server via the CLI. Update these templates to download the correct version.

In [`deployments/aws/chroma.cf.json`]():

```json
"Default": "1.2.3"
```

#### Increase the version number
1. Create a new PR for the release that upgrades the version in code. Name it `release/A.B.C` In [this file](https://github.com/chroma-core/chroma/blob/main/chromadb/__init__.py) update the __ version __. The commit comment (and hence PR title) should be `[RELEASE] A.B.C`
```
__version__ = "A.B.C"
```
2. On Github, add the "release" label to this PR
3. Once the PR checks pass, merge it. This will trigger Github Actions to release to PyPi, DockerHub, and the JS client. It may take a while before they complete.
4. Once the PR is merged and the Github Actions complete, tag your commit SHA with the release version
```
git tag A.B.C <SHA>
```
5. Push your tag to origin to create the release. This will trigger more Github Actions to perform the release.
```
git push origin A.B.C
```
6. On the right panel on Github, click on "Releases", and the new release should appear first. Make sure it is marked as "latest".
