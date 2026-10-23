# Develop

This readme is helpful for local dev.

### Prereqs:

- Make sure you have Java installed (for the generator). You can download it from [java.com](https://java.com)
- Make sure you set ALLOW_RESET=True for your Docker Container. If you don't do this, tests won't pass.

```
environment:
      - IS_PERSISTENT=TRUE
      - ALLOW_RESET=True
```

- Make sure you are running the docker backend at localhost:8000 (\*there is probably a way to stand up the fastapi server by itself and programmatically in the loop of generating this, but not prioritizing it for now. It may be important for the release)

### Running the Examples

To get started developing on the JS client libraries, you'll want to run the examples.

1. `pnpm install` to install deps.
1. `pnpm build` to build the library.
1. `cd examples/browser` or `cd examples/node`
1. `pnpm install` to install example deps.
1. `pnpm dev` to run the example.

### Generating REST Client Code

If you modify the REST API, you'll need to regenerate the generated code that underlies the JavaScript client libraries.

1. `pnpm install` to install deps
2. `pnpm genapi`
3. Examples are in the `examples` folder. There is one for the browser and one for node. Run them with `pnpm dev`, eg `cd examples/browser && pnpm dev`

### Running tests

`pnpm test` will launch a test docker backend, run a db cleanup and run tests.
`pnpm test:run` will run against the docker backend you have running. But CAUTION, it will delete data. This is the easiest and fastest way to run tests.

### Pushing to npm

#### Automatically

##### Increase the version number

1. Create a new PR for the release that upgrades the version in code. Name it `js_release/A.B.C` for production releases and `js_release_alpha/A.B.C` for alpha releases. In the package.json update the version number to the new version. For production releases this is just the version number, for alpha
   releases this is the version number with '-alphaX' appended to it. For example, if the current version is 1.0.0, the alpha release would be 1.0.0-alpha1 for the first alpha release, 1.0.0-alpha2 for the second alpha release, etc.
2. Add the "release" label to this PR
3. Once the PR is merged, tag your commit SHA with the release version

```bash
git tag js_release_A.B.C <SHA>

# or for alpha releases:

git tag js_release_alpha_A.B.C <SHA>
```

4. You need to then wait for the github action for main for `chroma js release` to complete on main.

##### Perform the release

1. Push your tag to origin to create the release

```bash

git push origin js_release_A.B.C

# or for alpha releases:

git push origin js_release_alpha_A.B.C
```

2. This will trigger a Github action which performs the release

#### Manually

`pnpm run release` pushes the `package.json` defined packaged to the package manager for authenticated users. It will build, test, and then publish the new version.

### Useful links

https://gaganpreet.in/posts/hyperproductive-apis-fastapi/