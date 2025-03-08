# Develop

This readme is helpful for local dev.

## Monorepo Structure

This project is structured as a monorepo with three packages:

- `@internal/chromadb-core`: Internal package containing shared code (not published)
- `chromadb`: Public package with bundled dependencies
- `chromadb-client`: Public package with peer dependencies

### Package Structure Explained

- **@internal/chromadb-core**: Contains all the core functionality and is used by both public packages.
- **chromadb**: Includes all embedding library dependencies bundled with the package. Use this if you want a simple installation without worrying about dependency management.
- **chromadb-client**: Uses peer dependencies for embedding libraries. Use this if you want to manage your own versions of embedding libraries or to keep your dependency tree lean.

### Prerequisites:

- Make sure you have Java installed (for the generator). You can download it from [java.com](https://java.com)
- Make sure you set ALLOW_RESET=True for your Docker Container. If you don't do this, tests won't pass.

```
environment:
      - IS_PERSISTENT=TRUE
      - ALLOW_RESET=True
```

- Make sure you are running the docker backend at localhost:8000 (\*there is probably a way to stand up the fastapi server by itself and programmatically in the loop of generating this, but not prioritizing it for now. It may be important for the release)

## Working with the Monorepo

### Installing Dependencies

To install all dependencies for the monorepo:

```bash
pnpm install
```

### Building Packages

To build all packages:

```bash
pnpm build
```

To build only the core package:

```bash
pnpm build:core
```

To build only the public packages:

```bash
pnpm build:packages
```

### Running the Examples

To get started developing on the JS client libraries, you'll want to run the examples.

1. `pnpm install` to install deps.
1. `pnpm build` to build all packages.
1. `cd examples/browser` or `cd examples/node`
1. `pnpm install` to install example deps.
1. `pnpm dev` to run the example.

### Generating REST Client Code

If you modify the REST API, you'll need to regenerate the generated code that underlies the JavaScript client libraries.

1. `pnpm install` to install deps
2. `pnpm genapi`
3. Examples are in the `examples` folder. There is one for the browser and one for node. Run them with `pnpm dev`, eg `cd examples/browser && pnpm dev`

### Running tests

`pnpm test` will run tests for all packages.

### Pushing to npm

#### Automatically

##### Increase the version number

1. Create a new PR for the release that upgrades the version in code. Name it `js_release/A.B.C` for production releases and `js_release_alpha/A.B.C` for alpha releases. Update the version number in the root `package.json` and all package.json files in the packages directory. For production releases this is just the version number, for alpha releases this is the version number with '-alphaX' appended to it.
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

`pnpm publish:packages` pushes the packages to the package manager for authenticated users. It will build, test, and then publish the new version.

### Useful links

https://gaganpreet.in/posts/hyperproductive-apis-fastapi/
