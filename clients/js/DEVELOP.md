# Develop

This readme is helpful for local dev.

### Prereqs:

- Make sure you have Java installed (for the generator). You can download it from [java.com](https://java.com)
- Make sure you set ALLOW_REST=True for your Docker Container. If you don't do this, tests won't pass.
```
environment:
      - IS_PERSISTENT=TRUE
      - ALLOW_RESET=True
```
- Make sure you are running the docker backend at localhost:8000 (\*there is probably a way to stand up the fastapi server by itself and programmatically in the loop of generating this, but not prioritizing it for now. It may be important for the release)

### Generating

1. `yarn` to install deps
2. `yarn genapi`
3. Examples are in the `examples` folder. There is one for the browser and one for node. Run them with `yarn dev`, eg `cd examples/browser && yarn dev`

### Running test

`yarn test` will launch a test docker backend, run a db cleanup and run tests.
`yarn test:run` will run against the docker backend you have running. But CAUTION, it will delete data. This is the easiest and fastest way to run tests.

### Pushing to npm

The goal of the design is that this will be added to our github action releases so that the JS API is always up to date and pinned against the python backend API.

`npm run release` pushes the `package.json` defined packaged to the package manager for authenticated users. It will build, test, and then publish the new version.

### Useful links

https://gaganpreet.in/posts/hyperproductive-apis-fastapi/
