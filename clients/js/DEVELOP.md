# Develop

This readme is helpful for local dev.

### Prereqs:
- Make sure you have Java installed (for the generator)
- Make sure you are running the docker backend at localhost:8000 (*there is probably a way to stand up the fastapi server by itself and programatically in the loop of generating this, but not prioritizing it for now. It may be important for the release)

### Generating
1. `yarn` to install deps
2. `yarn genapi-zsh` if you have zsh
3. `yarn start` will run the parcel server for easier testing. That is in the `example` folder.

### Pushing to npm
The goal of the design is that this will be added to our github action releases so that the JS API is always up to date and pinned against the python backend API. 

`npm publish` pushes the `package.json` defined packaged to the package manager for authenticated users.

### Useful links

https://gaganpreet.in/posts/hyperproductive-apis-fastapi/