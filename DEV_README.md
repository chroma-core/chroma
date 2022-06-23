# Chroma

## Tool selection

React, FastAPI, Sqlite and Graphql were chosen for their simplicity and ubiquity. We can get fancier with tooling when we need to. 

The folder structure is:
- `chroma-ui`: react app
- `chroma`: contains all python code, the core library and python apps
- `examples`: example scripts that uses the Chroma pip package

# Using Make
Make makes it easy to do stuff:
- `make install` will install all python and js dependencies for you
- `make run` will run the whole app for you. You can also use `make run_data_manager`/`make run_app` and more. 
- `make build_prod` will set up a production build for you
- `pip install .` and `chroma application run` will run the full stack

# Setup 

### The frontend
The frontend uses (see all boilerplate dependencies in chroma-ui/package.json):

- The frontend uses React via Create React App. 
- We use [Chakra](https://chakra-ui.com/) for UI elements. 
- This is a typescript app!
- Linting (eslint, `yarn lint`) and code formatting (prettier, `yarn format`) have been setup. This is not autorun right now.
- Jest has also been setup for testing, `yarn test`. This is not autorun right now.
- When the app is built with `yarn build`, it copies it's built artifacts to the `app_backend` which serves it. 
- Urql for easy Graphql state and hooks

One command up to install everything `make install`.

We use a custom build of `regl-scatterplot` and the supporting camera `dom-2d-camera`. Here is how to configure that

```
# if you need node, follow these instructions https://formulae.brew.sh/formula/node

# requires node version 16, we can probably use an even newer verison, this was the min version required by our deps
# if you need nvm, follow these instructions https://github.com/nvm-sh/nvm#installing-and-updating
# bumped from 14.13.1 to 16 for m1 mac support
nvm install 16
 
# if you need yarn, follow these instructions: https://classic.yarnpkg.com/lang/en/docs/install/#mac-stable

# simple setup
make fetch_deps_and_rebuild_chroma_ui

# OR step by step setup

# cd to your source where you have chroma and clone the repo
git clone git@github.com:chroma-core/dom-2d-camera.git

# build it
cd dom-2d-camera
yarn install --frozen-lockfile
yarn build

# cd to your source where you have chroma and clone the repo
git clone git@github.com:chroma-core/regl-scatterplot.git

# build it
cd regl-scatterplot
yarn install --frozen-lockfile
yarn build

# cd into chroma ui
cd ../chroma/chroma-ui

# optional cleanup if you have built regl-scatterplot before
rm -rf node_modules

# install dependencies
yarn install --frozen-lockfile

# run - this will load http://localhost:3000 in the browser
# to run the frontend, you will want the data manager and app backend running as well
yarn start
```

### The backend
The backend uses:
- FastAPI
- Strawberry (graphql)
- Alembic (migrations)

- `app_backend` runs on port 4000. `app_backend` serves a graphql playground at [http://127.0.0.1:5000/graphql](http://127.0.0.1:5000/graphql)
- `data_manager` runs on port 5000

```
# cd into directory
cd chroma/app

# Create the virtual environment.
python3 -m venv chroma_env

# load it in
source chroma_env/bin/activate

# install dependencies
pip install -r requirements.txt

# set up db if chroma.db doesn not exist
# python
# from main import db
# db.create_all()
# exit()
# The pip bundled flask app now handles the DB creation through checking to see if the DB exists at runtime, and if not, it creates it.

# run the app, development gives you hot reloading
uvicorn app:app --reload --host '::'

# Verify it works
open http://127.0.0.1:8000/graphql
# if the frontend has been built - it will be served automatically at the root path: http://127.0.0.1:8000
```

The pip bundled flask app installs these above dependencies through `setup.cfg`. 

### The pip package
`pip install chroma-core` (or whatever we end up naming it)
- We use Gorilla for monkey-patching

Run the example to see it in action! (make sure to have the backend running)

This uses `gorilla` for monkey patching.
```
# cd into directory
cd pip_package

# build pip package
pip install .
#  pip install . --use-feature=in-tree-build builds it locally which can be convenient for looking in it

# run the example
python examples/save.py
```

Building for release (WIP instructions. These are basically right, but need further testing to verify they are 100%.)
```
# manually build the react app (for now)
cd chroma-ui
yarn build
# this automatically copies a built version of the react app into the backend dir, the backend will serve up the react app

# build sdist and bdist
# BEWARE of caching! you might want to remove dist and the egg-info folder first
rm -rf dist && rm -rf chroma.egg-info && python -m build

# rm -rf dist && rm -rf chroma.egg-info && python -m build && cd dist && tar -xzf chroma-0.0.1.tar.gz && open chroma-0.0.1 && cd ..
# this will drop the files in a dist folder

# install twince to upload
pip install twine

# upload to test pypi (will ask for credentials)
python -m twine upload -repository testpypi dist/*

# uninstall locally
pip uninstall package_name

# test via test pypi
pip install -i https://test.pypi.org/simple package_name

# upload to prod pypi (will ask for credentials)
python -m twine upload -repository pypi dist/*
```

### Sqlite
Sqlite saves it's db to `chroma.db` in the app backend directory.

You can view this directly in terminal if you want to. For example:
```
cd chroma/app_backend
sqlite3
.open chroma_app.db
select * from datapoint;
.exit
```

# TODOs
- Test setup on linux

# Reference URLs
Tutorials I referenced lashing this up:
- https://www.twilio.com/blog/graphql-api-python-flask-ariadne
- https://blog.sethcorker.com/how-to-create-a-react-flask-graphql-project/
- https://www.youtube.com/watch?v=JkeNVaiUq_c
- https://gorilla.readthedocs.io/en/latest/tutorial.html
- https://github.com/mlflow/mlflow/blob/adb0a1142f90ad2832df643cb7b13e1ef5d5c536/tests/utils/test_gorilla.py#L40


# Test flow for pip package
```
cd chroma
rm -rf dist && rm -rf chroma.egg-info && python -m build && cd dist && tar -xzf chroma-0.0.1.tar.gz && cd ..
cd ..
mkdir fresh_test
cd fresh_test
# grab the example script and pull it in
pip uninstall -y chroma && pip install ../chroma/dist/chroma-0.0.1.tar.gz
chroma application run
# run an example script like the one in the examples folder
```

# Productionizing
- Add CLI
- Support multiple databases
- CI
- Release semver
- Docs (autogenerated from python?)
- Telemetry, bug reporting?
- Cloud deployment
