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

# Getting up and running
You will need
- `Docker`
- `Python 3.9`

```
# get the code
git clone git@github.com:chroma-core/chroma.git
cd chroma

# install python deps and js deps
make install

# run the service
make run

# or run via our sdk
chroma application run

# then you may want to set up background jobs running via celery

# run rabbitmq
docker run -d --name some-rabbit -p 4369:4369 -p 5671:5671 -p 5672:5672 -p 15672:15672 rabbitmq:3

# run celery from within the app folder
celery -A tasks.celery worker --loglevel=info

```

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
- uvirocn (webserver)
- FastAPI
- Strawberry (graphql)
- Alembic (migrations)
- Sqlite (db)

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
# currently python deps are maintained here and in the Pipfile
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
- https://blog.logrocket.com/using-graphql-strawberry-fastapi-next-js/
- https://kimsereylam.com/sqlalchemy/2019/10/18/get-started-with-alembic.html
- https://medium.com/thelorry-product-tech-data/celery-asynchronous-task-queue-with-fastapi-flower-monitoring-tool-e7135bd0479f

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

# New commands
Strawberry relies on `python 3.9` so we will need that. 

This will export the schema from the graphql backend, it's not especially helpful to us though.
`strawberry export-schema package.module:schema > schema.graphql`

# Adding a new field. 

Say you want to add a new field... like adding metadata to inference. You want to be able to write to that field via the python sdk and read it in the React frontend. Here is what you want to do step-by-step. 

1. Add the field to `models.py` - these are the sqlalchemy definitions that represent our db schema. (If you are trying to preserve production data, don't do this and make a migration with alembic instead). Then run `python models.py` to drop the existing db and set up a fresh one with the schema you want. For adding one-to-one, one-to-many, or many-to-many, we have examples of all of those in our data model in place now to glean from. 
2. Add the field to the class in the `strawberry` `types.py` definitions. `metadata` is not a relationship, but if it was you would also want to add a loader that helps minimize on the number of transactions. You can see examples of that in many of the one-to-many and many-to-many examples. 
3. For adding metadata, you don't have to update `qeuries.py`, but if you want to write to it, you will need to update the mutations for creating and updates. In `mutations.py` add the field to the create and update methods and make sure to add them to the object creating/update. 
4. Then we want to hook this new stuff up to the python sdk so we can use it in our python projects. Look at `sdk/api/queries` and `sdk/api/mutations`. In the queries, if you want to fetch that field, you will want to add it to the default `inferences` and `inference` get queries. You probably don't need to update the mutation because we are passing in a type that is defined by our backend. 
5. That being said, you will want to update `chroma_manager.py` to pass the correct fields down to that mutation! Again, look for the create and update methods especially for the field/model you are updating. 
6. Lastly you can optionally surface this via our rough cli in `cli/sdk.py` if you want to. 
7. Now to the frontend. Inside `chroma-ui`, `src/graphql/operations.graphql` - you can see a bunch of queries and mutations that we want `urql` to generate hooks for us. If you want to, add your fields to those queries/mutations. Then run `npm run codegen` to create the hooks. If it fails, it is probably right and it will tell you what you need to fix. 
8. Done! 

# running background jobs

1. `docker run -d --name some-rabbit -p 4369:4369 -p 5671:5671 -p 5672:5672 -p 15672:15672 rabbitmq:3` will run the rabbitmq service that sends messages from the fastapi app to the celery tasks. 
2. `celery -A tasks.celery worker --loglevel=info` runs the celery service for processing offline jobs.
3. Now commands can take a `.delay` to move them to a background queue. 


