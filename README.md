# End to end Todo example

This example demonstrates how to use together
- React
- Flask
- Sqlite db
- Pip package including monkey patching
- using Graphql for networking

React, Flask, Sqlite and Graphql were chosen for their simplicity and ubiquity. We can get fancier with tooling when we need to. 

The folder structure is:
- `frontend`: react app
- `backend`: flask app
- `pip_package`: pip package 
- `examples`: example script that uses the pip package

Currently this uses a simple todo list app to demonstrate this. You can 
- list todos in the react app or in the pip package, both powerwered via the graphql flask API
- create a todo via the react app or pip package
- that's it, no update or deleting right now

# Setup 

### The frontend
The frontend uses (see all boilerplate dependencies in frontend/package.json):
- React via Create React App

Right now graphql queries are handwritten. This can be changed over to a number of libraries.

```
# if you need node, follow these instructions https://formulae.brew.sh/formula/node

# requires node version 14.13.1, we can probably use an even newer verison, this was the min version required by our deps
# if you need nvm, follow these instructions https://github.com/nvm-sh/nvm#installing-and-updating
nvm install 14.13.1
 
# if you need yarn, follow these instructions: https://classic.yarnpkg.com/lang/en/docs/install/#mac-stable

# cd into directory
cd frontend

# install dependencies
yarn

# run - this will load http://localhost:3000 in the browser
yarn start
```

### The backend
The backend uses:
- Flask
- Araidne (graphql)

It runs on port 5000. You can view a graphql playgroun at http://127.0.0.1:5000/graphql.

```
# cd into directory
cd backend

# Create the virtual environment.
python3 -m venv todo_api_env

# load it in
source todo_api_env/bin/activate

# install dependencies
pip install flask ariadne flask-sqlalchemy

# run the app, development gives you hot reloading
FLASK_APP=main.py FLASK_ENV=development flask run

# Verify it works
open http://127.0.0.1:5000/graphql
```

### The pip package
This demonstrates how to send data to the flash backend via a library. It also demonstrates how to monkey patch a function, in this case pprint. 

Run the example to see it in action! (make sure to have the backend running)

This uses `gorilla` for monkey patching.
```
# cd into directory
cd pip_package

# build pip package
pip install .

# run the example
cd ../examples
python save_a_todo.py
```

### Sqlite
Sqlite saves it's db to `todo.db` in the backend directory.

You can view this directly in terminal if you want to. 
```
cd backend
sqlite3
.open todo.db
select * from todo;
.exit
```

# Running, once setup

### Running the frontend
`yarn start`

### Running the backend
`FLASK_APP=main.py FLASK_ENV=development flask run`

# Todos
- Test on M1 mac
- Test on linux

# Future work
- make the pip url configurable. right now it is hardcoded
- run the frontend and backend in one command, right now it requires multiple terminal windows which is annoying
- build the frontend and backend into the pip package, right now the pip package does not include them

# Reference URLs
Tutorials I referneced lashing this up:
- https://www.twilio.com/blog/graphql-api-python-flask-ariadne
- https://blog.sethcorker.com/how-to-create-a-react-flask-graphql-project/
- https://www.youtube.com/watch?v=v4bkJef4W94 
- https://gorilla.readthedocs.io/en/latest/tutorial.html
- https://github.com/mlflow/mlflow/blob/adb0a1142f90ad2832df643cb7b13e1ef5d5c536/tests/utils/test_gorilla.py#L40
