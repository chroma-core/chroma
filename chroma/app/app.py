import os

import strawberry
import models

import asyncio, concurrent.futures


from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from strawberry.fastapi import GraphQLRouter

from graphql_py.queries import Query
from graphql_py.mutations import Mutation, get_context
from utils import isSQLite3
from api.v1.react_app import serve_react_app
from api.v1.data_viewer import router

if not isSQLite3("chroma.db"):
    # create_db is async, so we have to run it sync this way
    # https://stackoverflow.com/questions/55147976/run-and-wait-for-asynchronous-function-from-a-synchronous-one-using-python-async
    pool = concurrent.futures.ThreadPoolExecutor()
    pool.submit(asyncio.run, models.create_db()).result()
    print("No DB existed. Created DB.")
else:
    print("DB in place")

app = FastAPI(title="AppBackend")

# mount graphql
schema = strawberry.Schema(query=Query, mutation=Mutation)
graphql_app = GraphQLRouter(schema, context_getter=get_context)
app.include_router(graphql_app, prefix="/graphql")

# mount rest
app.include_router(router)

# enable CORS
app.add_middleware(
    CORSMiddleware, allow_headers=["*"], allow_origins=["http://localhost:3000"], allow_methods=["*"]
)

# mount react app at :8000
if os.path.isdir('static/'):
    path_to_react_app_build_dir = "./static"
    app = serve_react_app(app, path_to_react_app_build_dir)
else:
    print("NOTICE: the frontend has not been built into the static directory. Serving frontend from localhost:8000 will be disabled.")
