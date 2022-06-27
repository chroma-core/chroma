import strawberry
import os
from os.path import getsize, isfile
from models import create_db
import asyncio, concurrent.futures

from typing import Optional

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from strawberry.extensions import Extension
from strawberry.fastapi import GraphQLRouter

from graphql_py.queries import Query
from graphql_py.mutations import Mutation, get_context
# from graphql_py.subscriptions import Subscription
from strawberry.subscriptions import GRAPHQL_TRANSPORT_WS_PROTOCOL, GRAPHQL_WS_PROTOCOL

def isSQLite3(filename):
    if not isfile(filename):
        return False
    if getsize(filename) < 100:  # SQLite database file header is 100 bytes
        return False

    with open(filename, "rb") as fd:
        header = fd.read(100)

    return header[:16].decode("utf-8") == "SQLite format 3\x00"

if not isSQLite3("chroma.db"):
    # create_db is async, so we have to run it sync this way
    # https://stackoverflow.com/questions/55147976/run-and-wait-for-asynchronous-function-from-a-synchronous-one-using-python-async
    pool = concurrent.futures.ThreadPoolExecutor()
    pool.submit(asyncio.run, create_db()).result()
    print("No DB existed. Created DB.")
else:
    print("DB in place")

schema = strawberry.Schema(query=Query, mutation=Mutation)
graphql_app = GraphQLRouter(schema, context_getter=get_context)

app = FastAPI(title="AppBackend")
app.include_router(graphql_app, prefix="/graphql")
app.add_middleware(
    CORSMiddleware, allow_headers=["*"], allow_origins=["http://localhost:3000"], allow_methods=["*"]
)

app.mount("/", StaticFiles(directory="static/", html=True), name="static")
