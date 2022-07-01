import strawberry
import os
from os.path import getsize, isfile
import models
import asyncio, concurrent.futures
from sqlalchemy import select
from sqlalchemy.orm import selectinload, joinedload, noload, subqueryload
import time

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
    pool.submit(asyncio.run, models.create_db()).result()
    print("No DB existed. Created DB.")
else:
    print("DB in place")

schema = strawberry.Schema(query=Query, mutation=Mutation)
graphql_app = GraphQLRouter(schema, context_getter=get_context)

app = FastAPI(title="AppBackend")

@app.get("/test")
async def root():
    async with models.get_session() as s:
        sql = select(models.Project)
        result = (await s.execute(sql)).scalars().unique().all()
    return result

# we go directly to sqlalchemy and skip graphql for fetching projections and their related data
# because it massively cuts down on the time to return data to the DOM, by ~3x! 
@app.get("/projection_set_data/{projection_set_id}")
async def get_projection_set_data(projection_set_id: str):
    async with models.get_session() as s:

        start = time.process_time()
        # benchmarked difference between selectinload (1s), subqueryload (~1.2s), joinedload (~.7) 
        sql = (select(models.ProjectionSet).where(models.ProjectionSet.id == int(projection_set_id))
            .options(joinedload(models.ProjectionSet.projections).load_only("id", "x", "y", "embedding_id")
                .options(joinedload(models.Projection.embedding).load_only("id", "datapoint_id")
                    .options(joinedload(models.Embedding.datapoint)
                        .options(
                            joinedload(models.Datapoint.label), 
                            joinedload(models.Datapoint.resource),
                            joinedload(models.Datapoint.dataset)
                        )
                        .options(joinedload(models.Datapoint.tags)
                            .options(joinedload(models.Tagdatapoint.tag))
                        )
                    )
                )
            )
        )
        val = (await s.execute(sql)).scalars().first()
        elapsedtime = time.process_time() - start
        print("got records in " + str(elapsedtime) + " seconds")

    return val

app.include_router(graphql_app, prefix="/graphql")
app.add_middleware(
    CORSMiddleware, allow_headers=["*"], allow_origins=["http://localhost:3000"], allow_methods=["*"]
)

# only mount the frontend if it is has been built
if os.path.isdir('static/'):
    app.mount("/", StaticFiles(directory="static/", html=True), name="static")
else:
    print("NOTICE: the frontend has not been built into the static directory. Serving frontend from localhost:8000 will be disabled.")