import resource
from termios import ECHOE
import strawberry
import os
from os.path import getsize, isfile
from chroma.app.profile2 import profiled
import models
import asyncio, concurrent.futures
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload, joinedload, noload, subqueryload, load_only
import time
# from ddtrace.contrib.asgi import TraceMiddleware

from typing import Optional, Union, Any

from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from strawberry.extensions import Extension
from strawberry.fastapi import GraphQLRouter
from fastapi.responses import FileResponse

# import ujson
# from starlette.responses import JSONResponse

# class UJSONResponse(JSONResponse):
#     def render(self, content: Any) -> bytes:
#         return ujson.dumps(content, ensure_ascii=False).encode("utf-8")

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
@app.get("/api/projection_set_data/{projection_set_id}")
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

# we go directly to sqlalchemy and skip graphql for fetching projections and their related data
# because it massively cuts down on the time to return data to the DOM 
@app.get("/api/projection_set_data_viewer/{projection_set_id}")
async def get_projection_set_data_viewer(projection_set_id: str):
    print("get_projection_set_data_viewer!")
    async with models.get_session() as s:
        print("get_projection_set_data_viewer models.get_session!" + str(s))
        start = time.process_time()

        sql = (
            select(models.ProjectionSet)
                .where(models.ProjectionSet.id == int(projection_set_id))
                .options(joinedload(models.ProjectionSet.projections)
                    .options(
                        load_only(models.Projection.x, models.Projection.y), 
                        joinedload(models.Projection.embedding)
                            .options(load_only(models.Embedding.id, models.Embedding.datapoint_id))
                        )
                )
        )
        val = (await s.execute(sql)).scalars().first()

        elapsedtime = time.process_time() - start
        print("got projections in " + str(elapsedtime) + " seconds")

    return val

# we go directly to sqlalchemy and skip graphql for fetching projections and their related data
# because it massively cuts down on the time to return data to the DOM, by ~3x! 
@app.get("/api/datapoints_old/{project_id}&page={page_id}")
async def get_datapoints_data_viewer(project_id: str, page_id: int):
    print("get_datapoints_data_viewer for project" + str(project_id) + " and page " + str(page_id))
    
    async with models.get_session() as s:
        print("get_datapoints_data_viewer models.get_session! " + str(s))
        start = time.process_time()

        # page is 0 index
        page_size = 20000
        offset = page_size * page_id 

        sql = (
            select(models.Datapoint)
                .where(models.Datapoint.project_id == int(project_id))
                    .options(
                        load_only(models.Datapoint.id, models.Datapoint.metadata_), 
                        joinedload(models.Datapoint.dataset)
                            .options(load_only(models.Dataset.id, models.Dataset.name)),
                        joinedload(models.Datapoint.resource)
                            .options(load_only(models.Resource.id, models.Resource.uri)),
                        joinedload(models.Datapoint.label)
                            .options(load_only(models.Label.id, models.Label.data)),
                        joinedload(models.Datapoint.inference)
                            .options(load_only(models.Inference.id, models.Inference.data)),
                        joinedload(models.Datapoint.tags)
                            .options(joinedload(models.Tagdatapoint.tag))
                        )
                ).limit(page_size).offset(offset)

        val = {}
        with profiled():
            res = (await s.execute(sql))
        
        val = res.scalars().unique().all()

        elapsedtime = time.process_time() - start
        print("got datapoints in " + str(elapsedtime) + " seconds")

    return val

@app.get("/api/datapoints_count/{project_id}")
async def get_datapoints_data_viewer(project_id: str):
    async with models.get_session() as s:
        query = select(func.count(models.Datapoint.id)).filter(models.Datapoint.project_id == project_id)
        res = (await s.execute(query)).scalar()
    return res

@app.get("/api/projections/{projection_set_id}")
async def get_projections_data_viewer(projection_set_id: str):
    async with models.get_session() as s:
        print("get_projections_data_viewer models.get_session! " + str(s))
        start = time.process_time()

        sql = (select(models.Projection).where(models.ProjectionSet.id == int(projection_set_id))
                    .options(
                        load_only(models.Projection.x, models.Projection.y), 
                        joinedload(models.Projection.embedding)
                            .options(load_only(models.Embedding.id, models.Embedding.datapoint_id))
                        )
        )
        projections = (await s.execute(sql)).scalars().unique().all()

        elapsedtime = time.process_time() - start
        print("got projections in " + str(elapsedtime) + " seconds")

        return {
            'projections': projections
        }


@app.get("/api/datapoints/{project_id}&page={page_id}")#, response_class=UJSONResponse)
async def get_datapoints_data_viewer(project_id: str, page_id: int):
    with profiled():
        async with models.get_session() as s:
            print("get_datapoints_data_viewer models.get_session! " + str(s))
            start = time.process_time()

            page_size = 10000
            offset = page_size * page_id 

            sql = (select(models.Datapoint).where(models.Datapoint.project_id == int(project_id))
                    .options(
                        load_only(
                            models.Datapoint.id, 
                            models.Datapoint.resource_id, 
                            models.Datapoint.dataset_id,
                            models.Datapoint.metadata_
                        )
                    )).limit(page_size).offset(offset)
            datapoints = (await s.execute(sql)).scalars().unique().all()

            datapoint_ids = []
            resource_ids = []
            dataset_list = {}

            for dp in datapoints:
                datapoint_ids.append(dp.id)
                resource_ids.append(dp.id)
                dataset_list[dp.dataset_id] = True # eg {4: True}, use this to prevent dupes
            
            # Labels
            sql = (select(models.Label).filter(models.Label.datapoint_id.in_(datapoint_ids)).options(load_only(models.Label.id, models.Label.data, models.Label.datapoint_id)))
            labels = (await s.execute(sql)).scalars().unique().all()

            # Resources
            sql = (select(models.Resource).filter(models.Resource.id.in_(resource_ids)).options(load_only(models.Resource.id, models.Resource.uri)))
            resources = (await s.execute(sql)).scalars().unique().all()

            # Inferences
            sql = (select(models.Inference).filter(models.Inference.datapoint_id.in_(datapoint_ids)).options(load_only(models.Inference.id, models.Inference.data, models.Inference.datapoint_id)))
            inferences = (await s.execute(sql)).scalars().unique().all()

            # Datasets
            sql = (select(models.Dataset).filter(models.Dataset.id.in_(dataset_list.keys())).options(load_only(models.Dataset.id, models.Dataset.name, models.Dataset.categories)))
            datasets = (await s.execute(sql)).scalars().unique().all()
            
            # Tags
            sql = (select(models.Tagdatapoint).filter(models.Tagdatapoint.right_id.in_(datapoint_ids)).options(joinedload(models.Tagdatapoint.tag)))
            tags = (await s.execute(sql)).scalars().unique().all()

            elapsedtime = time.process_time() - start
            print("got datapoints in " + str(elapsedtime) + " seconds")

        return ({
            'datapoints': datapoints,
            'labels': labels,
            'resources': resources,
            'inferences': inferences,
            'datasets': datasets,
            'tags': tags
        })


app.include_router(graphql_app, prefix="/graphql")
app.add_middleware(
    CORSMiddleware, allow_headers=["*"], allow_origins=["http://localhost:3000"], allow_methods=["*"]
)

# # only mount the frontend if it is has been built
if os.path.isdir('static/'):

    def serve_react_app(app: FastAPI, build_dir: Union[Path, str]) -> FastAPI:
        """Serves a React application in the root directory `/`

        Args:
            app: FastAPI application instance
            build_dir: React build directory (generated by `yarn build` or
                `npm run build`)

        Returns:
            FastAPI: instance with the react application added
        """
        if isinstance(build_dir, str):
            build_dir = Path(build_dir)

        app.mount(
            "/static/",
            StaticFiles(directory=build_dir / "static"),
            name="React App static files",
        )
        templates = Jinja2Templates(directory=build_dir.as_posix())

        @app.get("/manifest.json")
        async def serve_manifest_json():
            """Serve the react app
            `full_path` variable is necessary to serve each possible endpoint with
            `index.html` file in order to be compatible with `react-router-dom
            """
            return FileResponse("static/manifest.json")

        @app.get("/{full_path:path}")
        async def serve_react_app(request: Request, full_path: str):
            """Serve the react app
            `full_path` variable is necessary to serve each possible endpoint with
            `index.html` file in order to be compatible with `react-router-dom
            """
            return templates.TemplateResponse("index.html", {"request": request})

        return app

    path_to_react_app_build_dir = "./static"
    app = serve_react_app(app, path_to_react_app_build_dir)

else:
    print("NOTICE: the frontend has not been built into the static directory. Serving frontend from localhost:8000 will be disabled.")

# app = TraceMiddleware(app)