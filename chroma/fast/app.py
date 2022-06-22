import strawberry

from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from strawberry.fastapi import GraphQLRouter

from graphql_py.queries import Query
from graphql_py.mutations import Mutation, get_context
from graphql_py.subscriptions import Subscription

from celery_worker import create_order
from celery.result import AsyncResult

from strawberry.subscriptions import GRAPHQL_TRANSPORT_WS_PROTOCOL, GRAPHQL_WS_PROTOCOL

schema = strawberry.Schema(query=Query, mutation=Mutation, subscription=Subscription)
graphql_app = GraphQLRouter(schema, context_getter=get_context)

app = FastAPI(title="AppBackend")
app.include_router(graphql_app, prefix="/graphql")
app.add_middleware(
    CORSMiddleware, allow_headers=["*"], allow_origins=["http://localhost:3000"], allow_methods=["*"]
)

task = create_order.delay("asdf", 5)
task_id = task.task_id
print("CELERY OBJECT " + task_id)

# result = AsyncResult(task_id)
# print(result.get())
# # result = AsyncResult(result.task_id)
# # print(result.task_id)
# # print(result.status)
# # print(results.result)

