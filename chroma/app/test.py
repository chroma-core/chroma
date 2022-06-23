import strawberry

from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter

authors: list[str] = []

@strawberry.type
class Query:
    @strawberry.field
    def all_authors(self, info) -> list[str]:
        return authors

@strawberry.type
class Mutation:
    @strawberry.field
    def add_author(name: str) -> str:
        authors.append(name)
        return name

schema = strawberry.Schema(query=Query, mutation=Mutation)

graphql_app = GraphQLRouter(schema)

app = FastAPI()
app.include_router(graphql_app, prefix="/graphql")