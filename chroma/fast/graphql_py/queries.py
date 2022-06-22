import strawberry
import models
import base64

from typing import Optional, List
from graphql_py.types import Embedding, Projection, Dataset, get_embeddings
from strawberry.dataloader import DataLoader
from sqlalchemy import select
from strawberry import UNSET

@strawberry.type
class Query:
    embeddings: List[Embedding] = strawberry.field(resolver=get_embeddings)

    @strawberry.field
    async def projections(self) -> list[Projection]:
        async with models.get_session() as s:
            sql = select(models.Projection).order_by(models.Projection.name)
            db_projection = (await s.execute(sql)).scalars().unique().all()
        return [Projection.marshal(projection) for projection in db_projection]
    
    @strawberry.field
    async def projection(self, id: strawberry.ID) -> Projection:
        async with models.get_session() as s:
            sql = select(models.Projection).where(models.Projection.id == id)
            db_tasks = await s.execute(sql)
            val = db_tasks.scalars().first()
        return Projection.marshal(val)  

    # @strawberry.field
    # async def embeddings(self) -> list[Embedding]:
    #     async with models.get_session() as s:
    #         sql = select(models.Embedding).order_by(models.Embedding.name)
    #         db_embeddings = (await s.execute(sql)).scalars().unique().all()
    #     return [Embedding.marshal(loc) for loc in db_embeddings]

    @strawberry.field
    async def datasets(self) -> list[Dataset]:
        async with models.get_session() as s:
            sql = select(models.Dataset).order_by(models.Dataset.name)
            db_datasets = (await s.execute(sql)).scalars().unique().all()
        return [Dataset.marshal(loc) for loc in db_datasets]
    
    @strawberry.field
    async def dataset(self, id: strawberry.ID) -> Dataset:
        async with models.get_session() as s:
            sql = select(models.Dataset).where(models.Dataset.id == id)
            db_tasks = await s.execute(sql)
            val = db_tasks.scalars().first()
        return Dataset.marshal(val) 