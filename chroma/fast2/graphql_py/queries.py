import strawberry
import models
import base64

from typing import Optional, List
from graphql_py.types import (
    Embedding, 
    EmbeddingSet, 
    ProjectionSet, 
    Projection,
    get_embeddings
 ) 
from strawberry.dataloader import DataLoader
from sqlalchemy import select
from strawberry import UNSET

@strawberry.type
class Query:
   
    # EmbeddingSet
    @strawberry.field
    async def embedding_sets(self) -> list[EmbeddingSet]:
        async with models.get_session() as s:
            sql = select(models.EmbeddingSet)
            result = (await s.execute(sql)).scalars().unique().all()
        return [EmbeddingSet.marshal(loc) for loc in result]

    @strawberry.field
    async def embedding_set(self, id: strawberry.ID) -> EmbeddingSet:
        async with models.get_session() as s:
            sql = select(models.EmbeddingSet).where(models.EmbeddingSet.id == id)
            val = (await s.execute(sql)).scalars().first()
        return EmbeddingSet.marshal(val)  

    # ProjectionSet
    @strawberry.field
    async def projection_sets(self) -> list[ProjectionSet]:
        async with models.get_session() as s:
            sql = select(models.ProjectionSet)
            result = (await s.execute(sql)).scalars().unique().all()
        return [ProjectionSet.marshal(loc) for loc in result]

    @strawberry.field
    async def projection_set(self, id: strawberry.ID) -> ProjectionSet:
        async with models.get_session() as s:
            sql = select(models.ProjectionSet).where(models.ProjectionSet.id == id)
            val = (await s.execute(sql)).scalars().first()
        return ProjectionSet.marshal(val)  

    # Projection
    @strawberry.field
    async def projections(self) -> list[Projection]:
        async with models.get_session() as s:
            sql = select(models.Projection)
            db_projection = (await s.execute(sql)).scalars().unique().all()
        return [Projection.marshal(projection) for projection in db_projection]
    
    @strawberry.field
    async def projection(self, id: strawberry.ID) -> Projection:
        async with models.get_session() as s:
            sql = select(models.Projection).where(models.Projection.id == id)
            val = (await s.execute(sql)).scalars().first()
        return Projection.marshal(val)  

    # Embedding
    @strawberry.field
    async def embeddings(self) -> list[Embedding]:
        async with models.get_session() as s:
            sql = select(models.Embedding)
            db_embeddings = (await s.execute(sql)).scalars().unique().all()
        return [Embedding.marshal(loc) for loc in db_embeddings]

    @strawberry.field
    async def embedding(self, id: strawberry.ID) -> Embedding:
        async with models.get_session() as s:
            sql = select(models.Embedding).where(models.Embedding.id == id)
            db_tasks = await s.execute(sql)
            val = db_tasks.scalars().first()
        return Embedding.marshal(val)  
        

    # pagination
    embeddings_by_page: List[Embedding] = strawberry.field(resolver=get_embeddings)