import strawberry
import models

from typing import Optional, List
from graphql_py.types import (
    Embedding, 
    AddEmbeddingResponse,
    EmbeddingSet,
    AddEmbeddingSetResponse,
    ProjectionSet,
    AddProjectionSetResponse, 
    Projection,
    AddProjectionResponse,
    EmbeddingExists, 
    EmbeddingNotFound, 
    EmbeddingNameMissing
)
from strawberry.dataloader import DataLoader
from sqlalchemy import select

@strawberry.input
class EmbeddingInput:
    name: str
    data: str

@strawberry.input
class EmbeddingsInput:
    embeddings: list[EmbeddingInput]

@strawberry.type
class Mutation:

    @strawberry.mutation
    async def add_projection(self, embedding_id: int, projection_set_id: int) -> AddProjectionResponse:
        async with models.get_session() as s:

            sql = select(models.Embedding).where(models.Embedding.id == embedding_id)
            embedding = (await s.execute(sql)).scalars().first()

            sql = select(models.ProjectionSet).where(models.ProjectionSet.id == projection_set_id)
            projection_set = (await s.execute(sql)).scalars().first()
            
            res = models.Projection(embedding=embedding, projection_set=projection_set)
            s.add(res)
            await s.commit()
        return Projection.marshal(res)

    @strawberry.mutation
    async def add_projection_set(self, embedding_set_id: int) -> AddProjectionSetResponse:
        async with models.get_session() as s:

            sql = select(models.EmbeddingSet).where(models.EmbeddingSet.id == embedding_set_id)
            embedding_set = (await s.execute(sql)).scalars().first()

            res = models.ProjectionSet(embedding_set=embedding_set)
            s.add(res)
            await s.commit()
        return ProjectionSet.marshal(res)

    @strawberry.mutation
    async def add_embedding_set(self) -> AddEmbeddingSetResponse:
        async with models.get_session() as s:
            res = models.EmbeddingSet()
            s.add(res)
            await s.commit()
        return EmbeddingSet.marshal(res)

    @strawberry.mutation
    async def add_embedding(self, embedding_set_id: int) -> AddEmbeddingResponse:
        async with models.get_session() as s:

            sql = select(models.EmbeddingSet).where(models.EmbeddingSet.id == embedding_set_id)
            embedding_set = (await s.execute(sql)).scalars().first()

            db_embedding = models.Embedding(embedding_set=embedding_set)
            s.add(db_embedding)
            await s.commit()
        return Embedding.marshal(db_embedding)

    # batch query example
    @strawberry.mutation
    async def add_embeddings(self, embeddings: EmbeddingsInput) -> list[Embedding]:
        async with models.get_session() as s:
            objects = [models.Embedding(name=em.name, data=em.data) for em in embeddings.embeddings]
            s.add_all(objects)
            await s.commit()
        return [Embedding.marshal(loc) for loc in objects]


async def load_projections_by_embedding(keys: list) -> list[Projection]:
    async with models.get_session() as s:
        all_queries = [select(models.Projection).where(models.Projection.embedding_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def load_projection_sets_by_embedding_set(keys: list) -> list[ProjectionSet]:
    async with models.get_session() as s:
        all_queries = [select(models.ProjectionSet).where(models.ProjectionSet.embedding_set_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def load_embeddings_by_embedding_set(keys: list) -> list[Embedding]:
    async with models.get_session() as s:
        all_queries = [select(models.Embedding).where(models.Embedding.embedding_set_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def load_projections_by_projection_set(keys: list) -> list[Projection]:
    async with models.get_session() as s:
        all_queries = [select(models.Projection).where(models.Projection.projection_set_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def get_context() -> dict:
    return {
        "projections_by_embedding": DataLoader(load_fn=load_projections_by_embedding),
        "projection_sets_by_embedding_set": DataLoader(load_fn=load_projection_sets_by_embedding_set),
        "embeddings_by_embedding_set": DataLoader(load_fn=load_embeddings_by_embedding_set),
        "projections_by_projection_set": DataLoader(load_fn=load_projections_by_projection_set),
    }

# async def load_embedding_by_projection(keys: list) -> list[Projection]:
#     async with models.get_session() as s:
#         sql = select(models.Embedding).where(models.Embedding.id in keys)
#         data = (await s.execute(sql)).scalars().unique().all()
#     if not data:
#         data.append([])
#     return data