import strawberry
import models

from typing import Optional, List
from graphql_py.types import Embedding, Projection, Dataset, AddProjectionResponse, AddEmbeddingResponse, EmbeddingExists, EmbeddingNotFound, EmbeddingNameMissing, AddDatasetResponse
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
    async def add_projection(self, name: str, embedding_name: Optional[str]) -> AddProjectionResponse:
        async with models.get_session() as s:
            db_embedding = None
            if embedding_name:
                sql = select(models.Embedding).where(models.Embedding.name == embedding_name)
                db_embedding = (await s.execute(sql)).scalars().first()
                if not db_embedding:
                    return EmbeddingNotFound()
            else:
                return EmbeddingNameMissing()
            db_projection = models.Projection(name=name, embedding=db_embedding)
            s.add(db_projection)
            await s.commit()
        return Projection.marshal(db_projection)

    @strawberry.mutation
    async def add_embedding(self, name: str, data: str) -> AddEmbeddingResponse:
        async with models.get_session() as s:
            sql = select(models.Embedding).where(models.Embedding.name == name)
            existing_db_embedding = (await s.execute(sql)).first()
            if existing_db_embedding is not None:
                return EmbeddingExists()
            db_embedding = models.Embedding(name=name, data=data)
            s.add(db_embedding)
            await s.commit()
        return Embedding.marshal(db_embedding)

    @strawberry.mutation
    async def add_embeddings(self, embeddings: EmbeddingsInput) -> list[Embedding]:
        async with models.get_session() as s:
            objects = [models.Embedding(name=em.name, data=em.data) for em in embeddings.embeddings]
            s.add_all(objects)
            await s.commit()
        return [Embedding.marshal(loc) for loc in objects]

    @strawberry.mutation
    async def add_dataset(self, name: str) -> AddDatasetResponse:
        async with models.get_session() as s:
            db_dataset = models.Dataset(name=name)
            s.add(db_dataset)
            await s.commit()
        return Dataset.marshal(db_dataset)


async def load_projections_by_embedding(keys: list) -> list[Projection]:
    async with models.get_session() as s:
        all_queries = [select(models.Projection).where(models.Projection.embedding_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
        print(keys, data)
    return data


async def load_embedding_by_projection(keys: list) -> list[Projection]:
    async with models.get_session() as s:
        sql = select(models.Embedding).where(models.Embedding.id in keys)
        data = (await s.execute(sql)).scalars().unique().all()
    if not data:
        data.append([])
    return data


async def get_context() -> dict:
    return {
        "embedding_by_projection": DataLoader(load_fn=load_embedding_by_projection),
        "projections_by_embedding": DataLoader(load_fn=load_projections_by_embedding),
    }
