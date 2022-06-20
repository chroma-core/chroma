import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy import Column, ForeignKey, Integer, String, Text, DateTime, Float
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func

Base = declarative_base()

class EmbeddingSet(Base):
    __tablename__ = "embedding_sets"
    id: int = Column(Integer, primary_key=True, index=True)
    embeddings: list["Embedding"] = relationship("Embedding", lazy="joined", back_populates="embedding_set") # has_many embeddings
    projection_sets: list["ProjectionSet"] = relationship("ProjectionSet", lazy="joined", back_populates="embedding_set") # has_many projection_sets

class ProjectionSet(Base):
    __tablename__ = "projection_sets"
    id: int = Column(Integer, primary_key=True, index=True)
    # belongs_to embedding_set
    embedding_set_id: Optional[int] = Column(Integer, ForeignKey(EmbeddingSet.id), nullable=True)
    embedding_set: Optional[EmbeddingSet] = relationship(EmbeddingSet, lazy="joined", back_populates="projection_sets")
    # has_many projections
    projections: list["Projection"] = relationship("Projection", lazy="joined", back_populates="projection_set")

class Embedding(Base):
    __tablename__ = "embeddings"
    id: int = Column(Integer, primary_key=True, index=True)

    # specific
    # data = Column(Text)
    # input_identifier = Column(Text)
    # inference_identifier = Column(Text)
    # label = Column(Text)

    # # has_many projections
    projections: list["Projection"] = relationship("Projection", lazy="joined", back_populates="embedding")

    # # belongs_to embedding_set
    embedding_set_id: Optional[int] = Column(Integer, ForeignKey(EmbeddingSet.id), nullable=True)
    embedding_set: Optional[EmbeddingSet] = relationship(EmbeddingSet, lazy="joined", back_populates="embeddings")


class Projection(Base):
    __tablename__ = "projections"
    
    # generic
    id: int = Column(Integer, primary_key=True, index=True)
    # created_at = Column(DateTime(timezone=True), server_default=func.now())
    # updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # specific: has position, x, y
    # x: float = Column(Float)
    # y: float = Column(Float)

    # belongs_to embedding
    embedding_id: Optional[int] = Column(Integer, ForeignKey(Embedding.id), nullable=True)
    embedding: Optional[Embedding] = relationship(Embedding, lazy="joined", back_populates="projections")

    # belongs_to projection_set
    projection_set_id: Optional[int] = Column(Integer, ForeignKey(ProjectionSet.id), nullable=True)
    projection_set: Optional[ProjectionSet] = relationship(ProjectionSet, lazy="joined", back_populates="projections")


engine = create_async_engine(
    "sqlite+aiosqlite:///./fastapi-db.db", connect_args={"check_same_thread": False}
)

async_session = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)

@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session() as session:
        async with session.begin():
            try:
                yield session
            finally:
                await session.close()

async def _async_main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()


if __name__ == "__main__":
    print("Dropping and creating tables")
    asyncio.run(_async_main())
    print("Done.")