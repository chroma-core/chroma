import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy import Column, ForeignKey, Integer, String, Text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()


class Embedding(Base):
    __tablename__ = "embeddings"
    id: int = Column(Integer, primary_key=True, index=True)
    name: str = Column(String, nullable=False, unique=True)
    data: str = Column(Text, nullable=False)
    label: str = Column(Text, nullable=True)
    identifier: str = Column(Text, nullable=True)

    # has_many projections
    projections: list["Projection"] = relationship("Projection", lazy="joined", back_populates="embedding")

class Projection(Base):
    __tablename__ = "projections"
    id: int = Column(Integer, primary_key=True, index=True)
    name: str = Column(String, nullable=False)

    # belongs_to embedding
    embedding_id: Optional[int] = Column(Integer, ForeignKey(Embedding.id), nullable=True)
    embedding: Optional[Embedding] = relationship(Embedding, lazy="joined", back_populates="projections")

class Dataset(Base):
    __tablename__ = "datasets"
    id: int = Column(Integer, primary_key=True, index=True)
    name: str = Column(String, nullable=False)

engine = create_async_engine(
    "sqlite+aiosqlite:///../fastapi-db.db", connect_args={"check_same_thread": False}
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
        # await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()


if __name__ == "__main__":
    print("Dropping and creating tables")
    asyncio.run(_async_main())
    print("Done.")