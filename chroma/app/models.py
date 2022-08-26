import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy import Column, ForeignKey, Integer, String, Text, DateTime, Float, UniqueConstraint
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func

# Note to developer
# - use lazy="select" to prevent greedily fetching relationships, this is important for performance reasons at our scale

Base = declarative_base()

class BaseModel(object):
    """
    - __mapper_args__ = {"eager_defaults": True} ensures our timestamps get written correctly
    - every table should have id, created_at, and updated_at
    """
    __mapper_args__ = {"eager_defaults": True}
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class Project(BaseModel, Base):
    """
    Project: a project contains all the relavent datapoints, datasets, etc relevant to a particular goal/project
    - has many datasets
    - has many projection_sets
    """
    __tablename__ = "projects"

    # attributes
    name = Column(String)
    
    # relationships
    datasets: list["Dataset"] = relationship("Dataset", lazy="select", back_populates="project")
    datapoints: list["Datapoint"] = relationship("Datapoint", lazy="select", back_populates="project")
    projection_sets: list["ProjectionSet"] = relationship("ProjectionSet", lazy="select", back_populates="project")


class Dataset(BaseModel, Base):
    """
    Dataset: set of datapoints
    - has many datapoints
    - has many embedding_sets
    - belongs to a project
    """
    __tablename__ = "datasets"
    
    # attributes
    name = Column(String)
    categories = Column(Text)

    # relationships
    datapoints: list["Datapoint"] = relationship("Datapoint", lazy="select", back_populates="dataset")
    project_id: Optional[int] = Column(Integer, ForeignKey(Project.id), nullable=True)
    project: Optional[Project] = relationship("Project", lazy="select", back_populates="datasets")
    embedding_sets: list["EmbeddingSet"] = relationship("EmbeddingSet", lazy="select", back_populates="dataset")  


class Resource(BaseModel, Base):
    """
    Resource: points to a file, either on disk, or a URL
    - has many datapoints
    """
    __tablename__ = "resources"

    # attributes
    uri = Column(Text)

    # relationships
    datapoints: list["Datapoint"] = relationship("Datapoint", lazy="select", back_populates="resource")


class Tagdatapoint(Base):
    """
    Tagdatapoint: has and belongs to many mapping table between tags and datapoints
    """
    __tablename__ = "tagdatapoints"
    __table_args__ = (
        UniqueConstraint('left_id', 'right_id', 'target'),
    )
    id: int = Column(Integer, primary_key=True, index=True)
    left_id = Column(ForeignKey("tags.id"))
    right_id = Column(ForeignKey("datapoints.id"))
    tag = relationship("Tag", back_populates="datapoints")
    datapoint = relationship("Datapoint", back_populates="tags")
    target = Column(String, nullable=True)


class Datapoint(BaseModel, Base):
    """
    Datapoint: maps together things used in training, or that come out of inference
    - belongs to project
    - belongs to dataset
    - has one resource
    - has one label
    - has one inference
    - has and belongs to many tags
    - has many embeddings
    """
    __tablename__ = "datapoints"

    # attributes
    metadata_ = Column("metadata", Text)

    # relationships
    project_id: Optional[int] = Column(Integer, ForeignKey(Project.id), nullable=True)
    project: Optional[Project] = relationship("Project", lazy="select", back_populates="datapoints")
    dataset_id: Optional[int] = Column(Integer, ForeignKey(Dataset.id), nullable=True)
    dataset: Optional[Dataset] = relationship("Dataset", lazy="select", back_populates="datapoints")
    resource_id: Optional[int] = Column(Integer, ForeignKey(Resource.id), nullable=True)
    resource: Optional[Resource] = relationship("Resource", lazy="select", back_populates="datapoints")
    label = relationship("Label", back_populates="datapoint", uselist=False)
    inference = relationship("Inference", back_populates="datapoint", uselist=False)
    tags = relationship("Tagdatapoint", back_populates="datapoint")
    embeddings: list["Embedding"] = relationship("Embedding", lazy="select", back_populates="datapoint")


class Tag(BaseModel, Base):
    """
    Tag: semantic string on a datapoint
    - has and belongs to many datapoints
    """
    __tablename__ = "tags"

    # attributes
    name = Column(String, unique=True)

    # relationships
    datapoints = relationship("Tagdatapoint", back_populates="tag")


class Label(BaseModel, Base):
    """
    Label: human annotations on a datapoint
    - has one datapoint
    """
    __tablename__ = "labels"

    # attributes
    data = Column(Text)

    # relationships
    datapoint_id: Optional[int] = Column(Integer, ForeignKey(Datapoint.id), nullable=True)
    datapoint = relationship("Datapoint", back_populates="label", uselist=False)


class Inference(BaseModel, Base):
    """
    Inference: results of inference (annotations) on a datapoint
    - has one datapoint
    """
    __tablename__ = "inferences"

    # attributes
    data = Column(Text)

    # relationships
    datapoint_id: Optional[int] = Column(Integer, ForeignKey(Datapoint.id), nullable=True)
    datapoint = relationship("Datapoint", back_populates="inference", uselist=False)


class Job(BaseModel, Base):
    """
    Job: wrapper around celery job to expose status back to our users
    """
    __tablename__ = "jobs"

    # attributes
    name = Column(String)


class EmbeddingSet(BaseModel, Base):
    """
    EmbeddingSet: a set of embeddings
    - has many embeddings
    - has many projections sets
    - belongs to a dataset
    """
    __tablename__ = "embedding_sets"
    
    # relationships
    embeddings: list["Embedding"] = relationship("Embedding", lazy="select", back_populates="embedding_set") 
    projection_sets: list["ProjectionSet"] = relationship("ProjectionSet", lazy="select", back_populates="embedding_set") 
    dataset_id: Optional[int] = Column(Integer, ForeignKey(Dataset.id), nullable=True)
    dataset: Optional[Dataset] = relationship("Dataset", lazy="select", back_populates="embedding_sets")


class ProjectionSet(BaseModel, Base):
    """
    ProjectionSet: a set of projections
    - belongs to an embedding set
    - has many projections
    - belongs to a project
    """
    __tablename__ = "projection_sets"

    # attributes
    setType = Column(Text)
    
    # relationships
    embedding_set_id: Optional[int] = Column(Integer, ForeignKey(EmbeddingSet.id), nullable=True)
    embedding_set: Optional[EmbeddingSet] = relationship("EmbeddingSet", lazy="select", back_populates="projection_sets")
    projections: list["Projection"] = relationship("Projection", lazy="select", back_populates="projection_set")
    project_id: Optional[int] = Column(Integer, ForeignKey(Project.id), nullable=True)
    project: Optional[Project] = relationship("Project", lazy="select", back_populates="projection_sets")


class Embedding(BaseModel, Base):
    """
    Embedding: the data as pertains to a datapoint at a specific layer of a network
    - has_many projections
    - belongs_to embedding_set
    - belongs_to layer
    """
    __tablename__ = "embeddings"
    
    # attributes
    data = Column(Text)
    
    # relationships
    projections: list["Projection"] = relationship("Projection", lazy="select", back_populates="embedding")
    embedding_set_id: Optional[int] = Column(Integer, ForeignKey(EmbeddingSet.id), nullable=True)
    embedding_set: Optional[EmbeddingSet] = relationship("EmbeddingSet", lazy="select", back_populates="embeddings")
    datapoint_id: Optional[int] = Column(Integer, ForeignKey(Datapoint.id), nullable=True)
    datapoint: Optional[Datapoint] = relationship("Datapoint", lazy="select", back_populates="embeddings")


class Projection(BaseModel, Base):
    """
    Projeciton: a xy projection of an embedding
    - belongs to  an embedding
    - belongs to a projection_set
    """
    __tablename__ = "projections"

    # attributes
    x: float = Column(Float)
    y: float = Column(Float)
    target = Column(Text)

    # relationships
    embedding_id: Optional[int] = Column(Integer, ForeignKey(Embedding.id), nullable=True)
    embedding: Optional[Embedding] = relationship("Embedding", lazy="select", back_populates="projections")
    projection_set_id: Optional[int] = Column(Integer, ForeignKey(ProjectionSet.id), nullable=True)
    projection_set: Optional[ProjectionSet] = relationship("ProjectionSet", lazy="select", back_populates="projections")


engine = create_async_engine(
    "sqlite+aiosqlite:///./chroma.db",
    connect_args={"check_same_thread": False}, #echo=True,
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


async def create_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()


if __name__ == "__main__":
    print("Dropping and creating tables")
    asyncio.run(_async_main())
    print("Done.")
