import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy import Column, ForeignKey, Integer, String, Text, DateTime, Float, Table
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, backref, scoped_session
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.sql import func
from sqlalchemy import create_engine

# Note to developer
# - has_many should have lazy="select"
# - belongs_to should have lazy="joined"
# there are also subquery and dynamic options, but i dont know how those work

Base = declarative_base()

class Project(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "projects"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    name = Column(String)
    # [x] has many datasets, has many model architectures
    datasets: list["Dataset"] = relationship("Dataset", lazy="select", back_populates="project")
    datapoints: list["Datapoint"] = relationship("Datapoint", lazy="select", back_populates="project")
    projection_sets: list["ProjectionSet"] = relationship("ProjectionSet", lazy="select", back_populates="project")
    model_architectures: list["ModelArchitecture"] = relationship("ModelArchitecture", lazy="select", back_populates="project")

class Dataset(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "datasets"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    name = Column(String)
    # has many slices, has many datapoints, belongs_to project
    slices: list["Slice"] = relationship("Slice", lazy="select", back_populates="dataset")
    datapoints: list["Datapoint"] = relationship("Datapoint", lazy="select", back_populates="dataset")
    project_id: Optional[int] = Column(Integer, ForeignKey(Project.id), nullable=True)
    project: Optional[Project] = relationship("Project", lazy="select", back_populates="datasets")
    embedding_sets: list["EmbeddingSet"] = relationship("EmbeddingSet", lazy="select", back_populates="dataset") # has_many embedding_sets

class Resource(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "resources"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    uri = Column(Text)
    # has many datapoints
    datapoints: list["Datapoint"] = relationship("Datapoint", lazy="select", back_populates="resource")

# assocation table
class Slice_Datapoint(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = 'slice_datasets'
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    slice_id = Column(Integer, ForeignKey('slices.id'), primary_key=True)
    datapoint_id = Column(Integer, ForeignKey('datapoints.id'), primary_key=True)

class Slice(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "slices"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    name = Column(String)
    # habtm datapoints, belongs_to dataset, has_many trained_models
    dataset_id: Optional[int] = Column(Integer, ForeignKey(Dataset.id), nullable=True)
    dataset: Optional[Dataset] = relationship("Dataset", lazy="joined", back_populates="slices")
    datapoints = relationship('Slice_Datapoint', backref='slice',  primaryjoin=id == Slice_Datapoint.slice_id)
    trained_models: list["TrainedModel"] = relationship("TrainedModel", lazy="select", back_populates="slice")

class Tagdatapoint(Base):
    __tablename__ = "tagdatapoints"
    left_id = Column(ForeignKey("tags.id"), primary_key=True)
    right_id = Column(ForeignKey("datapoints.id"), primary_key=True)
    tag = relationship("Tag", back_populates="datapoints")
    datapoint = relationship("Datapoint", back_populates="tags")

class Datapoint(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "datapoints"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    # belongs to dataset, has one resource, has one label, has one inference, habtm tags, habtm slices
    dataset_id: Optional[int] = Column(Integer, ForeignKey(Dataset.id), nullable=True)
    dataset: Optional[Dataset] = relationship("Dataset", lazy="joined", back_populates="datapoints")
    resource_id: Optional[int] = Column(Integer, ForeignKey(Resource.id), nullable=True)
    resource: Optional[Resource] = relationship("Resource", lazy="select", back_populates="datapoints")
    slices = relationship('Slice_Datapoint', backref='datapoint', primaryjoin=id == Slice_Datapoint.datapoint_id)
    label = relationship("Label", back_populates="datapoint", uselist=False)
    inference = relationship("Inference", back_populates="datapoint", uselist=False)
    tags = relationship("Tagdatapoint", back_populates="datapoint")
    embeddings: list["Embedding"] = relationship("Embedding", lazy="select", back_populates="datapoint")
    metadata_ = Column("metadata", Integer)
    project_id: Optional[int] = Column(Integer, ForeignKey(Project.id), nullable=True)
    project: Optional[Project] = relationship("Project", lazy="joined", back_populates="datapoints")

class Tag(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "tags"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    name = Column(String, unique=True)
    # habtm datapoints
    datapoints = relationship("Tagdatapoint", back_populates="tag")

class Label(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "labels"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    data = Column(Text)
    # has_one datapoint
    datapoint_id: Optional[int] = Column(Integer, ForeignKey(Datapoint.id), nullable=True)
    datapoint = relationship("Datapoint", back_populates="label", uselist=False)

class Inference(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "inferences"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    # has one trained model, has_one datapoint
    datapoint_id: Optional[int] = Column(Integer, ForeignKey(Datapoint.id), nullable=True)
    datapoint = relationship("Datapoint", back_populates="inference", uselist=False)
    trained_model_id: Optional[int] = Column(Integer, ForeignKey("trained_models.id"), nullable=True)
    trained_model = relationship("TrainedModel", back_populates="inferences", uselist=False)

class ModelArchitecture(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "model_architectures"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    name = Column(String)
    # has many trained models, belongs_to project
    trained_models: list["TrainedModel"] = relationship("TrainedModel", lazy="select", back_populates="model_architecture")
    project_id: Optional[int] = Column(Integer, ForeignKey(Project.id), nullable=True)
    project: Optional[Project] = relationship("Project", lazy="joined", back_populates="model_architectures")

class TrainedModel(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "trained_models"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    # has many layersets, belongs_to modelarchitecture, has one slice
    inferences: list["Inference"] = relationship("Inference", lazy="select", back_populates="trained_model")
    slice_id: Optional[int] = Column(Integer, ForeignKey("slices.id"), nullable=True)
    slice = relationship("Slice", back_populates="trained_models", uselist=False)
    layer_sets: list["LayerSet"] = relationship("LayerSet", lazy="select", back_populates="trained_model")
    model_architecture_id: Optional[int] = Column(Integer, ForeignKey(ModelArchitecture.id), nullable=True)
    model_architecture: Optional[ModelArchitecture] = relationship("ModelArchitecture", lazy="joined", back_populates="trained_models")

class LayerSet(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "layer_sets"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    # has many layers, belongs_to trained model
    layers: list["Layer"] = relationship("Layer", lazy="select", back_populates="layer_set")
    trained_model_id: Optional[int] = Column(Integer, ForeignKey(TrainedModel.id), nullable=True)
    trained_model: Optional[TrainedModel] = relationship("TrainedModel", lazy="joined", back_populates="layer_sets")

class Layer(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "layers"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    # has many embeddings, belongs_to layerset
    embeddings: list["Embedding"] = relationship("Embedding", lazy="select", back_populates="layer")
    layer_set_id: Optional[int] = Column(Integer, ForeignKey(LayerSet.id), nullable=True)
    layer_set: Optional[LayerSet] = relationship("LayerSet", lazy="joined", back_populates="layers")

class Projector(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "projectors"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    # has many embeddings
    # has many projections
    # embeddings: list["Embedding"] = relationship("Embedding", lazy="select", back_populates="projector")
    # projections: list["Projection"] = relationship("Projection", lazy="select", back_populates="projector")

class Job(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "jobs"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    name = Column(String)

class EmbeddingSet(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "embedding_sets"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    # has one slice, has one trained model
    embeddings: list["Embedding"] = relationship("Embedding", lazy="select", back_populates="embedding_set") # has_many embeddings
    projection_sets: list["ProjectionSet"] = relationship("ProjectionSet", lazy="select", back_populates="embedding_set") # has_many projection_sets

    dataset_id: Optional[int] = Column(Integer, ForeignKey(Dataset.id), nullable=True)
    dataset: Optional[Dataset] = relationship("Dataset", lazy="joined", back_populates="embedding_sets")

class ProjectionSet(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "projection_sets"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    # belongs_to embedding_set, has_many projections
    embedding_set_id: Optional[int] = Column(Integer, ForeignKey(EmbeddingSet.id), nullable=True)
    embedding_set: Optional[EmbeddingSet] = relationship("EmbeddingSet", lazy="select", back_populates="projection_sets")
    projections: list["Projection"] = relationship("Projection", lazy="select", back_populates="projection_set")

    project_id: Optional[int] = Column(Integer, ForeignKey(Project.id), nullable=True)
    project: Optional[Project] = relationship("Project", lazy="select", back_populates="projection_sets")

class Embedding(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "embeddings"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    # specific
    data = Column(Text)
    input_identifier = Column(Text) # resource
    inference_identifier = Column(Text)
    label = Column(Text) # label
    # has_many projections, belongs_to embedding_set, belongs_to layer
    projections: list["Projection"] = relationship("Projection", lazy="select", back_populates="embedding")
    embedding_set_id: Optional[int] = Column(Integer, ForeignKey(EmbeddingSet.id), nullable=True)
    embedding_set: Optional[EmbeddingSet] = relationship("EmbeddingSet", lazy="select", back_populates="embeddings")
    layer_id: Optional[int] = Column(Integer, ForeignKey(Layer.id), nullable=True)
    layer: Optional[Layer] = relationship("Layer", lazy="select", back_populates="embeddings")
    datapoint_id: Optional[int] = Column(Integer, ForeignKey(Datapoint.id), nullable=True)
    datapoint: Optional[Datapoint] = relationship("Datapoint", lazy="select", back_populates="embeddings")

class Projection(Base):
    __mapper_args__ = {'eager_defaults': True}
    __tablename__ = "projections"
    id: int = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    x: float = Column(Float)
    y: float = Column(Float)
    # belongs_to embedding, belongs_to projection_set
    embedding_id: Optional[int] = Column(Integer, ForeignKey(Embedding.id), nullable=True)
    embedding: Optional[Embedding] = relationship("Embedding", lazy="select", back_populates="projections")
    projection_set_id: Optional[int] = Column(Integer, ForeignKey(ProjectionSet.id), nullable=True)
    projection_set: Optional[ProjectionSet] = relationship("ProjectionSet", lazy="select", back_populates="projections")

engine = create_async_engine(
    "sqlite+aiosqlite:///./chroma.db", connect_args={"check_same_thread": False}
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