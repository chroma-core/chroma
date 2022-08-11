import strawberry
import base64
import models
import datetime
import json

from typing import Optional, List, Generic, TypeVar
from strawberry.types import Info
from strawberry import UNSET
from sqlalchemy import select
from strawberry.scalars import JSON

GenericType = TypeVar("GenericType")


@strawberry.type
class Project:
    id: strawberry.ID
    name: Optional[str]
    created_at: datetime.datetime
    updated_at: datetime.datetime

    # has_many datasets
    @strawberry.field
    async def datasets(self, info: Info) -> list["Dataset"]:
        datasets = await info.context["datasets_by_project"].load(self.id)
        return [Dataset.marshal(dataset) for dataset in datasets]

    # has_many model_architectures
    @strawberry.field
    async def model_architectures(self, info: Info) -> list["ModelArchitecture"]:
        model_architectures = await info.context["model_architectures_by_project"].load(self.id)
        return [
            ModelArchitecture.marshal(model_architecture)
            for model_architecture in model_architectures
        ]

    @classmethod
    def marshal(cls, model: models.Project) -> "Project":
        return cls(
            id=strawberry.ID(str(model.id)),
            name=model.name if model.name else None,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


@strawberry.type
class Dataset:
    id: strawberry.ID
    name: Optional[str]
    created_at: datetime.datetime
    updated_at: datetime.datetime
    project_id: Optional[int]
    categories: Optional[JSON] = None

    # belongs_to project
    @strawberry.field
    async def project(self, info: Info) -> Project:
        async with models.get_session() as s:
            sql = select(models.Project).where(models.Project.id == self.project_id)
            project = (await s.execute(sql)).scalars().first()
        return Project.marshal(project)

    # has_many slices
    @strawberry.field
    async def slices(self, info: Info) -> list["Slice"]:
        slices = await info.context["slices_by_dataset"].load(self.id)
        return [Slice.marshal(slice) for slice in slices]

    # has_many datapoints
    @strawberry.field
    async def datapoints(self, info: Info) -> list["Datapoint"]:
        datapoints = await info.context["datapoints_by_dataset"].load(self.id)
        return [Datapoint.marshal(datapoint) for datapoint in datapoints]

    # habm embedding_sets
    @strawberry.field
    async def embedding_sets(self, info: Info) -> list["EmbeddingSet"]:
        embedding_sets = await info.context["embedding_sets_by_dataset"].load(self.id)
        return [EmbeddingSet.marshal(embedding_set) for embedding_set in embedding_sets]

    @classmethod
    def marshal(cls, model: models.Dataset) -> "Dataset":
        return cls(
            id=strawberry.ID(str(model.id)),
            name=model.name if model.name else None,
            created_at=model.created_at,
            updated_at=model.updated_at,
            project_id=model.project_id,
            categories=json.loads(model.categories) if model.categories else None,
        )


@strawberry.type
class Slice:
    id: strawberry.ID
    name: Optional[str]
    created_at: datetime.datetime
    updated_at: datetime.datetime
    dataset: Optional[Dataset] = None

    # has_many datapoints
    @strawberry.field
    async def datapoints(self, info: Info) -> list["Datapoint"]:
        datapoints = await info.context["datapoints_by_slice"].load(self.id)
        return [Datapoint.marshal(datapoint) for datapoint in datapoints]

    @classmethod
    def marshal(cls, model: models.Slice) -> "Slice":
        return cls(
            id=strawberry.ID(str(model.id)),
            dataset=Dataset.marshal(model.dataset) if model.dataset else None,
            name=model.name if model.name else None,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


@strawberry.type
class Datapoint:
    id: strawberry.ID
    created_at: datetime.datetime
    updated_at: datetime.datetime
    metadata_: Optional[str]
    resource_id: Optional[int]
    dataset: Optional[Dataset] = None

    # has_many embeddings
    @strawberry.field
    async def embeddings(self, info: Info) -> list["Embedding"]:
        embeddings = await info.context["embeddings_by_datapoint"].load(self.id)
        return [Embedding.marshal(embedding) for embedding in embeddings]

    # has_many tag
    @strawberry.field
    async def tags(self, info: Info) -> list["Tag"]:
        associations = await info.context["tags_by_datapoints"].load(self.id)
        return [Tag.marshal(association.tag) for association in associations]

    # has_many slices
    @strawberry.field
    async def slices(self, info: Info) -> list["Slice"]:
        slices = await info.context["slices_by_datapoints"].load(self.id)
        return [Slice.marshal(slice) for slice in slices]

    # has_one label
    @strawberry.field
    async def label(self, info: Info) -> "Label":
        labels = await info.context["label_by_datapoint"].load(self.id)
        return Label.marshal(labels[0])

    # has_one inference
    async def inference(self, info: Info) -> "Inference":
        inferences = await info.context["inference_by_datapoint"].load(self.id)
        return Inference.marshal(inferences[0])

    # belongs_to resource
    @strawberry.field
    async def resource(self, info: Info) -> "Resource":
        async with models.get_session() as s:
            sql = select(models.Resource).where(models.Resource.id == self.resource_id)
            resource = (await s.execute(sql)).scalars().first()
        return Resource.marshal(resource)

    # belongs_to dataset
    @strawberry.field
    async def dataset(self, info: Info) -> "Dataset":
        async with models.get_session() as s:
            sql = select(models.Dataset).where(models.Dataset.id == self.dataset_id)
            dataset = (await s.execute(sql)).scalars().first()
        return Dataset.marshal(dataset)

    @classmethod
    def marshal(cls, model: models.Datapoint) -> "Datapoint":
        return cls(
            id=strawberry.ID(str(model.id)),
            created_at=model.created_at,
            updated_at=model.updated_at,
            metadata_=model.metadata_,
            resource_id=model.resource_id,
        )


@strawberry.type
class Resource:
    id: strawberry.ID
    uri: str
    created_at: datetime.datetime
    updated_at: datetime.datetime

    # has_many datapoints
    @strawberry.field
    async def datapoints(self, info: Info) -> list["Datapoint"]:
        datapoints = await info.context["datapoints_by_resource"].load(self.id)
        return [Datapoint.marshal(datapoint) for datapoint in datapoints]

    @classmethod
    def marshal(cls, model: models.Resource) -> "Resource":
        return cls(
            id=strawberry.ID(str(model.id)),
            created_at=model.created_at,
            updated_at=model.updated_at,
            uri=model.uri,
        )


@strawberry.type
class Label:
    id: strawberry.ID
    data: JSON
    created_at: datetime.datetime
    updated_at: datetime.datetime

    @classmethod
    def marshal(cls, model: models.Label) -> "Label":
        return cls(
            id=strawberry.ID(str(model.id)),
            created_at=model.created_at,
            updated_at=model.updated_at,
            data=json.loads(model.data),
        )


@strawberry.type
class Tag:
    id: strawberry.ID
    name: Optional[str]
    created_at: datetime.datetime
    updated_at: datetime.datetime

    # has_many datapoints
    @strawberry.field
    async def datapoints(self, info: Info) -> list["Datapoint"]:
        datapoints = await info.context["datapoints_by_tag"].load(self.id)
        return [Datapoint.marshal(datapoint) for datapoint in datapoints]

    @classmethod
    def marshal(cls, model: models.Tag) -> "Tag":
        return cls(
            id=strawberry.ID(str(model.id)),
            name=model.name if model.name else None,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


@strawberry.type
class Inference:
    id: strawberry.ID
    created_at: datetime.datetime
    updated_at: datetime.datetime
    datapoint: Optional[Datapoint] = None

    @classmethod
    def marshal(cls, model: models.Inference) -> "Inference":
        return cls(
            id=strawberry.ID(str(model.id)),
            datapoint=Datapoint.marshal(model.datapoint) if model.datapoint else None,
            trained_model=TrainedModel.marshal(model.trained_model)
            if model.trained_model
            else None,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


@strawberry.type
class ModelArchitecture:
    id: strawberry.ID
    name: Optional[str]
    created_at: datetime.datetime
    updated_at: datetime.datetime
    project: Optional[Project] = None

    # has_many trained models
    @strawberry.field
    async def trained_models(self, info: Info) -> list["TrainedModel"]:
        trained_models = await info.context["trained_models_by_model_architecture"].load(self.id)
        return [TrainedModel.marshal(trained_model) for trained_model in trained_models]

    @classmethod
    def marshal(cls, model: models.ModelArchitecture) -> "ModelArchitecture":
        return cls(
            id=strawberry.ID(str(model.id)),
            project=Project.marshal(model.project) if model.project else None,
            name=model.name if model.name else None,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


@strawberry.type
class TrainedModel:
    id: strawberry.ID
    created_at: datetime.datetime
    updated_at: datetime.datetime
    model_architecture: Optional[ModelArchitecture] = None

    # has_many layer_sets
    @strawberry.field
    async def layer_sets(self, info: Info) -> list["LayerSet"]:
        layer_sets = await info.context["layer_sets_by_trained_model"].load(self.id)
        return [LayerSet.marshal(layer_set) for layer_set in layer_sets]

    @classmethod
    def marshal(cls, model: models.TrainedModel) -> "TrainedModel":
        return cls(
            id=strawberry.ID(str(model.id)),
            model_architecture=ModelArchitecture.marshal(model.model_architecture)
            if model.model_architecture
            else None,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


@strawberry.type
class LayerSet:
    id: strawberry.ID
    created_at: datetime.datetime
    updated_at: datetime.datetime
    trained_model: Optional[TrainedModel] = None

    # has_many layers
    @strawberry.field
    async def layers(self, info: Info) -> list["Layer"]:
        layers = await info.context["layers_by_layer_set"].load(self.id)
        return [Layer.marshal(layer) for layer in layers]

    @classmethod
    def marshal(cls, model: models.LayerSet) -> "LayerSet":
        return cls(
            id=strawberry.ID(str(model.id)),
            trained_model=TrainedModel.marshal(model.trained_model)
            if model.trained_model
            else None,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


@strawberry.type
class Layer:
    id: strawberry.ID
    created_at: datetime.datetime
    updated_at: datetime.datetime
    layer_set: Optional[LayerSet] = None

    # has_many embeddings
    @strawberry.field
    async def embeddings(self, info: Info) -> list["Embedding"]:
        embeddings = await info.context["embeddings_by_layer"].load(self.id)
        return [Embedding.marshal(embedding) for embedding in embeddings]

    @classmethod
    def marshal(cls, model: models.Layer) -> "Layer":
        return cls(
            id=strawberry.ID(str(model.id)),
            layer_set=LayerSet.marshal(model.layer_set) if model.layer_set else None,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


@strawberry.type
class Projector:
    id: strawberry.ID
    created_at: datetime.datetime
    updated_at: datetime.datetime

    @classmethod
    def marshal(cls, model: models.Projector) -> "Projector":
        return cls(
            id=strawberry.ID(str(model.id)),
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


@strawberry.type
class Job:
    id: strawberry.ID
    name: Optional[str]
    created_at: datetime.datetime
    updated_at: datetime.datetime

    @classmethod
    def marshal(cls, model: models.Job) -> "Job":
        return cls(
            id=strawberry.ID(str(model.id)),
            name=model.name if model.name else None,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


@strawberry.type
class EmbeddingSet:
    id: strawberry.ID
    created_at: datetime.datetime
    updated_at: datetime.datetime
    dataset: Optional[Dataset] = None  # belongs_to embedding_set

    # has_many projection_sets
    @strawberry.field
    async def projection_sets(self, info: Info) -> list["ProjectionSet"]:
        projection_sets = await info.context["projection_sets_by_embedding_set"].load(self.id)
        return [ProjectionSet.marshal(projection_set) for projection_set in projection_sets]

    # has_many embeddings
    @strawberry.field
    async def embeddings(self, info: Info) -> list["Embedding"]:
        embeddings = await info.context["embeddings_by_embedding_set"].load(self.id)
        return [Embedding.marshal(embedding) for embedding in embeddings]

    @classmethod
    def marshal(cls, model: models.EmbeddingSet) -> "EmbeddingSet":
        return cls(
            id=strawberry.ID(str(model.id)),
            created_at=model.created_at,
            updated_at=model.updated_at,
            dataset=Dataset.marshal(model.dataset) if model.dataset else None,
        )


@strawberry.type
class ProjectionSet:
    id: strawberry.ID
    created_at: datetime.datetime
    updated_at: datetime.datetime
    project_id: Optional[int]
    embedding_set: Optional[EmbeddingSet] = None  # belongs_to embedding_set

    # has_many projections
    @strawberry.field
    async def projections(self, info: Info) -> list["Projection"]:
        projections = await info.context["projections_by_projection_set"].load(self.id)
        return [Projection.marshal(projection) for projection in projections]

    # belongs_to project
    @strawberry.field
    async def project(self, info: Info) -> Project:
        async with models.get_session() as s:
            sql = select(models.Project).where(models.Project.id == self.project_id)
            project = (await s.execute(sql)).scalars().first()
        return Project.marshal(project)

    # belongs_to embedding_set
    @strawberry.field
    async def embedding_set(self, info: Info) -> EmbeddingSet:
        async with models.get_session() as s:
            sql = select(models.EmbeddingSet).where(models.EmbeddingSet.id == self.embedding_set_id)
            embedding_set = (await s.execute(sql)).scalars().first()
        return EmbeddingSet.marshal(embedding_set)

    @classmethod
    def marshal(cls, model: models.ProjectionSet) -> "ProjectionSet":
        return cls(
            id=strawberry.ID(str(model.id)),
            created_at=model.created_at,
            updated_at=model.updated_at,
            project_id=model.project_id,
        )


@strawberry.type
class Embedding:
    id: strawberry.ID
    data: Optional[str]
    label: Optional[str]
    inference_identifier: Optional[str]
    input_identifier: Optional[str]
    created_at: datetime.datetime
    updated_at: datetime.datetime
    embedding_set_id: Optional[int]
    datapoint_id: Optional[int]

    # has_many projections
    @strawberry.field
    async def projections(self, info: Info) -> list["Projection"]:
        projections = await info.context["projections_by_embedding"].load(self.id)
        return [Projection.marshal(projection) for projection in projections]

    # belongs_to datapoint
    @strawberry.field
    async def datapoint(self, info: Info) -> Datapoint:
        async with models.get_session() as s:
            sql = select(models.Datapoint).where(models.Datapoint.id == self.datapoint_id)
            datapoint = (await s.execute(sql)).scalars().first()
        return Datapoint.marshal(datapoint)

    # belongs_to embedding_set
    @strawberry.field
    async def embedding_set(self, info: Info) -> EmbeddingSet:
        async with models.get_session() as s:
            sql = select(models.EmbeddingSet).where(models.EmbeddingSet.id == self.embedding_set_id)
            embedding_set = (await s.execute(sql)).scalars().first()
        return EmbeddingSet.marshal(embedding_set)

    @classmethod
    def marshal(cls, model: models.Embedding) -> "Embedding":
        return cls(
            id=strawberry.ID(str(model.id)),
            data=model.data if model.data else None,
            label=model.label if model.label else None,
            inference_identifier=model.inference_identifier,
            input_identifier=model.input_identifier,
            created_at=model.created_at,
            updated_at=model.updated_at,
            datapoint_id=model.datapoint_id,
            embedding_set_id=model.embedding_set_id,
        )


@strawberry.type
class Projection:
    id: strawberry.ID
    x: float
    y: float
    created_at: datetime.datetime
    updated_at: datetime.datetime
    embedding_id: Optional[int]
    projection_set_id: Optional[int]

    # belongs_to projection_set
    @strawberry.field
    async def projection_set(self, info: Info) -> ProjectionSet:
        async with models.get_session() as s:
            sql = select(models.ProjectionSet).where(
                models.ProjectionSet.id == self.projection_set_id
            )
            projection_set = (await s.execute(sql)).scalars().first()
        return ProjectionSet.marshal(projection_set)

    # belongs_to embedding
    @strawberry.field
    async def embedding(self, info: Info) -> Embedding:
        async with models.get_session() as s:
            sql = select(models.Embedding).where(models.Embedding.id == self.embedding_id)
            embedding = (await s.execute(sql)).scalars().first()
        return Embedding.marshal(embedding)

    @classmethod
    def marshal(cls, model: models.Projection) -> "Projection":
        return cls(
            id=strawberry.ID(str(model.id)),
            x=model.x,
            y=model.y,
            created_at=model.created_at,
            updated_at=model.updated_at,
            projection_set_id=model.projection_set_id,
            embedding_id=model.embedding_id,
        )


@strawberry.type
class EmbeddingExists:
    message: str = "Embedding with this name already exist"


@strawberry.type
class EmbeddingNotFound:
    message: str = "Couldn't find an embedding with the supplied name"


@strawberry.type
class EmbeddingNameMissing:
    message: str = "Please supply an embedding name"


@strawberry.type
class ObjectDeleted:
    message: str = "This object has been deleted"


@strawberry.type
class ProjectDoesNotExist:
    message: str = "No Project by this ID exists, Object not created"


@strawberry.type
class DatasetDoesNotExist:
    message: str = "No Dataset by this ID exists, Object not created"


@strawberry.type
class ModelArchitectureDoesNotExist:
    message: str = "No Model Architecture by this ID exists, Object not created"


@strawberry.type
class TrainedModelDoesNotExist:
    message: str = "No Trained Model by this ID exists, Object not created"


@strawberry.type
class LayerSetDoesNotExist:
    message: str = "No Layer Set by this ID exists, Object not created"


@strawberry.type
class LabelDoesNotExist:
    message: str = "No Label by this ID exists, Object not created"


@strawberry.type
class ResourceDoesNotExist:
    message: str = "No Resource by this ID exists, Object not created"


AddEmbeddingResponse = strawberry.union("AddEmbeddingResponse", (Embedding, EmbeddingExists))
AddEmbeddingSetResponse = EmbeddingSet
AddProjectionSetResponse = ProjectionSet
AddProjectionResponse = Projection
DeleteProjectResponse = ObjectDeleted
AddDatasetResponse = strawberry.union("AddDatasetResponse", (Dataset, ProjectDoesNotExist))
AddSliceResponse = strawberry.union("AddSliceResponse", (Slice, DatasetDoesNotExist))
AddTagResponse = Tag
AddModelArchitectureResponse = strawberry.union(
    "AddModelArchitectureResponse", (ModelArchitecture, ProjectDoesNotExist)
)
AddTrainedModelResponse = strawberry.union(
    "AddTrainedModelResponse", (TrainedModel, ModelArchitectureDoesNotExist)
)
AddLayerSetResponse = strawberry.union("AddLayerSetResponse", (LayerSet, TrainedModelDoesNotExist))
AddLayerResponse = strawberry.union("AddLayerResponse", (Layer, LayerSetDoesNotExist))

AddResourceResponse = Resource
AddLabelResponse = Label
AddDatapointResponse = strawberry.union(
    "AddDatapointResponse", (Datapoint, LabelDoesNotExist, ResourceDoesNotExist)
)

# Pagination
# https://strawberry.rocks/docs/guides/pagination


@strawberry.type
class Connection(Generic[GenericType]):
    """Represents a paginated relationship between two entities

    This pattern is used when the relationship itself has attributes.
    In a Facebook-based domain example, a friendship between two people
    would be a connection that might have a `friendshipStartTime`
    """

    page_info: "PageInfo"
    edges: list["Edge[GenericType]"]


@strawberry.type
class PageInfo:
    """Pagination context to navigate objects with cursor-based pagination

    Instead of classic offset pagination via `page` and `limit` parameters,
    here we have a cursor of the last object and we fetch items starting from that one

    Read more at:
        - https://graphql.org/learn/pagination/#pagination-and-edges
        - https://relay.dev/graphql/connections.htm
    """

    has_next_page: bool
    has_previous_page: bool
    start_cursor: Optional[str]
    end_cursor: Optional[str]


@strawberry.type
class Edge(Generic[GenericType]):
    """An edge may contain additional information of the relationship. This is the trivial case"""

    node: GenericType
    cursor: str


def build_embedding_cursor(embedding: Embedding):
    """Adapt this method to build an *opaque* ID from an instance"""
    embeddingid = f"{(embedding.id)}".encode("utf-8")
    return base64.b64encode(embeddingid).decode()


Cursor = str


@strawberry.input
class PageInput:
    first: int
    after: Optional[Cursor]


async def get_embeddings_from_db(after_id, range):
    async with models.get_session() as s:
        sql = select(models.Embedding).offset(after_id).limit(range)
        db_embeddings = (await s.execute(sql)).scalars().unique().all()
        return db_embeddings


async def get_embeddings(pageInput: PageInput) -> Connection[Embedding]:
    """
    A non-trivial implementation should efficiently fetch only
    the necessary embeddings after the offset.
    For simplicity, here we build the list and then slice it accordingly
    """
    after = pageInput.after
    first = pageInput.first
    if after is not UNSET:
        after = int(base64.b64decode(after).decode())
    else:
        after = None

    embeddings = await get_embeddings_from_db(after, first)

    edges = [
        Edge(node=Embedding.marshal(embedding), cursor=build_embedding_cursor(embedding))
        for embedding in embeddings
    ]

    return Connection(
        page_info=PageInfo(
            has_previous_page=False,
            has_next_page=len(embeddings) > first,
            start_cursor=edges[0].cursor if edges else None,
            end_cursor=edges[-2].cursor if len(edges) > 1 else None,
        ),
        edges=edges[:-1],  # exclude last one as it was fetched to know if there is a next page
    )
