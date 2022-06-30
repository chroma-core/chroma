from curses.ascii import EM
from re import A, L
from h11 import Data
from numpy import Inf
import strawberry
import base64
import models
import datetime
import json

from typing import Optional,  List, Generic, TypeVar
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
        return [ModelArchitecture.marshal(model_architecture) for model_architecture in model_architectures]

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
    project: Optional[Project] = None

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

    @classmethod
    def marshal(cls, model: models.Dataset) -> "Dataset":
        return cls(
            id=strawberry.ID(str(model.id)), 
            project=Project.marshal(model.project) if model.project else None,
            name=model.name if model.name else None,
            created_at=model.created_at,
            updated_at=model.updated_at
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
    dataset: Optional[Dataset] = None
    resource: Optional["Resource"] = None

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
    # @strawberry.field
    # async def inference(self, info: Info) -> "Inference":
    #     inferences = await info.context["inference_by_datapoint"].load(self.id)
    #     return Inference.marshal(inferences[0]) if inferences[0] != None : None

    @classmethod
    def marshal(cls, model: models.Datapoint) -> "Datapoint":
        return cls(
            id=strawberry.ID(str(model.id)),
            dataset=Dataset.marshal(model.dataset) if model.dataset else None,
            resource=Resource.marshal(model.resource) if model.resource else None,
            created_at=model.created_at,
            updated_at=model.updated_at,
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
            uri=model.uri
        )   

@strawberry.type
class Label:
    id: strawberry.ID
    data: JSON
    created_at: datetime.datetime
    updated_at: datetime.datetime
    # datapoint: Optional[Datapoint] = None

    @classmethod
    def marshal(cls, model: models.Label) -> "Label":
        return cls(
            id=strawberry.ID(str(model.id)), 
            # datapoint=Datapoint.marshal(model.datapoint) if model.datapoint else None,
            created_at=model.created_at,
            updated_at=model.updated_at,
            data=json.loads(model.data)
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
    # trained_model: Optional[TrainedModel] = None

    @classmethod
    def marshal(cls, model: models.Inference) -> "Inference":
        return cls(
            id=strawberry.ID(str(model.id)), 
            datapoint=Datapoint.marshal(model.datapoint) if model.datapoint else None,
            trained_model=TrainedModel.marshal(model.trained_model) if model.trained_model else None,
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
            model_architecture=ModelArchitecture.marshal(model.model_architecture) if model.model_architecture else None,
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
            trained_model=TrainedModel.marshal(model.trained_model) if model.trained_model else None,
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
    dataset: Optional[Dataset] = None # belongs_to embedding_set

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
    embedding_set: Optional[EmbeddingSet] = None # belongs_to embedding_set

    # has_many projections
    @strawberry.field
    async def projections(self, info: Info) -> list["Projection"]:
        projections = await info.context["projections_by_projection_set"].load(self.id)
        return [Projection.marshal(projection) for projection in projections]

    @classmethod
    def marshal(cls, model: models.ProjectionSet) -> "ProjectionSet":
        return cls(
            id=strawberry.ID(str(model.id)), 
            embedding_set=EmbeddingSet.marshal(model.embedding_set) if model.embedding_set else None,
            created_at=model.created_at,
            updated_at=model.updated_at,
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
    embedding_set: Optional[EmbeddingSet] = None # belongs_to embedding_set
    datapoint: Optional[Datapoint] = None # belongs_to projection_set

    # has_many projections
    @strawberry.field
    async def projections(self, info: Info) -> list["Projection"]:
        projections = await info.context["projections_by_embedding"].load(self.id)
        return [Projection.marshal(projection) for projection in projections]

    @classmethod
    def marshal(cls, model: models.Embedding) -> "Embedding":
        return cls(
            id=strawberry.ID(str(model.id)), 
            data=model.data if model.data else None,
            label=model.label if model.label else None,
            inference_identifier=model.inference_identifier,
            input_identifier=model.input_identifier,
            # layer=Layer.marshal(model.layer) if model.layer else None,
            embedding_set=EmbeddingSet.marshal(model.embedding_set) if model.embedding_set else None,
            datapoint=Datapoint.marshal(model.datapoint) if model.datapoint else None,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )

@strawberry.type
class Projection:
    id: strawberry.ID
    x: float
    y: float
    created_at: datetime.datetime
    updated_at: datetime.datetime
    embedding: Optional[Embedding] = None # belongs_to embedding
    projection_set: Optional[ProjectionSet] = None # belongs_to projection_set

    @classmethod
    def marshal(cls, model: models.Projection) -> "Projection":
        return cls(
            id=strawberry.ID(str(model.id)),
            x=model.x,
            y=model.y,
            embedding=Embedding.marshal(model.embedding) if model.embedding else None,
            projection_set=ProjectionSet.marshal(model.projection_set) if model.projection_set else None,
            created_at=model.created_at,
            updated_at=model.updated_at,
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
class DatasetDoesntExist:
    message: str = "No Dataset by this ID exists, Object not created"

@strawberry.type
class ModelArchitectureDoesntExist:
    message: str = "No Model Architecture by this ID exists, Object not created"

@strawberry.type
class TrainedModelDoesntExist:
    message: str = "No Trained Model by this ID exists, Object not created"

@strawberry.type
class LayerSetDoesntExist:
    message: str = "No Layer Set by this ID exists, Object not created"

@strawberry.type
class LabelDoesntExist:
    message: str = "No Label by this ID exists, Object not created"

@strawberry.type
class ResourceDoesntExist:
    message: str = "No Resource by this ID exists, Object not created"

AddEmbeddingResponse = strawberry.union("AddEmbeddingResponse", (Embedding, EmbeddingExists))
AddEmbeddingSetResponse = EmbeddingSet
AddProjectionSetResponse = ProjectionSet
AddProjectionResponse = Projection
DeleteProjectResponse = ObjectDeleted
AddDatasetResponse = strawberry.union("AddDatasetResponse", (Dataset, ProjectDoesNotExist))
AddSliceResponse = strawberry.union("AddSliceResponse", (Slice, DatasetDoesntExist))
AddTagResponse = Tag
AddModelArchitectureResponse = strawberry.union("AddModelArchitectureResponse", (ModelArchitecture, ProjectDoesNotExist))
AddTrainedModelResponse = strawberry.union("AddTrainedModelResponse", (TrainedModel, ModelArchitectureDoesntExist))
AddLayerSetResponse = strawberry.union("AddLayerSetResponse", (LayerSet, TrainedModelDoesntExist))
AddLayerResponse = strawberry.union("AddLayerResponse", (Layer, LayerSetDoesntExist))

AddResourceResponse = Resource
AddLabelResponse = Label
AddDatapointResponse = strawberry.union("AddDatapointResponse", (Datapoint, LabelDoesntExist, ResourceDoesntExist))

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
        edges=edges[:-1]  # exclude last one as it was fetched to know if there is a next page
    )