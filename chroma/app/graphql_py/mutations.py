from xmlrpc.client import Boolean
from h11 import Data
import strawberry
from yaml import load
from chroma.app.graphql_py.types import ResourceDoesntExist
from chroma.app.models import Tagdatapoint
import models
from sqlalchemy.orm import selectinload

from celery.result import AsyncResult
from tasks import process_embeddings

from typing import Optional, List, Annotated
from graphql_py.types import (
    Embedding, 
    AddEmbeddingResponse,
    EmbeddingSet,
    AddEmbeddingSetResponse,
    LayerSetDoesntExist,
    ObjectDeleted,
    ProjectionSet,
    AddProjectionSetResponse, 
    Projection,
    AddProjectionResponse,
    EmbeddingExists, 
    EmbeddingNotFound, 
    EmbeddingNameMissing,
    ModelArchitecture,
    Slice,
    Datapoint,
    Dataset,
    TrainedModel,
    LayerSet,
    Job,
    Inference,
    Layer,
    Tag,
    Projector,
    Project,
    DeleteProjectResponse,
    AddDatasetResponse,
    ProjectDoesNotExist,
    DatasetDoesntExist,
    AddSliceResponse,
    AddTagResponse,
    AddModelArchitectureResponse,
    AddTrainedModelResponse,
    ModelArchitectureDoesntExist,
    TrainedModelDoesntExist,
    AddLayerSetResponse,
    AddLayerResponse,
    AddDatapointResponse,
    AddLabelResponse,
    AddResourceResponse,
    Resource,
    Label,
    Datapoint,
    LabelDoesntExist,
    ResourceDoesntExist
)
from strawberry.dataloader import DataLoader
from sqlalchemy import insert, select, update, delete


@strawberry.input
class EmbeddingInput:
    data: str
    label: str
    inference_identifier: str
    input_identifier: str
    embedding_set_id: int

@strawberry.input
class EmbeddingsInput:
    embeddings: list[EmbeddingInput]

@strawberry.input
class EmbeddingSetInput:
    dataset_id: int

@strawberry.input
class ProjectionInput:
    embedding_id: int
    projection_set_id: int
    x: float
    y: float

@strawberry.input
class ProjectionSetInput:
    projection_set_id: int

# Project Inputs
@strawberry.input
class CreateProjectInput:
    name: str

@strawberry.input
class UpdateProjectInput:
    id: strawberry.ID
    name: Optional[str] = None

# Dataset Inputs
@strawberry.input
class CreateDatasetInput:
    name: str
    project_id: int

@strawberry.input
class UpdateDatasetInput:
    id: strawberry.ID
    name: Optional[str] = None

# Slice Inputs
@strawberry.input
class CreateSliceInput:
    name: str
    dataset_id: int

@strawberry.input
class UpdateSliceInput:
    id: strawberry.ID
    name: Optional[str] = None

# Datapoint Inputs
@strawberry.input
class CreateDatapointInput:
    dataset_id: int
    resource_id: int
    label_id: Optional[int] = None
    inference_id: Optional[int] = None

@strawberry.input
class UpdateDatapointInput:
    id: strawberry.ID
    resource_id: int
    label_id: Optional[int] = None
    inference_id: Optional[int] = None

# Resource Inputs
@strawberry.input
class CreateResourceInput:
    uri: str 

@strawberry.input
class UpdateResourceInput:
    id: strawberry.ID
    uri: str

# Label Inputs
@strawberry.input
class CreateLabelInput:
    data: str

@strawberry.input
class UpdateLabelInput:
    id: strawberry.ID
    data: str

# Tag Inputs
@strawberry.input
class CreateTagInput:
    name: str

@strawberry.input
class UpdateTagInput:
    id: strawberry.ID
    name: Optional[str] = None

# Inference Inputs
@strawberry.input
class CreateInferenceInput:
    id: Optional[strawberry.ID] = None # remove this later, placeholder for now

@strawberry.input
class UpdateInferenceInput:
    id: strawberry.ID

# ModelArchitecture Inputs
@strawberry.input
class CreateModelArchitectureInput:
    name: str
    project_id: int

@strawberry.input
class UpdateModelArchitectureInput:
    id: strawberry.ID
    name: Optional[str] = None

# TrainedModel Inputs
@strawberry.input
class CreateTrainedModelInput:
    model_architecture_id: int
    id: Optional[strawberry.ID] = None # remove this later, placeholder for now

@strawberry.input
class UpdateTrainedModelInput:
    id: strawberry.ID

# LayerSet Inputs
@strawberry.input
class CreateLayerSetInput:
    trained_model_id: int
    id: Optional[strawberry.ID] = None # remove this later, placeholder for now

@strawberry.input
class UpdateLayerSetInput:
    id: strawberry.ID

# Layer Inputs
@strawberry.input
class CreateLayerInput:
    layer_set_id: int
    id: Optional[strawberry.ID] = None # remove this later, placeholder for now

@strawberry.input
class UpdateLayerInput:
    id: strawberry.ID

# Projector Inputs
@strawberry.input
class CreateProjectorInput:
    id: Optional[strawberry.ID] = None # remove this later, placeholder for now

@strawberry.input
class UpdateProjectorInput:
    id: strawberry.ID

# Job Inputs
@strawberry.input
class CreateJobInput:
    name: str

@strawberry.input
class UpdateJobInput:
    id: strawberry.ID
    name: Optional[str] = None

# Abstract Inputs
@strawberry.input
class CreateDatapointSetInput:
    datasetId: int
    label_data: str
    resource_uri: str

# Abstract Inputs
@strawberry.input
class CreateDatapointEmbeddingSetInput:
    datasetId: int
    label_data: str
    resource_uri: str
    embedding_data: str
    embedding_set_id: int

@strawberry.input
class CreateBatchDatapointEmbeddingSetInput:
    batch_data: list[CreateDatapointEmbeddingSetInput]

@strawberry.input
class TagToDataPointInput:
    tagId: int
    datapointId: int

@strawberry.input
class TagToDataPointsInput:
    tagId: int
    datapointIds: Optional[list[int]]

@strawberry.input
class TagByNameToDataPointsInput:
    tagName: str
    datapointIds: Optional[list[int]]
    
@strawberry.type
class Mutation:

    #
    # Abstract
    #
    @strawberry.mutation
    def run_projector_on_embedding_set(self, embedding_set_id: int) -> Boolean:
        process_embeddings.delay(embedding_set_id)
        return True

    @strawberry.mutation
    async def remove_tag_from_datapoints(self, data: TagByNameToDataPointsInput) -> ObjectDeleted:
        async with models.get_session() as s:
            sql = select(models.Tag).where(models.Tag.name == data.tagName)
            tag = (await s.execute(sql)).scalars().first()
            await s.flush()

            for datapointId in data.datapointIds:
                sql = select(models.Datapoint).where(models.Datapoint.id == datapointId).options(selectinload(models.Datapoint.tags))
                datapoint = (await s.execute(sql)).scalar_one()
            
                # you have to explicitly delete this via the association
                # there has to be a better way of doing this......
                query = delete(models.Tagdatapoint).where(models.Tagdatapoint.tag == tag).where(models.Tagdatapoint.datapoint == datapoint)
                await s.execute(query)

            try:
                await s.commit()
            except Exception:
                await s.rollback()
                raise
                
        return ObjectDeleted

    @strawberry.mutation
    async def remove_tag_from_datapoint(self, data: TagToDataPointInput) -> ObjectDeleted:
        async with models.get_session() as s:
            sql = select(models.Tag).where(models.Tag.id == data.tagId)
            tag = (await s.execute(sql)).scalar_one()
            sql = select(models.Datapoint).where(models.Datapoint.id == data.datapointId).options(selectinload(models.Datapoint.tags))
            datapoint = (await s.execute(sql)).scalar_one()
            
            # you have to explicitly delete this via the association
            # there has to be a better way of doing this......
            query = delete(models.Tagdatapoint).where(models.Tagdatapoint.tag == tag).where(models.Tagdatapoint.datapoint == datapoint)
            await s.execute(query)
            try:
                await s.commit()
            except Exception:
                await s.rollback()
                raise
        return ObjectDeleted

    @strawberry.mutation
    async def append_tag_by_name_to_datapoints(self, data: TagByNameToDataPointsInput) -> list[Datapoint]:
        async with models.get_session() as s:
            sql = select(models.Tag).where(models.Tag.name == data.tagName)
            tag = (await s.execute(sql)).scalars().first()

            if tag is None: 
                tag = models.Tag(
                    name=data.tagName 
                )
                s.add(tag)
                await s.flush()

            datapoints = []
            tagdatapoints_to_add = []
            for datapointId in data.datapointIds:
                sql = select(models.Datapoint).where(models.Datapoint.id == datapointId).options(selectinload(models.Datapoint.tags))
                datapoint = (await s.execute(sql)).scalar_one()
                
                tagdatapoints_to_add.append(dict(left_id=tag.id, right_id=datapoint.id))
                s.add(datapoint)
                datapoints.append(datapoint)
            
            # we have to add things this way to avoid the key constraint
            # throwing an error if there is a duplicate. we just want to ignore that case
            await s.execute(insert(models.Tagdatapoint, values=tagdatapoints_to_add))#, prefixes=['OR IGNORE']))
            await s.flush()
            await s.commit()
        return [Datapoint.marshal(loc) for loc in datapoints]

    @strawberry.mutation
    async def append_tag_to_datapoints(self, data: TagToDataPointsInput) -> list[Datapoint]:
        async with models.get_session() as s:
            sql = select(models.Tag).where(models.Tag.id == data.tagId)
            tag = (await s.execute(sql)).scalar_one()

            datapoints = []
            for datapointId in data.datapointIds:
                sql = select(models.Datapoint).where(models.Datapoint.id == datapointId).options(selectinload(models.Datapoint.tags))
                datapoint = (await s.execute(sql)).scalar_one()
                
                # you have to explicitly add this via the association
                # there has to be a better way of doing this......
                datapoint.tags.append(models.Tagdatapoint(tag=tag))
                s.add(datapoint)
                datapoints.append(datapoint)
            
            await s.flush()
            await s.commit()
        return [Datapoint.marshal(loc) for loc in datapoints]

    @strawberry.mutation
    async def append_tag_to_datapoint(self, data: TagToDataPointInput) -> Datapoint:
        async with models.get_session() as s:
            sql = select(models.Tag).where(models.Tag.id == data.tagId)
            tag = (await s.execute(sql)).scalar_one()
            
            sql = select(models.Datapoint).where(models.Datapoint.id == data.datapointId).options(selectinload(models.Datapoint.tags))
            datapoint = (await s.execute(sql)).scalar_one()
            
            # you have to explicitly add this via the association
            # there has to be a better way of doing this......
            datapoint.tags.append(models.Tagdatapoint(tag=tag))
            
            s.add(datapoint)
            
            await s.flush()
            await s.commit()
        return Datapoint.marshal(datapoint)

    @strawberry.mutation
    async def create_datapoint_set(self, data: CreateDatapointSetInput) -> Datapoint:
        async with models.get_session() as s:
            label = models.Label(data=data.label_data)
            s.add(label)
            resource = models.Resource(uri=data.resource_uri)
            s.add(resource)
            await s.flush()

            sql = select(models.Dataset).where(models.Dataset.id == data.datasetId)
            dataset = (await s.execute(sql)).scalars().first()

            datapoint = models.Datapoint(
                label=label,
                dataset=dataset,
                resource=resource
            )
            s.add(datapoint)
            await s.commit()
        return Datapoint.marshal(datapoint)

    @strawberry.mutation
    async def create_datapoint_embedding_set(self, data: CreateDatapointEmbeddingSetInput) -> Datapoint:
        async with models.get_session() as s:
            label = models.Label(data=data.label_data)
            s.add(label)
            resource = models.Resource(uri=data.resource_uri)
            s.add(resource)
            embedding = models.Embedding(data=data.embedding_data)
            s.add(embedding)
            await s.flush()

            sql = select(models.Dataset).where(models.Dataset.id == data.datasetId)
            dataset = (await s.execute(sql)).scalars().first()

            datapoint = models.Datapoint(
                label=label,
                dataset=dataset,
                resource=resource,
            )
            datapoint.embeddings.append(embedding)
            s.add(datapoint)
            await s.commit()
        return Datapoint.marshal(datapoint)

    @strawberry.mutation
    async def create_batch_datapoint_embedding_set(self, batch_data: CreateBatchDatapointEmbeddingSetInput) -> Boolean:
        async with models.get_session() as s:
            objs_to_add = []

            sql = select(models.Dataset).where(models.Dataset.id == batch_data.batch_data[0].datasetId)
            dataset = (await s.execute(sql)).scalars().first()

            sql = select(models.EmbeddingSet).where(models.EmbeddingSet.id == batch_data.batch_data[0].embedding_set_id)
            embedding_set = (await s.execute(sql)).scalars().first()

            for datapoint_embedding_set in batch_data.batch_data:
                label = models.Label(data=datapoint_embedding_set.label_data)
                resource = models.Resource(uri=datapoint_embedding_set.resource_uri)
                embedding = models.Embedding(data=datapoint_embedding_set.embedding_data, embedding_set=embedding_set)
                objs_to_add.extend([label, resource, embedding])

                datapoint = models.Datapoint(label=label, dataset=dataset, resource=resource)
                datapoint.embeddings.append(embedding)
                objs_to_add.append(datapoint)

            # add all is very important for speed! 
            s.add_all(objs_to_add)
            await s.commit()

        return True

    #
    # Project
    #
    @strawberry.mutation
    async def create_project(self, project: CreateProjectInput) -> Project:
        async with models.get_session() as s:
            res = models.Project(
                name=project.name 
            )
            s.add(res)
            await s.commit()
        return Project.marshal(res)

    # this is used to create or return a project
    @strawberry.mutation
    async def create_or_get_project(self, project: CreateProjectInput) -> Project:
        async with models.get_session() as s:

            sql = select(models.Project).where(models.Project.name == project.name)
            result = (await s.execute(sql)).scalars().first()

            if result is None: 
                ret = models.Project(
                    name=project.name 
                )
                s.add(ret)
                await s.commit()
            else: 
                ret = result
            
        return Project.marshal(ret)

    @strawberry.mutation
    async def update_project(self, project: UpdateProjectInput) -> Project:
        async with models.get_session() as s:
            query = update(models.Project).where(models.Project.id == project.id)
            if project.name:
                query = query.values(name=project.name)
            await s.execute(query)
            await s.flush()
            final_project = select(models.Project).where(models.Project.id == project.id)
            val = (await s.execute(final_project)).scalars().first()
            await s.commit()
        return Project.marshal(val)

    @strawberry.mutation
    async def delete_project(self, project: UpdateProjectInput) -> DeleteProjectResponse:
        async with models.get_session() as s:
            query = delete(models.Project).where(models.Project.id == project.id)
            await s.execute(query)
            try:
                await s.commit()
            except Exception:
                await s.rollback()
                raise
        return DeleteProjectResponse

    #
    # Dataset
    #
    @strawberry.mutation
    async def create_dataset(self, dataset: CreateDatasetInput) -> AddDatasetResponse:
        async with models.get_session() as s:
            sql = select(models.Project).where(models.Project.id == dataset.project_id)
            project = (await s.execute(sql)).scalars().first()

            if project is None: 
                return ProjectDoesNotExist

            res = models.Dataset(
                name=dataset.name,
                project=project
            )
            s.add(res)
            await s.commit()
        return Dataset.marshal(res)

    @strawberry.mutation
    async def create_or_get_dataset(self, dataset: CreateDatasetInput) -> Dataset:
        async with models.get_session() as s:

            sql = select(models.Project).where(models.Project.id == dataset.project_id)
            project = (await s.execute(sql)).scalars().first()

            sql = select(models.Dataset).where(models.Dataset.name == dataset.name).where(models.Dataset.project==project)
            result = (await s.execute(sql)).scalars().first()

            if result is None: 
                ret = models.Dataset(
                    name=dataset.name,
                    project=project
                )
                s.add(ret)
                await s.commit()
            else: 
                ret = result
            
        return Dataset.marshal(ret)

    @strawberry.mutation
    async def update_dataset(self, dataset: UpdateDatasetInput) -> Dataset:
        async with models.get_session() as s:
            query = update(models.Dataset).where(models.Dataset.id == dataset.id)
            if dataset.name:
                query = query.values(name=dataset.name)

            await s.execute(query)
            await s.flush()

            final_dataset = select(models.Dataset).where(models.Dataset.id == dataset.id)
            val = (await s.execute(final_dataset)).scalars().first()
            await s.commit()
        return Dataset.marshal(val)

    @strawberry.mutation
    async def delete_dataset(self, dataset: UpdateDatasetInput) -> ObjectDeleted:
        async with models.get_session() as s:
            query = delete(models.Dataset).where(models.Dataset.id == dataset.id)
            await s.execute(query)
            try:
                await s.commit()
            except Exception:
                await s.rollback()
                raise
        return ObjectDeleted

    #
    # Slice
    #
    @strawberry.mutation
    async def create_slice(self, slice: CreateSliceInput) -> AddSliceResponse:
        async with models.get_session() as s:
            sql = select(models.Dataset).where(models.Dataset.id == slice.dataset_id)
            dataset = (await s.execute(sql)).scalars().first()

            if dataset is None: 
                return DatasetDoesntExist

            res = models.Slice(
                name=slice.name,
                dataset=dataset
            )
            s.add(res)
            await s.commit()
        return Slice.marshal(res)

    @strawberry.mutation
    async def update_slice(self, slice: UpdateSliceInput) -> Slice:
        async with models.get_session() as s:
            query = update(models.Slice).where(models.Slice.id == slice.id)
            if slice.name:
                query = query.values(name=slice.name)

            await s.execute(query)
            await s.flush()

            final_slice = select(models.Slice).where(models.Slice.id == slice.id)
            val = (await s.execute(final_slice)).scalars().first()
            await s.commit()
        return Slice.marshal(val)

    @strawberry.mutation
    async def delete_slice(self, slice: UpdateSliceInput) -> ObjectDeleted:
        async with models.get_session() as s:
            query = delete(models.Slice).where(models.Slice.id == slice.id)
            await s.execute(query)
            try:
                await s.commit()
            except Exception:
                await s.rollback()
                raise
        return ObjectDeleted

    #
    # Tag
    #
    @strawberry.mutation
    async def create_tag(self, tag: CreateTagInput) -> AddTagResponse:
        async with models.get_session() as s:
            res = models.Tag(
                name=tag.name
            )
            s.add(res)
            await s.commit()
        return Tag.marshal(res)

    @strawberry.mutation
    async def update_tag(self, tag: UpdateTagInput) -> Tag:
        async with models.get_session() as s:
            query = update(models.Tag).where(models.Tag.id == tag.id)
            if tag.name:
                query = query.values(name=tag.name)

            await s.execute(query)
            await s.flush()

            final_tag = select(models.Tag).where(models.Tag.id == tag.id)
            val = (await s.execute(final_tag)).scalars().first()
            await s.commit()
        return Tag.marshal(val)

    @strawberry.mutation
    async def delete_tag(self, tag: UpdateTagInput) -> ObjectDeleted:
        async with models.get_session() as s:
            query = delete(models.Tag).where(models.Tag.id == tag.id)
            await s.execute(query)
            try:
                await s.commit()
            except Exception:
                await s.rollback()
                raise
        return ObjectDeleted

    #
    # Model Architecture
    #
    @strawberry.mutation
    async def create_model_architecture(self, model_architecture: CreateModelArchitectureInput) -> AddModelArchitectureResponse:
        async with models.get_session() as s:
            sql = select(models.Project).where(models.Project.id == model_architecture.project_id)
            project = (await s.execute(sql)).scalars().first()

            if project is None: 
                return ProjectDoesNotExist

            res = models.ModelArchitecture(
                name=model_architecture.name,
                project=project
            )
            s.add(res)
            await s.commit()
        return ModelArchitecture.marshal(res)

    @strawberry.mutation
    async def update_model_architecture(self, model_architecture: UpdateModelArchitectureInput) -> ModelArchitecture:
        async with models.get_session() as s:
            query = update(models.ModelArchitecture).where(models.ModelArchitecture.id == model_architecture.id)
            if model_architecture.name:
                query = query.values(name=model_architecture.name)

            await s.execute(query)
            await s.flush()

            final_model_architecture = select(models.ModelArchitecture).where(models.ModelArchitecture.id == model_architecture.id)
            val = (await s.execute(final_model_architecture)).scalars().first()
            await s.commit()
        return ModelArchitecture.marshal(val)

    @strawberry.mutation
    async def delete_model_architecture(self, model_architecture: UpdateModelArchitectureInput) -> ObjectDeleted:
        async with models.get_session() as s:
            query = delete(models.ModelArchitecture).where(models.ModelArchitecture.id == model_architecture.id)
            await s.execute(query)
            try:
                await s.commit()
            except Exception:
                await s.rollback()
                raise
        return ObjectDeleted

    #
    # Trained Model
    #
    @strawberry.mutation
    async def create_trained_model(self, trained_model: CreateTrainedModelInput) -> AddTrainedModelResponse:
        async with models.get_session() as s:
            sql = select(models.ModelArchitecture).where(models.ModelArchitecture.id == trained_model.model_architecture_id)
            model_architecture = (await s.execute(sql)).scalars().first()

            if model_architecture is None: 
                return ModelArchitectureDoesntExist

            res = models.TrainedModel(
                model_architecture=model_architecture
            )
            s.add(res)
            await s.commit()
        return TrainedModel.marshal(res)

    # we dont have any fields to update on this object yet
    # @strawberry.mutation
    # async def update_trained_model(self, trained_model: UpdateTrainedModelInput) -> TrainedModel:
    #     async with models.get_session() as s:
    #         query = update(models.TrainedModel).where(models.TrainedModel.id == trained_model.id)
    #         if trained_model.name:
    #             query = query.values(name=trained_model.name)

    #         await s.execute(query)
    #         await s.flush()

    #         final_trained_model = select(models.TrainedModel).where(models.TrainedModel.id == trained_model.id)
    #         val = (await s.execute(final_trained_model)).scalars().first()
    #         await s.commit()
    #     return TrainedModel.marshal(val)

    @strawberry.mutation
    async def delete_trained_model(self, trained_model: UpdateTrainedModelInput) -> ObjectDeleted:
        async with models.get_session() as s:
            query = delete(models.TrainedModel).where(models.TrainedModel.id == trained_model.id)
            await s.execute(query)
            try:
                await s.commit()
            except Exception:
                await s.rollback()
                raise
        return ObjectDeleted

    #
    # Layer Set
    #
    @strawberry.mutation
    async def create_layer_set(self, layer_set: CreateLayerSetInput) -> AddLayerSetResponse:
        async with models.get_session() as s:
            sql = select(models.TrainedModel).where(models.TrainedModel.id == layer_set.trained_model_id)
            trained_model = (await s.execute(sql)).scalars().first()

            if trained_model is None: 
                return TrainedModelDoesntExist

            res = models.LayerSet(
                trained_model=trained_model
            )
            s.add(res)
            await s.commit()
        return LayerSet.marshal(res)

    # we dont have any fields to update on this object yet
    # @strawberry.mutation
    # async def update_layer_set(self, layer_set: UpdateLayerSetInput) -> LayerSet:
    #     async with models.get_session() as s:
    #         query = update(models.LayerSet).where(models.LayerSet.id == layer_set.id)
    #         if layer_set.name:
    #             query = query.values(name=layer_set.name)

    #         await s.execute(query)
    #         await s.flush()

    #         final_layer_set = select(models.LayerSet).where(models.LayerSet.id == layer_set.id)
    #         val = (await s.execute(final_layer_set)).scalars().first()
    #         await s.commit()
    #     return LayerSet.marshal(val)

    @strawberry.mutation
    async def delete_layer_set(self, layer_set: UpdateLayerSetInput) -> ObjectDeleted:
        async with models.get_session() as s:
            query = delete(models.LayerSet).where(models.LayerSet.id == layer_set.id)
            await s.execute(query)
            try:
                await s.commit()
            except Exception:
                await s.rollback()
                raise
        return ObjectDeleted

    #
    # Layer
    #
    @strawberry.mutation
    async def create_layer(self, layer: CreateLayerInput) -> AddLayerResponse:
        async with models.get_session() as s:
            sql = select(models.LayerSet).where(models.LayerSet.id == layer.layer_set_id)
            layer_set = (await s.execute(sql)).scalars().first()

            if layer_set is None: 
                return LayerSetDoesntExist

            res = models.Layer(
                layer_set=layer_set
            )
            s.add(res)
            await s.commit()
        return Layer.marshal(res)

    # we dont have any fields to update on this object yet
    # @strawberry.mutation
    # async def update_layer(self, layer: UpdateLayerInput) -> Layer:
    #     async with models.get_session() as s:
    #         query = update(models.Layer).where(models.Layer.id == layer.id)
    #         if layer.name:
    #             query = query.values(name=layer.name)

    #         await s.execute(query)
    #         await s.flush()

    #         final_layer = select(models.Layer).where(models.Layer.id == layer.id)
    #         val = (await s.execute(final_layer)).scalars().first()
    #         await s.commit()
    #     return Layer.marshal(val)

    @strawberry.mutation
    async def delete_layer(self, layer: UpdateLayerInput) -> ObjectDeleted:
        async with models.get_session() as s:
            query = delete(models.Layer).where(models.Layer.id == layer.id)
            await s.execute(query)
            try:
                await s.commit()
            except Exception:
                await s.rollback()
                raise
        return ObjectDeleted

    #
    # Projector
    #
    @strawberry.mutation
    async def create_projector(self, projector: CreateProjectorInput) -> Projector:
        async with models.get_session() as s:
            res = models.Projector()
            s.add(res)
            await s.commit()
        return Projector.marshal(res)

    # we dont have any fields to update on this object yet
    # @strawberry.mutation
    # async def update_projector(self, layer: UpdateLayerInput) -> Layer:
    #     async with models.get_session() as s:
    #         query = update(models.Layer).where(models.Layer.id == layer.id)
    #         if layer.name:
    #             query = query.values(name=layer.name)

    #         await s.execute(query)
    #         await s.flush()

    #         final_projector = select(models.Layer).where(models.Layer.id == layer.id)
    #         val = (await s.execute(final_layer)).scalars().first()
    #         await s.commit()
    #     return Layer.marshal(val)

    @strawberry.mutation
    async def delete_projector(self, projector: UpdateProjectorInput) -> ObjectDeleted:
        async with models.get_session() as s:
            query = delete(models.Projector).where(models.Projector.id == projector.id)
            await s.execute(query)
            try:
                await s.commit()
            except Exception:
                await s.rollback()
                raise
        return ObjectDeleted

    #
    # Job
    #
    @strawberry.mutation
    async def create_job(self, job: CreateJobInput) -> Job:
        async with models.get_session() as s:
            res = models.Job(
                name=job.name 
            )
            s.add(res)
            await s.commit()
        return Job.marshal(res)

    @strawberry.mutation
    async def update_job(self, job: UpdateJobInput) -> Job:
        async with models.get_session() as s:
            query = update(models.Job).where(models.Job.id == job.id)
            if job.name:
                query = query.values(name=job.name)

            await s.execute(query)
            await s.flush()

            final_job = select(models.Job).where(models.Job.id == job.id)
            val = (await s.execute(final_job)).scalars().first()
            await s.commit()
        return Job.marshal(val)

    @strawberry.mutation
    async def delete_job(self, job: UpdateJobInput) -> ObjectDeleted:
        async with models.get_session() as s:
            query = delete(models.Job).where(models.Job.id == job.id)
            await s.execute(query)
            try:
                await s.commit()
            except Exception:
                await s.rollback()
                raise
        return ObjectDeleted

    #
    # Projection
    #
    @strawberry.mutation
    async def add_projection(self, projection_input: ProjectionInput) -> AddProjectionResponse:
        async with models.get_session() as s:

            sql = select(models.Embedding).where(models.Embedding.id == projection_input.embedding_id)
            embedding = (await s.execute(sql)).scalars().first()

            sql = select(models.ProjectionSet).where(models.ProjectionSet.id == projection_input.projection_set_id)
            projection_set = (await s.execute(sql)).scalars().first()

            res = models.Projection(
                x=projection_input.x, 
                y=projection_input.y,
                embedding=embedding, 
                projection_set=projection_set, 
            )
            s.add(res)
            await s.commit()
        return Projection.marshal(res)

    #
    # Projection Set
    #
    @strawberry.mutation
    async def add_projection_set(self, projection_set_input: ProjectionSetInput) -> AddProjectionSetResponse:
        async with models.get_session() as s:
            sql = select(models.EmbeddingSet).where(models.EmbeddingSet.id == projection_set_input.embedding_set_id)
            embedding_set = (await s.execute(sql)).scalars().first()

            res = models.ProjectionSet(embedding_set=embedding_set)
            s.add(res)
            await s.commit()
        return ProjectionSet.marshal(res)

    #
    # Embedding Set
    #
    @strawberry.mutation
    async def create_embedding_set(self, embedding_set: EmbeddingSetInput) -> AddEmbeddingSetResponse:
        async with models.get_session() as s:
            sql = select(models.Dataset).where(models.Dataset.id == embedding_set.dataset_id)
            dataset = (await s.execute(sql)).scalars().first()

            res = models.EmbeddingSet(dataset=dataset)
            s.add(res)
            await s.commit()
        return EmbeddingSet.marshal(res)

    #
    # Embedding 
    #
    @strawberry.mutation
    async def add_embedding(self, embedding_input: EmbeddingInput) -> AddEmbeddingResponse:
        async with models.get_session() as s:

            sql = select(models.EmbeddingSet).where(models.EmbeddingSet.id == embedding_input.embedding_set_id)
            embedding_set = (await s.execute(sql)).scalars().first()

            db_embedding = models.Embedding(
                data=embedding_input.data,
                label=embedding_input.label,
                inference_identifier=embedding_input.inference_identifier,
                input_identifier=embedding_input.input_identifier,
                embedding_set=embedding_set,
                )
            s.add(db_embedding)
            await s.commit()
        return Embedding.marshal(db_embedding)

    # TODO: fix this function so it doesn't hard code the embedding set
    # @strawberry.mutation
    # async def add_embeddings(self, embeddings_input: EmbeddingsInput) -> list[Embedding]: # batch query example
    #     async with models.get_session() as s:
    #         objects = []

    #         sql = select(models.EmbeddingSet).where(models.EmbeddingSet.id == 1)
    #         embedding_set = (await s.execute(sql)).scalars().first()

    #         for em in embeddings_input.embeddings:
    #             objects.append(models.Embedding(
    #                 data=em.data,
    #                 label=em.label,
    #                 inference_identifier=em.inference_identifier,
    #                 input_identifier=em.input_identifier,
    #                 embedding_set=embedding_set,
    #             ))

    #         s.add_all(objects)
    #         await s.commit()
    #     return [Embedding.marshal(loc) for loc in objects]

    #
    # Resource
    #
    @strawberry.mutation
    async def create_resource(self, resource: CreateResourceInput) -> AddResourceResponse:
        async with models.get_session() as s:
            res = models.Resource(
                uri=resource.uri
            )
            s.add(res)
            await s.commit()
        return Resource.marshal(res)

    @strawberry.mutation
    async def update_resource(self, resource: UpdateResourceInput) -> Resource:
        async with models.get_session() as s:
            query = update(models.Resource).where(models.Resource.id == resource.id)
            if resource.uri:
                query = query.values(uri=resource.uri)

            await s.execute(query)
            await s.flush()

            final_resource = select(models.Resource).where(models.Resource.id == resource.id)
            val = (await s.execute(final_resource)).scalars().first()
            await s.commit()
        return Resource.marshal(val)

    @strawberry.mutation
    async def delete_resource(self, resource: UpdateResourceInput) -> ObjectDeleted:
        async with models.get_session() as s:
            query = delete(models.Resource).where(models.Resource.id == resource.id)
            await s.execute(query)
            try:
                await s.commit()
            except Exception:
                await s.rollback()
                raise
        return ObjectDeleted

    #
    # Label
    #
    @strawberry.mutation
    async def create_label(self, label: CreateLabelInput) -> AddLabelResponse:
        async with models.get_session() as s:
            res = models.Label(
                data=label.data
            )
            s.add(res)
            await s.commit()
        return Label.marshal(res)

    @strawberry.mutation
    async def update_label(self, label: UpdateLabelInput) -> Label:
        async with models.get_session() as s:
            query = update(models.Label).where(models.Label.id == label.id)
            if label.data:
                query = query.values(data=label.data)

            await s.execute(query)
            await s.flush()

            final_label = select(models.Label).where(models.Label.id == label.id)
            val = (await s.execute(final_label)).scalars().first()
            await s.commit()
        return Label.marshal(val)

    @strawberry.mutation
    async def delete_label(self, label: UpdateLabelInput) -> ObjectDeleted:
        async with models.get_session() as s:
            query = delete(models.Label).where(models.Label.id == label.id)
            await s.execute(query)
            try:
                await s.commit()
            except Exception:
                await s.rollback()
                raise
        return ObjectDeleted

    #
    # Datapoint
    #
    @strawberry.mutation
    async def create_datapoint(self, datapoint: CreateDatapointInput) -> AddDatapointResponse:
        async with models.get_session() as s:

            sql = select(models.Dataset).where(models.Dataset.id == datapoint.dataset_id)
            dataset = (await s.execute(sql)).scalars().first()
    
            if dataset is None: 
                return LabelDoesntExist

            sql = select(models.Label).where(models.Label.id == datapoint.label_id)
            label = (await s.execute(sql)).scalars().first()

            if label is None: 
                return LabelDoesntExist

            sql = select(models.Resource).where(models.Resource.id == datapoint.resource_id)
            resource = (await s.execute(sql)).scalars().first()

            if resource is None: 
                return ResourceDoesntExist
            
            res = models.Datapoint(
                dataset=dataset,
                resource=resource, 
                label=label
            )
            s.add(res)
            await s.commit()
        return Datapoint.marshal(res)

    @strawberry.mutation
    async def update_datapoint(self, datapoint: UpdateDatapointInput) -> Datapoint:
        async with models.get_session() as s:
            query = update(models.Datapoint).where(models.Datapoint.id == datapoint.id)
            if datapoint.data:
                query = query.values(data=datapoint.data)

            await s.execute(query)
            await s.flush()

            final_datapoint = select(models.Datapoint).where(models.Datapoint.id == datapoint.id)
            val = (await s.execute(final_datapoint)).scalars().first()
            await s.commit()
        return Datapoint.marshal(val)

    @strawberry.mutation
    async def delete_datapoint(self, datapoint: UpdateDatapointInput) -> ObjectDeleted:
        async with models.get_session() as s:
            query = delete(models.Datapoint).where(models.Datapoint.id == datapoint.id)
            await s.execute(query)
            try:
                await s.commit()
            except Exception:
                await s.rollback()
                raise
        return ObjectDeleted

# TODO: move these to a different file I think....... they are used in types, and not in this file

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

async def load_model_architectures_by_project(keys: list) -> list[ModelArchitecture]:
    async with models.get_session() as s:
        all_queries = [select(models.ModelArchitecture).where(models.ModelArchitecture.project_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def load_slices_by_dataset(keys: list) -> list[Slice]:
    async with models.get_session() as s:
        all_queries = [select(models.Slice).where(models.Slice.dataset_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def load_datapoints_by_dataset(keys: list) -> list[Datapoint]:
    async with models.get_session() as s:
        all_queries = [select(models.Datapoint).where(models.Datapoint.dataset_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def load_datapoints_by_slice(keys: list) -> list[Datapoint]:
    async with models.get_session() as s:
        all_queries = [select(models.Datapoint).where(models.Datapoint.slice_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def load_slices_by_datapoints(keys: list) -> list[Slice]:
    async with models.get_session() as s:
        all_queries = [select(models.Slice).where(models.Slice.datapoint_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def load_tags_by_datapoints(keys: list) -> list[Tag]:
    async with models.get_session() as s:
        # you have to preload tags through the association
        # there has to be a better way of doing this......
        all_queries = [select(models.Tagdatapoint).where(models.Tagdatapoint.right_id == key).options(selectinload(models.Tagdatapoint.tag)) for key in keys]
        data = [(await s.execute(sql)).scalars().all() for sql in all_queries]
    return data

async def load_datapoints_by_resource(keys: list) -> list[Datapoint]:
    async with models.get_session() as s:
        all_queries = [select(models.Datapoint).where(models.Datapoint.resource_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def load_datapoints_by_tag(keys: list) -> list[Datapoint]:
    async with models.get_session() as s:
        all_queries = [select(models.Datapoint).where(models.Datapoint.tag_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def load_trained_models_by_model_architecture(keys: list) -> list[TrainedModel]:
    async with models.get_session() as s:
        all_queries = [select(models.TrainedModel).where(models.TrainedModel.model_architecture_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def load_layer_sets_by_trained_model(keys: list) -> list[LayerSet]:
    async with models.get_session() as s:
        all_queries = [select(models.LayerSet).where(models.LayerSet.trained_model_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def load_layers_by_layer_set(keys: list) -> list[Layer]:
    async with models.get_session() as s:
        all_queries = [select(models.Layer).where(models.Layer.layer_set_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def load_embeddings_by_layer(keys: list) -> list[Embedding]:
    async with models.get_session() as s:
        all_queries = [select(models.Embedding).where(models.Embedding.layer_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def load_datasets_by_project(keys: list) -> list[Dataset]:
    async with models.get_session() as s:
        all_queries = [select(models.Dataset).where(models.Dataset.project_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def load_label_by_datapoint(keys: list) -> list[Label]:
    async with models.get_session() as s:
        all_queries = [select(models.Label).where(models.Label.datapoint_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def load_inference_by_datapoint(keys: list) -> list[Inference]:
    async with models.get_session() as s:
        all_queries = [select(models.Inference).where(models.Inference.datapoint_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def load_embeddings_by_datapoint(keys: list) -> list[Embedding]:
    async with models.get_session() as s:
        all_queries = [select(models.Embedding).where(models.Embedding.datapoint_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data

async def get_context() -> dict:
    return {
        "projections_by_embedding": DataLoader(load_fn=load_projections_by_embedding),
        "projection_sets_by_embedding_set": DataLoader(load_fn=load_projection_sets_by_embedding_set),
        "embeddings_by_embedding_set": DataLoader(load_fn=load_embeddings_by_embedding_set),
        "projections_by_projection_set": DataLoader(load_fn=load_projections_by_projection_set),
        "model_architectures_by_project": DataLoader(load_fn=load_model_architectures_by_project),
        "slices_by_dataset": DataLoader(load_fn=load_slices_by_dataset),
        "datapoints_by_dataset": DataLoader(load_fn=load_datapoints_by_dataset),
        "datapoints_by_slice": DataLoader(load_fn=load_datapoints_by_slice),
        "slices_by_datapoints": DataLoader(load_fn=load_slices_by_datapoints),
        "tags_by_datapoints": DataLoader(load_fn=load_tags_by_datapoints),
        "datapoints_by_resource": DataLoader(load_fn=load_datapoints_by_resource),
        "datapoints_by_tag": DataLoader(load_fn=load_datapoints_by_tag),
        "trained_models_by_model_architecture": DataLoader(load_fn=load_trained_models_by_model_architecture),
        "layer_sets_by_trained_model": DataLoader(load_fn=load_layer_sets_by_trained_model),
        "layers_by_layer_set": DataLoader(load_fn=load_layers_by_layer_set),
        "embeddings_by_layer": DataLoader(load_fn=load_embeddings_by_layer),
        "datasets_by_project": DataLoader(load_fn=load_datasets_by_project),
        "label_by_datapoint": DataLoader(load_fn=load_label_by_datapoint),
        "inference_by_datapoint": DataLoader(load_fn=load_inference_by_datapoint),
        "embeddings_by_datapoint": DataLoader(load_fn=load_embeddings_by_datapoint),
    }

