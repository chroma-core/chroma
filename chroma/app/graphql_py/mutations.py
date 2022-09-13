from xmlrpc.client import Boolean
from h11 import Data
import strawberry
from yaml import load
from chroma.app.models import Tagdatapoint
from chroma.app.graphql_py.types import ResourceDoesNotExist, TagDatapoint
import models
from sqlalchemy.orm import selectinload

from celery.result import AsyncResult
from tasks import process_embeddings, compute_class_distances
from celery import chain, group

from typing import Optional, List, Annotated, Union
from graphql_py.types import (
    Embedding,
    AddEmbeddingResponse,
    EmbeddingSet,
    AddEmbeddingSetResponse,
    ObjectDeleted,
    ProjectionSet,
    AddProjectionSetResponse,
    Projection,
    AddProjectionResponse,
    EmbeddingExists,
    EmbeddingNotFound,
    EmbeddingNameMissing,
    Datapoint,
    Dataset,
    Job,
    Inference,
    Tag,
    Project,
    DeleteProjectResponse,
    AddDatasetResponse,
    ProjectDoesNotExist,
    DatasetDoesNotExist,
    AddTagResponse,
    AddDatapointResponse,
    AddLabelResponse,
    AddResourceResponse,
    Resource,
    Label,
    Datapoint,
    LabelDoesNotExist,
    ResourceDoesNotExist,
)
from strawberry.dataloader import DataLoader
from sqlalchemy import insert, select, update, delete


@strawberry.input
class EmbeddingInput:
    data: str
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
    categories: Optional[str] = None


@strawberry.input
class UpdateDatasetInput:
    id: strawberry.ID
    name: Optional[str] = None
    categories: Optional[str] = None


# Datapoint Inputs
@strawberry.input
class CreateDatapointInput:
    dataset_id: int
    resource_id: int
    label_id: Optional[int] = None
    # inference_id: Optional[int] = None


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
    id: Optional[strawberry.ID] = None  # remove this later, placeholder for now


@strawberry.input
class UpdateInferenceInput:
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
    inference_data: str
    resource_uri: str
    embedding_data: List[str]
    embedding_set_id: int

    ctx_embedding_data: Optional[List[str]] = None
    ctx_embedding_set_id: Optional[int] = None

    metadata: Optional[str] = ""


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
    target: Optional[list[str]]
    datapointIds: Optional[list[int]]


@strawberry.type
class Mutation:

    #
    # Abstract
    #

    @strawberry.mutation
    def run_projector_on_embedding_sets(self, embedding_set_ids: list[int]) -> Boolean:
        process_embeddings.delay(embedding_set_ids)
        return True

    @strawberry.mutation
    def compute_class_distances(
        self, training_embedding_set_id: int, target_embedding_set_id: int
    ) -> Boolean:
        compute_class_distances.delay(
            training_embedding_set_id=training_embedding_set_id,
            target_embedding_set_id=target_embedding_set_id,
        )
        return True

    @strawberry.mutation
    async def remove_tag_from_datapoints(self, data: TagByNameToDataPointsInput) -> ObjectDeleted:
        async with models.get_session() as s:
            sql = select(models.Tag).where(models.Tag.name == data.tagName)
            tag = (await s.execute(sql)).scalars().first()
            await s.flush()

            targetData = [None for element in range(len(data.datapointIds))]
            if data.target != None:
                targetData = data.target

            for datapointId, target in zip(data.datapointIds, targetData):
                sql = (
                    select(models.Datapoint)
                    .where(models.Datapoint.id == datapointId)
                    .options(selectinload(models.Datapoint.tags))
                )
                datapoint = (await s.execute(sql)).scalar_one()

                # you have to explicitly delete this via the association
                # there has to be a better way of doing this......
                query = (
                    delete(models.Tagdatapoint)
                    .where(models.Tagdatapoint.tag == tag)
                    .where(models.Tagdatapoint.target == target)
                    .where(models.Tagdatapoint.datapoint == datapoint)
                )
                await s.execute(query)

            try:
                await s.commit()
            except Exception:
                await s.rollback()
                raise

        return ObjectDeleted

    # @strawberry.mutation
    # async def remove_tag_from_datapoint(self, data: TagToDataPointInput) -> ObjectDeleted:
    #     async with models.get_session() as s:
    #         sql = select(models.Tag).where(models.Tag.id == data.tagId)
    #         tag = (await s.execute(sql)).scalar_one()
    #         sql = (
    #             select(models.Datapoint)
    #             .where(models.Datapoint.id == data.datapointId)
    #             .options(selectinload(models.Datapoint.tags))
    #         )
    #         datapoint = (await s.execute(sql)).scalar_one()

    #         # you have to explicitly delete this via the association
    #         # there has to be a better way of doing this......
    #         query = (
    #             delete(models.Tagdatapoint)
    #             .where(models.Tagdatapoint.tag == tag)
    #             .where(models.Tagdatapoint.datapoint == datapoint)
    #         )
    #         await s.execute(query)
    #         try:
    #             await s.commit()
    #         except Exception:
    #             await s.rollback()
    #             raise
    #     return ObjectDeleted

    @strawberry.mutation
    async def append_tag_by_name_to_datapoints(
        self, data: TagByNameToDataPointsInput
    ) -> list[Datapoint]:
        async with models.get_session() as s:
            sql = select(models.Tag).where(models.Tag.name == data.tagName)
            tag = (await s.execute(sql)).scalars().first()

            if tag is None:
                tag = models.Tag(name=data.tagName)
                s.add(tag)
                await s.flush()

            targetData = [None for element in range(len(data.datapointIds))]
            if data.target != None:
                targetData = data.target

            datapoints = []
            tagdatapoints_to_add = []
            for datapointId, target in zip(data.datapointIds, targetData):
                sql = (
                    select(models.Datapoint)
                    .where(models.Datapoint.id == datapointId)
                    .options(selectinload(models.Datapoint.tags))
                )
                datapoint = (await s.execute(sql)).scalar_one()

                tagdatapoints_to_add.append(
                    dict(left_id=tag.id, right_id=datapoint.id, target=target)
                )
                s.add(datapoint)
                datapoints.append(datapoint)

            # raise Exception(str(tagdatapoints_to_add))
            # we have to add things this way to avoid the key constraint
            # throwing an error if there is a duplicate. we just want to ignore that case
            await s.execute(
                insert(models.Tagdatapoint, values=tagdatapoints_to_add, prefixes=["OR IGNORE"])
            )
            await s.flush()
            await s.commit()

        return [Datapoint.marshal(loc) for loc in datapoints]

    # @strawberry.mutation
    # async def append_tag_to_datapoints(self, data: TagToDataPointsInput) -> list[Datapoint]:
    #     async with models.get_session() as s:
    #         sql = select(models.Tag).where(models.Tag.id == data.tagId)
    #         tag = (await s.execute(sql)).scalar_one()

    #         datapoints = []
    #         for datapointId in data.datapointIds:
    #             sql = (
    #                 select(models.Datapoint)
    #                 .where(models.Datapoint.id == datapointId)
    #                 .options(selectinload(models.Datapoint.tags))
    #             )
    #             datapoint = (await s.execute(sql)).scalar_one()

    #             # you have to explicitly add this via the association
    #             # there has to be a better way of doing this......
    #             datapoint.tags.append(models.Tagdatapoint(tag=tag))
    #             s.add(datapoint)
    #             datapoints.append(datapoint)

    #         await s.flush()
    #         await s.commit()
    #     return [Datapoint.marshal(loc) for loc in datapoints]

    # @strawberry.mutation
    # async def append_tag_to_datapoint(self, data: TagToDataPointInput) -> Datapoint:
    #     async with models.get_session() as s:
    #         sql = select(models.Tag).where(models.Tag.id == data.tagId)
    #         tag = (await s.execute(sql)).scalar_one()

    #         sql = (
    #             select(models.Datapoint)
    #             .where(models.Datapoint.id == data.datapointId)
    #             .options(selectinload(models.Datapoint.tags))
    #         )
    #         datapoint = (await s.execute(sql)).scalar_one()

    #         # you have to explicitly add this via the association
    #         # there has to be a better way of doing this......
    #         datapoint.tags.append(models.Tagdatapoint(tag=tag))

    #         s.add(datapoint)

    #         await s.flush()
    #         await s.commit()
    #     return Datapoint.marshal(datapoint)

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
                label=label, dataset=dataset, resource=resource, project_id=dataset.project_id
            )
            s.add(datapoint)
            await s.commit()
        return Datapoint.marshal(datapoint)

    # @strawberry.mutation
    # async def create_datapoint_embedding_set(
    #     self, data: CreateDatapointEmbeddingSetInput
    # ) -> Datapoint:
    #     async with models.get_session() as s:
    #         label = models.Label(data=data.label_data)
    #         s.add(label)
    #         inference = models.Inference(data=data.inference_data)
    #         s.add(inference)
    #         resource = models.Resource(uri=data.resource_uri)
    #         s.add(resource)
    #         embedding = models.Embedding(data=data.embedding_data)
    #         s.add(embedding)
    #         inference = models.Inference(data=data.inference_data)
    #         await s.flush()

    #         sql = select(models.Dataset).where(models.Dataset.id == data.datasetId)
    #         dataset = (await s.execute(sql)).scalars().first()

    #         datapoint = models.Datapoint(
    #             label=label,
    #             inference=inference,
    #             dataset=dataset,
    #             resource=resource,
    #             metadata_=data.metadata,
    #             project_id=dataset.project_id,
    #         )
    #         datapoint.embeddings.append(embedding)
    #         s.add(datapoint)
    #         await s.commit()
    #     return Datapoint.marshal(datapoint)

    @strawberry.mutation
    async def create_batch_datapoint_embedding_set(
        self, batch_data: CreateBatchDatapointEmbeddingSetInput
    ) -> Boolean:
        async with models.get_session() as s:
            objs_to_add = []

            sql = select(models.Dataset).where(
                models.Dataset.id == batch_data.batch_data[0].datasetId
            )
            dataset = (await s.execute(sql)).scalars().first()

            for datapoint_embedding_set in batch_data.batch_data:
                label = models.Label(data=datapoint_embedding_set.label_data)
                inference = models.Inference(data=datapoint_embedding_set.inference_data)
                resource = models.Resource(uri=datapoint_embedding_set.resource_uri)
                objs_to_add.extend([label, inference, resource])

                embeddings = [
                    models.Embedding(
                        data=embedding_data,
                        embedding_set_id=datapoint_embedding_set.embedding_set_id,
                    )
                    for embedding_data in datapoint_embedding_set.embedding_data
                ]
                objs_to_add.extend(embeddings)

                ctx_embeddings = []
                if datapoint_embedding_set.ctx_embedding_data != None:
                    ctx_embeddings = [
                        models.Embedding(
                            data=ctx_embedding_data,
                            embedding_set_id=datapoint_embedding_set.ctx_embedding_set_id,
                        )
                        for ctx_embedding_data in datapoint_embedding_set.ctx_embedding_data
                    ]
                    objs_to_add.extend(ctx_embeddings)

                datapoint = models.Datapoint(
                    project_id=dataset.project_id,
                    label=label,
                    inference=inference,
                    dataset=dataset,
                    resource=resource,
                    metadata_=datapoint_embedding_set.metadata,
                )
                datapoint.embeddings.extend(embeddings)
                datapoint.embeddings.extend(ctx_embeddings)
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
            res = models.Project(name=project.name)
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
                ret = models.Project(name=project.name)
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

            res = models.Dataset(name=dataset.name, project=project)
            if dataset.categories:
                res.categories = dataset.categories
            s.add(res)
            await s.commit()
        return Dataset.marshal(res)

    @strawberry.mutation
    async def create_or_get_dataset(self, dataset: CreateDatasetInput) -> Dataset:
        async with models.get_session() as s:

            sql = select(models.Project).where(models.Project.id == dataset.project_id)
            project = (await s.execute(sql)).scalars().first()

            sql = (
                select(models.Dataset)
                .where(models.Dataset.name == dataset.name)
                .where(models.Dataset.project == project)
            )
            result = (await s.execute(sql)).scalars().first()

            if result is None:
                ret = models.Dataset(name=dataset.name, project=project)
                if dataset.categories:
                    ret.categories = dataset.categories

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
            if dataset.categories:
                query = query.values(categories=dataset.categories)

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
    # Tag
    #
    @strawberry.mutation
    async def create_tag(self, tag: CreateTagInput) -> AddTagResponse:
        async with models.get_session() as s:
            res = models.Tag(name=tag.name)
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
    # Job
    #
    @strawberry.mutation
    async def create_job(self, job: CreateJobInput) -> Job:
        async with models.get_session() as s:
            res = models.Job(name=job.name)
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

            sql = select(models.Embedding).where(
                models.Embedding.id == projection_input.embedding_id
            )
            embedding = (await s.execute(sql)).scalars().first()

            sql = select(models.ProjectionSet).where(
                models.ProjectionSet.id == projection_input.projection_set_id
            )
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
    async def add_projection_set(
        self, projection_set_input: ProjectionSetInput
    ) -> AddProjectionSetResponse:
        async with models.get_session() as s:
            sql = select(models.EmbeddingSet).where(
                models.EmbeddingSet.id == projection_set_input.embedding_set_id
            )
            embedding_set = (await s.execute(sql)).scalars().first()

            res = models.ProjectionSet(embedding_set=embedding_set)
            s.add(res)
            await s.commit()
        return ProjectionSet.marshal(res)

    #
    # Embedding Set
    #
    @strawberry.mutation
    async def create_embedding_set(
        self, embedding_set: EmbeddingSetInput
    ) -> AddEmbeddingSetResponse:
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
    # @strawberry.mutation
    # async def add_embedding(self, embedding_input: EmbeddingInput) -> AddEmbeddingResponse:
    #     async with models.get_session() as s:

    #         sql = select(models.EmbeddingSet).where(
    #             models.EmbeddingSet.id == embedding_input.embedding_set_id
    #         )
    #         embedding_set = (await s.execute(sql)).scalars().first()

    #         db_embedding = models.Embedding(
    #             data=embedding_input.data,
    #             label=embedding_input.label,
    #             inference_identifier=embedding_input.inference_identifier,
    #             input_identifier=embedding_input.input_identifier,
    #             embedding_set=embedding_set,
    #         )
    #         s.add(db_embedding)
    #         await s.commit()
    #     return Embedding.marshal(db_embedding)

    #
    # Resource
    #
    @strawberry.mutation
    async def create_resource(self, resource: CreateResourceInput) -> AddResourceResponse:
        async with models.get_session() as s:
            res = models.Resource(uri=resource.uri)
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
            res = models.Label(data=label.data)
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
                return LabelDoesNotExist

            sql = select(models.Label).where(models.Label.id == datapoint.label_id)
            label = (await s.execute(sql)).scalars().first()

            if label is None:
                return LabelDoesNotExist

            sql = select(models.Resource).where(models.Resource.id == datapoint.resource_id)
            resource = (await s.execute(sql)).scalars().first()

            if resource is None:
                return ResourceDoesNotExist

            res = models.Datapoint(
                dataset=dataset, resource=resource, label=label, project_id=dataset.project_id
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
        all_queries = [
            select(models.Projection).where(models.Projection.embedding_id == key) for key in keys
        ]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data


async def load_projection_sets_by_embedding_set(keys: list) -> list[ProjectionSet]:
    async with models.get_session() as s:
        all_queries = [
            select(models.ProjectionSet).where(models.ProjectionSet.embedding_set_id == key)
            for key in keys
        ]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data


async def load_embeddings_by_embedding_set(keys: list) -> list[Embedding]:
    async with models.get_session() as s:
        all_queries = [
            select(models.Embedding).where(models.Embedding.embedding_set_id == key) for key in keys
        ]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data


async def load_projections_by_projection_set(keys: list) -> list[Projection]:
    async with models.get_session() as s:
        all_queries = [
            select(models.Projection).where(models.Projection.projection_set_id == key)
            for key in keys
        ]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data


async def load_datapoints_by_dataset(keys: list) -> list[Datapoint]:
    async with models.get_session() as s:
        all_queries = [
            select(models.Datapoint).where(models.Datapoint.dataset_id == key) for key in keys
        ]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data


async def load_tags_by_datapoints(keys: list) -> list[Tag]:
    async with models.get_session() as s:
        # there has to be a better way of doing this......
        all_queries = [
            select(models.Tagdatapoint)
            .where(models.Tagdatapoint.right_id == key)
            .options(selectinload(models.Tagdatapoint.tag))
            for key in keys
        ]
        data = [(await s.execute(sql)).scalars().all() for sql in all_queries]
    return data


async def load_tagdatapoints_by_datapoints(keys: list) -> list[TagDatapoint]:
    async with models.get_session() as s:
        all_queries = [
            select(models.Tagdatapoint).where(models.Tagdatapoint.right_id == key) for key in keys
        ]
        data = [(await s.execute(sql)).scalars().all() for sql in all_queries]
    return data


async def load_datapoints_by_resource(keys: list) -> list[Datapoint]:
    async with models.get_session() as s:
        all_queries = [
            select(models.Datapoint).where(models.Datapoint.resource_id == key) for key in keys
        ]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data


async def load_datapoints_by_tag(keys: list) -> list[Datapoint]:
    async with models.get_session() as s:
        all_queries = [
            select(models.Datapoint).where(models.Datapoint.tag_id == key) for key in keys
        ]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data


async def load_embedding_sets_by_dataset(keys: list) -> list[EmbeddingSet]:
    async with models.get_session() as s:
        all_queries = [
            select(models.EmbeddingSet).where(models.EmbeddingSet.dataset_id == key) for key in keys
        ]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data


async def load_datasets_by_project(keys: list) -> list[Dataset]:
    async with models.get_session() as s:
        all_queries = [
            select(models.Dataset).where(models.Dataset.project_id == key) for key in keys
        ]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data


async def load_label_by_datapoint(keys: list) -> list[Label]:
    async with models.get_session() as s:
        all_queries = [select(models.Label).where(models.Label.datapoint_id == key) for key in keys]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data


async def load_inference_by_datapoint(keys: list) -> list[Inference]:
    async with models.get_session() as s:
        all_queries = [
            select(models.Inference).where(models.Inference.datapoint_id == key) for key in keys
        ]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data


async def load_embeddings_by_datapoint(keys: list) -> list[Embedding]:
    async with models.get_session() as s:
        all_queries = [
            select(models.Embedding).where(models.Embedding.datapoint_id == key) for key in keys
        ]
        data = [(await s.execute(sql)).scalars().unique().all() for sql in all_queries]
    return data


async def get_context() -> dict:
    return {
        "projections_by_embedding": DataLoader(load_fn=load_projections_by_embedding),
        "projection_sets_by_embedding_set": DataLoader(
            load_fn=load_projection_sets_by_embedding_set
        ),
        "embeddings_by_embedding_set": DataLoader(load_fn=load_embeddings_by_embedding_set),
        "embedding_sets_by_dataset": DataLoader(load_fn=load_embedding_sets_by_dataset),
        "projections_by_projection_set": DataLoader(load_fn=load_projections_by_projection_set),
        "datapoints_by_dataset": DataLoader(load_fn=load_datapoints_by_dataset),
        "tags_by_datapoints": DataLoader(load_fn=load_tags_by_datapoints),
        "datapoints_by_resource": DataLoader(load_fn=load_datapoints_by_resource),
        "datapoints_by_tag": DataLoader(load_fn=load_datapoints_by_tag),
        "datasets_by_project": DataLoader(load_fn=load_datasets_by_project),
        "label_by_datapoint": DataLoader(load_fn=load_label_by_datapoint),
        "inference_by_datapoint": DataLoader(load_fn=load_inference_by_datapoint),
        "embeddings_by_datapoint": DataLoader(load_fn=load_embeddings_by_datapoint),
        "tagdatapoints_by_datapoints": DataLoader(load_fn=load_tagdatapoints_by_datapoints),
    }
