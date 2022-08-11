import resource
import strawberry
import models
import json
import time
import requests
import base64
from sqlalchemy.orm import selectinload, joinedload, noload, subqueryload
from strawberry.scalars import JSON 
from math import floor, ceil
import numpy as np

from io import BytesIO
import io
from torchvision.datasets.mnist import MNIST, read_image_file, torch
from PIL import Image, ImageOps

def image_to_byte_array(image: Image) -> bytes:
  imgByteArr = io.BytesIO()
  image.save(imgByteArr, format="png")
  imgByteArr = imgByteArr.getvalue()
  return imgByteArr

from typing import Optional, List
from graphql_py.types import (
    Embedding, 
    EmbeddingSet, 
    ProjectionSet, 
    Projection,
    get_embeddings,
    Project, 
    Dataset,
    Slice,
    Datapoint, 
    Resource,
    Label,
    Tag, 
    Inference, 
    ModelArchitecture,
    TrainedModel,
    LayerSet, 
    Layer, 
    Projector,
    Job, 
 ) 
from strawberry.dataloader import DataLoader
from sqlalchemy import select
from strawberry import UNSET

@strawberry.input
class FilterDatapoints:
    tag_name: Optional[str]
    dataset_id: Optional[int]

@strawberry.input
class FilterProjectionSets:
    project_id: Optional[int]

@strawberry.type
class ImageAndSize:
    imageData: str 
    original_width: int 
    original_height: int 

@strawberry.type
class Query:

    # Abstract
    @strawberry.field
    async def mnist_image(self, identifier: str) -> str:

        test_data = read_image_file('../../examples/data/MNIST/raw/t10k-images-idx3-ubyte')
        train_data = read_image_file('../../examples/data/MNIST/raw/train-images-idx3-ubyte')

        # t10k-images-idx3-ubyte-7262
        split_id = identifier.split('-')
        dataset = split_id[0]
        index = int(split_id[-1])

        img = test_data[index] if dataset == 't10k' else train_data[index]
        img = torch.Tensor.numpy(img)
        image = Image.fromarray(img)
        inverted_image = ImageOps.invert(image)
        my_encoded_img = base64.encodebytes(image_to_byte_array(inverted_image)).decode('ascii')
        return my_encoded_img


    # Abstract
    @strawberry.field
    async def image_resolver(self, 
            identifier: str, 
            thumbnail: bool, 
            resolver_name: str, 
            leftOffset: float = None, 
            topOffset: float = None, 
            cropWidth: float = None, 
            cropHeight: float = None
        ) -> ImageAndSize:

        image = ""
        width = 0
        height = 0
        if (resolver_name == 'mnist'):
            test_data = read_image_file('../../examples/data/MNIST/raw/t10k-images-idx3-ubyte')
            train_data = read_image_file('../../examples/data/MNIST/raw/train-images-idx3-ubyte')

            # t10k-images-idx3-ubyte-7262
            split_id = identifier.split('-')
            dataset = split_id[0]
            index = int(split_id[-1])

            img = test_data[index] if dataset == 't10k' else train_data[index]
            img = torch.Tensor.numpy(img)
            image = Image.fromarray(img)
            width, height = image.size
            image = ImageOps.invert(image)

        if (resolver_name == 'filepath'):
            image = Image.open(identifier)
            width, height = image.size

        if (resolver_name == 'url'):
            response = requests.get('http://i.imgur.com/1T6nTzl.jpg')
            image = Image.open(BytesIO(response.content))
            width, height = image.size

        # apply custom crop
        if ((leftOffset != None) and (topOffset != None) and (cropHeight != None) and (cropWidth != None)):

            max_dimension = 0
            if (cropWidth > cropHeight):
                max_dimension = floor(cropWidth)
            else:
                max_dimension = floor(cropHeight)
            
            squares_in_max_dimension = 10
            n = ceil(max_dimension/squares_in_max_dimension)

            segment_black = np.ones(shape = [n,n])*120 # gray scale value
            segment_white = np.ones(shape = [n,n])*150 # gray scale value
            chessboard = np.hstack((segment_black,segment_white))
            for i in range(4):
                chessboard = np.hstack((chessboard,segment_black))
                chessboard = np.hstack((chessboard,segment_white))
            temp = chessboard
            for i in range(11):
                chessboard = np.concatenate((np.fliplr(chessboard),temp))
            img = Image.fromarray(chessboard.astype(np.uint8))

            result = Image.new(image.mode, (floor(cropWidth), floor(cropHeight)), (0, 0, 255))
            result.paste(img, (0,0))
            result.paste(image, (-floor(leftOffset), -floor(topOffset)))
            image = result

        # apply thumbnail
        if (thumbnail == True):
            image.thumbnail([120, 120])

        my_encoded_img = base64.encodebytes(image_to_byte_array(image)).decode('ascii')
        return ImageAndSize(imageData=my_encoded_img, original_height=height, original_width=width)

    # Project
    @strawberry.field
    async def projects(self) -> list[Project]:
        async with models.get_session() as s:
            sql = select(models.Project)
            result = (await s.execute(sql)).scalars().unique().all()
        return [Project.marshal(loc) for loc in result]

    @strawberry.field
    async def project(self, id: strawberry.ID) -> Project:
        async with models.get_session() as s:
            sql = select(models.Project).where(models.Project.id == int(id))
            val = (await s.execute(sql)).scalars().first()
        return Project.marshal(val)  
    
    # Dataset
    @strawberry.field
    async def datasets(self) -> list[Dataset]:
        async with models.get_session() as s:
            sql = select(models.Dataset)
            result = (await s.execute(sql)).scalars().unique().all()
        return [Dataset.marshal(loc) for loc in result]

    @strawberry.field
    async def dataset(self, id: strawberry.ID) -> Dataset:
        async with models.get_session() as s:
            sql = select(models.Dataset).where(models.Dataset.id == id)
            val = (await s.execute(sql)).scalars().first()
        return Dataset.marshal(val)  

    # Slice
    @strawberry.field
    async def slices(self) -> list[Slice]:
        async with models.get_session() as s:
            sql = select(models.Slice)
            result = (await s.execute(sql)).scalars().unique().all()
        return [Slice.marshal(loc) for loc in result]

    @strawberry.field
    async def slice(self, id: strawberry.ID) -> Slice:
        async with models.get_session() as s:
            sql = select(models.Slice).where(models.Slice.id == id)
            val = (await s.execute(sql)).scalars().first()
        return Slice.marshal(val)    
    
    # Datapoint
    @strawberry.field
    async def datapoints(self, filter: FilterDatapoints = None) -> list[Datapoint]:
        async with models.get_session() as s:
            sql = select(models.Datapoint)
            
            if filter and filter.tag_name is not None:
                sql = sql.join(models.Tagdatapoint).filter(models.Tagdatapoint.tag.has(name=filter.tag_name))

            if filter and filter.dataset_id is not None:
                sql = sql.where(models.Datapoint.dataset_id == filter.dataset_id)

            result = (await s.execute(sql)).scalars().unique().all()
        return [Datapoint.marshal(loc) for loc in result]

    @strawberry.field
    async def datapoint(self, id: strawberry.ID) -> Datapoint:
        async with models.get_session() as s:
            sql = select(models.Datapoint).where(models.Datapoint.id == id)
            val = (await s.execute(sql)).scalars().first()
        return Datapoint.marshal(val)  

    # Resource
    @strawberry.field
    async def resources(self) -> list[Resource]:
        async with models.get_session() as s:
            sql = select(models.Resource)
            result = (await s.execute(sql)).scalars().unique().all()
        return [Resource.marshal(loc) for loc in result]

    @strawberry.field
    async def resource(self, id: strawberry.ID) -> Resource:
        async with models.get_session() as s:
            sql = select(models.Resource).where(models.Resource.id == id)
            val = (await s.execute(sql)).scalars().first()
        return Resource.marshal(val)

    # Label
    @strawberry.field
    async def labels(self) -> list[Label]:
        async with models.get_session() as s:
            sql = select(models.Label)
            result = (await s.execute(sql)).scalars().unique().all()
        return [Label.marshal(loc) for loc in result]

    @strawberry.field
    async def label(self, id: strawberry.ID) -> Label:
        async with models.get_session() as s:
            sql = select(models.Label).where(models.Label.id == id)
            val = (await s.execute(sql)).scalars().first()
        return Label.marshal(val)  

    # Tag
    @strawberry.field
    async def tags(self) -> list[Tag]:
        async with models.get_session() as s:
            sql = select(models.Tag)
            result = (await s.execute(sql)).scalars().unique().all()
        return [Tag.marshal(loc) for loc in result]

    @strawberry.field
    async def tag(self, id: strawberry.ID) -> Tag:
        async with models.get_session() as s:
            sql = select(models.Tag).where(models.Tag.id == id)
            val = (await s.execute(sql)).scalars().first()
        return Tag.marshal(val)  

    # Inference
    @strawberry.field
    async def inferences(self) -> list[Inference]:
        async with models.get_session() as s:
            sql = select(models.Inference)
            result = (await s.execute(sql)).scalars().unique().all()
        return [Inference.marshal(loc) for loc in result]

    @strawberry.field
    async def inference(self, id: strawberry.ID) -> Inference:
        async with models.get_session() as s:
            sql = select(models.Inference).where(models.Inference.id == id)
            val = (await s.execute(sql)).scalars().first()
        return Inference.marshal(val)  
    
    # ModelArchitecture
    @strawberry.field
    async def model_architectures(self) -> list[ModelArchitecture]:
        async with models.get_session() as s:
            sql = select(models.ModelArchitecture)
            result = (await s.execute(sql)).scalars().unique().all()
        return [ModelArchitecture.marshal(loc) for loc in result]

    @strawberry.field
    async def model_architecture(self, id: strawberry.ID) -> ModelArchitecture:
        async with models.get_session() as s:
            sql = select(models.ModelArchitecture).where(models.ModelArchitecture.id == id)
            val = (await s.execute(sql)).scalars().first()
        return ModelArchitecture.marshal(val)  

    # TrainedModel
    @strawberry.field
    async def trained_models(self) -> list[TrainedModel]:
        async with models.get_session() as s:
            sql = select(models.TrainedModel)
            result = (await s.execute(sql)).scalars().unique().all()
        return [TrainedModel.marshal(loc) for loc in result]

    @strawberry.field
    async def trained_model(self, id: strawberry.ID) -> TrainedModel:
        async with models.get_session() as s:
            sql = select(models.TrainedModel).where(models.TrainedModel.id == id)
            val = (await s.execute(sql)).scalars().first()
        return TrainedModel.marshal(val)  

    # LayerSet
    @strawberry.field
    async def layer_sets(self) -> list[LayerSet]:
        async with models.get_session() as s:
            sql = select(models.LayerSet)
            result = (await s.execute(sql)).scalars().unique().all()
        return [LayerSet.marshal(loc) for loc in result]

    @strawberry.field
    async def layer_set(self, id: strawberry.ID) -> LayerSet:
        async with models.get_session() as s:
            sql = select(models.LayerSet).where(models.LayerSet.id == id)
            val = (await s.execute(sql)).scalars().first()
        return LayerSet.marshal(val)  

    # Layer
    @strawberry.field
    async def layers(self) -> list[Layer]:
        async with models.get_session() as s:
            sql = select(models.Layer)
            result = (await s.execute(sql)).scalars().unique().all()
        return [Layer.marshal(loc) for loc in result]

    @strawberry.field
    async def layer(self, id: strawberry.ID) -> Layer:
        async with models.get_session() as s:
            sql = select(models.Layer).where(models.Layer.id == id)
            val = (await s.execute(sql)).scalars().first()
        return Layer.marshal(val)  

    # Projector
    @strawberry.field
    async def projectors(self) -> list[Projector]:
        async with models.get_session() as s:
            sql = select(models.Projector)
            result = (await s.execute(sql)).scalars().unique().all()
        return [Projector.marshal(loc) for loc in result]

    @strawberry.field
    async def projector(self, id: strawberry.ID) -> Projector:
        async with models.get_session() as s:
            sql = select(models.Projector).where(models.Projector.id == id)
            val = (await s.execute(sql)).scalars().first()
        return Projector.marshal(val)  

    # Job
    @strawberry.field
    async def jobs(self) -> list[Job]:
        async with models.get_session() as s:
            sql = select(models.Job)
            result = (await s.execute(sql)).scalars().unique().all()
        return [Job.marshal(loc) for loc in result]

    @strawberry.field
    async def job(self, id: strawberry.ID) -> Job:
        async with models.get_session() as s:
            sql = select(models.Job).where(models.Job.id == id)
            val = (await s.execute(sql)).scalars().first()
        return Job.marshal(val)  

    # EmbeddingSet
    @strawberry.field
    async def embedding_sets(self) -> list[EmbeddingSet]:
        async with models.get_session() as s:
            sql = select(models.EmbeddingSet)
            result = (await s.execute(sql)).scalars().unique().all()
        return [EmbeddingSet.marshal(loc) for loc in result]

    @strawberry.field
    async def embedding_set(self, id: strawberry.ID) -> EmbeddingSet:
        async with models.get_session() as s:
            sql = select(models.EmbeddingSet).where(models.EmbeddingSet.id == id)
            val = (await s.execute(sql)).scalars().first()
        return EmbeddingSet.marshal(val)  

    # ProjectionSet
    @strawberry.field
    async def projection_sets(self, filter: FilterProjectionSets = None) -> list[ProjectionSet]:
        async with models.get_session() as s:
            sql = select(models.ProjectionSet)

            if filter and filter.project_id is not None:
                sql = sql.filter(models.ProjectionSet.project_id == filter.project_id)

            result = (await s.execute(sql)).scalars().unique().all()
        return [ProjectionSet.marshal(loc) for loc in result]

    def as_dict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    # Projection
    @strawberry.field
    async def projections(self) -> list[Projection]:
        async with models.get_session() as s:
            sql = select(models.Projection)
            db_projection = (await s.execute(sql)).scalars().unique().all()
        return [Projection.marshal(projection) for projection in db_projection]
    
    @strawberry.field
    async def projection(self, id: strawberry.ID) -> Projection:
        async with models.get_session() as s:
            sql = select(models.Projection).where(models.Projection.id == id)
            val = (await s.execute(sql)).scalars().first()
        return Projection.marshal(val)  

    # Embedding
    @strawberry.field
    async def embeddings(self) -> list[Embedding]:
        async with models.get_session() as s:
            sql = select(models.Embedding)
            db_embeddings = (await s.execute(sql)).scalars().unique().all()
        return [Embedding.marshal(loc) for loc in db_embeddings]

    @strawberry.field
    async def embedding(self, id: strawberry.ID) -> Embedding:
        async with models.get_session() as s:
            sql = select(models.Embedding).where(models.Embedding.id == id)
            db_tasks = await s.execute(sql)
            val = db_tasks.scalars().first()
        return Embedding.marshal(val)  

    # pagination
    embeddings_by_page: List[Embedding] = strawberry.field(resolver=get_embeddings)