
import databases
import ormar
import time

import sqlalchemy
import ormar_postgres_extensions as ormar_pg_ext
from chroma_server.db.abstract import Database

import psycopg2
from io import BytesIO
from struct import pack

# from .config import settings
from chroma_server.utils.config.settings import get_settings

database = databases.Database(get_settings().DATABASE_URL)
metadata = sqlalchemy.MetaData()
from sqlalchemy import Column, Integer, String, ARRAY, Float

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class EmbeddingSqlAlchemy(Base):
    __tablename__ = "embeddings"
    id = Column(Integer, primary_key=True)
    embedding_data = Column(ARRAY(Float))
    dataset = Column(String)
    category_name = Column(String)
    input_uri = Column(String)
    custom_quality_score = Column(Float)

class BaseMeta(ormar.ModelMeta):
    metadata = metadata
    database = database

class User(ormar.Model):
    class Meta(BaseMeta):
        tablename = "users"

    id: int = ormar.Integer(primary_key=True)
    email: str = ormar.String(max_length=128, unique=True, nullable=False)
    active: bool = ormar.Boolean(default=True, nullable=False)

class Embedding(ormar.Model):
    class Meta(BaseMeta):
        tablename = "embeddings"

    id: int = ormar.Integer(primary_key=True)
    embedding_data: list = ormar_pg_ext.ARRAY(item_type=sqlalchemy.Float(), nullable=True)
    input_uri: str = ormar.String(max_length=256)
    dataset: str = ormar.String(max_length=256)
    custom_quality_score: float = ormar.Float(nullable=True)
    category_name: str = ormar.String(max_length=256)

engine = sqlalchemy.create_engine(get_settings().DATABASE_URL)
metadata.create_all(engine)


class Postgres(Database):
#     _conn = None

    def __init__(self):
        pass

    async def add_batch2(self, embedding_data, input_uri, dataset=None, custom_quality_score=None, category_name=None):
        return

    async def add_batch(self, embedding_data, input_uri, dataset=None, custom_quality_score=None, category_name=None):
        '''
        Add embeddings to the database
        This accepts both a single input and a list of inputs
        '''
        preprocessing = [] 
        
        for i in range(len(embedding_data)):
            preprocessing.append({
                # "embedding_data":embedding_data[i],
                "input_uri":input_uri[i],
                "dataset":dataset[i],
                "category_name":category_name[i],
            })

        # t0 = time.time()
        # engine.execute(
        #     EmbeddingSqlAlchemy.__table__.insert(),
        #     preprocessing
        # )
        # print(
        #     "SQLAlchemy Core: Total time for " + str(len(preprocessing)) +
        #     " records " + str(time.time() - t0) + " secs")

   

        return
        
    async def count(self):
        # count the number of embeddings in the database
        no_of_embeddings = await Embedding.objects.count()
        return no_of_embeddings

    def update(self, data): # call this update_custom_quality_score! that is all it does
        '''
        I was not able to figure out (yet) how to do a bulk update in duckdb
        This is going to be fairly slow
        '''
        pass

    async def fetch(self, where_filter={}, sort=None, limit=None):
        # fetch embeddings from database with filter, sort, and limit
        return await Embedding.objects.limit(100).all()
        # return Embedding.objects.filter(**where_filter).order_by(sort).limit(limit)

    def delete_batch(self, batch):
        raise NotImplementedError

    def persist(self):
        '''
        Persist the database to disk
        '''
        pass

    def load(self, path=".chroma/chroma.parquet"):
        '''
        Load the database from disk
        '''
        pass

    def get_by_ids(self, ids=list):
        # get embeddings by id
        return Embedding.objects.filter(id__in=ids)