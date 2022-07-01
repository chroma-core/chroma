from time import sleep
from celery import Celery
from celery.utils.log import get_task_logger
from celery.utils.log import get_task_logger
from models import EmbeddingSet, Embedding
from sqlalchemy import select
import models
import json
from celery import Celery
import umap
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

# Initialize celery
celery = Celery('tasks', broker='amqp://guest:guest@127.0.0.1:5672//')

# Create logger - enable to display messages on task logger
celery_log = get_task_logger(__name__)

# Celery maintians it's own sync connection to our database
# instead of using the graphql API, it connects directly.
engine = create_engine(
    "sqlite:///./chroma.db", 
    # convert_unicode=True,
    # pool_recycle=3600, 
    # pool_size=10
)
db_session = scoped_session(
    sessionmaker(
        autocommit=False, 
        autoflush=False, 
        bind=engine
    )
)

# Project high-dimensional vectors to 2-D via UMAP (https://umap-learn.readthedocs.io)
def umap_project(vectors):
    reducer = umap.UMAP()
    reducer.fit(vectors)
    projection = reducer.transform(vectors)
    return projection

class SqlAlchemyTask(celery.Task):
    """An abstract Celery Task that ensures that the connection the the
    database is closed on task completion"""
    abstract = True

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        db_session.remove()

@celery.task(base=SqlAlchemyTask, max_retries=3, default_retry_delay=60)
def process_embeddings(embedding_set_id):

    celery_log.info(f"Started processing embeddings for embedding set: " + str(embedding_set_id))

    sql = select(models.EmbeddingSet).where(models.EmbeddingSet.id == embedding_set_id)
    embedding_set = (db_session.execute(sql)).scalars().first()
    sql = select(models.Embedding).where(models.Embedding.embedding_set == embedding_set)
    embeddings = (db_session.execute(sql)).scalars().unique().all()

    vectors = [json.loads(emb.data) for emb in embeddings]

    celery_log.info(f"Fetched data")

    projections = umap_project(vectors)

    celery_log.info(f"Calculated projections")

    # TODO: adding these records actually takes a quite a while, look for opps to speed up
    # create the projection set
    projection_set = models.ProjectionSet(embedding_set=embedding_set)
    db_session.add(projection_set)
    db_session.commit()

    # create the projections
    new_projections = []
    for index, projection in enumerate(projections):
        new_projections.append(models.Projection(
            x=projection[0],
            y=projection[1],
            projection_set=projection_set,
            embedding=embeddings[index]
        ))

    db_session.add_all(new_projections)
    db_session.commit()

    celery_log.info(f"Processing embeddings complete")
    return {"message": f"Projections completed"}