# from __future__ import absolute_import
import celery

from .db import db_session
from celery.utils.log import get_task_logger
from models import EmbeddingSet, Embedding
from .celery_agent import celery_instance
from sqlalchemy import select
import models
import json

import umap

# Project high-dimensional vectors to 2-D via UMAP (https://umap-learn.readthedocs.io)
def umap_project(vectors):
    reducer = umap.UMAP()
    reducer.fit(vectors)
    projection = reducer.transform(vectors)
    return projection

celery_log = get_task_logger(__name__)

class SqlAlchemyTask(celery.Task):
    """An abstract Celery Task that ensures that the connection the the
    database is closed on task completion"""
    abstract = True

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        db_session.remove()

@celery_instance.task(base=SqlAlchemyTask, max_retries=3, default_retry_delay=60)
def process_embeddings(embedding_set_id):

    sql = select(models.EmbeddingSet).where(models.EmbeddingSet.id == embedding_set_id)
    embedding_set = (db_session.execute(sql)).scalars().first()
    sql = select(models.Embedding).where(models.Embedding.embedding_set == embedding_set)
    embeddings = (db_session.execute(sql)).scalars().unique().all()

    vectors = [json.loads(emb.data) for emb in embeddings]
    projections = umap_project(vectors)

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

    # print("app utils: packing datapoints")
    # annotated_projections = zip(projections, raw_embeddings)
    # datapoints = [{"x": proj[0], "y": proj[1], "metadata": json.dumps({ "class": raw_emb["label"], "type": raw_emb["inference_identifier"],"ml_model_version": "v2"})} for proj, raw_emb in annotated_projections]
    # print("datapoints!" + str(datapoints))
