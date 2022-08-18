from typing import List
from celery import Celery
from celery.utils.log import get_task_logger
from celery.utils.log import get_task_logger
import chroma.app.models as models
from sqlalchemy import select
import json
from celery import Celery
import umap
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from scipy.spatial.distance import mahalanobis

# Initialize celery
celery = Celery("tasks", broker="redis://127.0.0.1:6379/0")

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
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

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
def process_embeddings(embedding_set_ids):

    celery_log.info(f"Started processing embeddings for embedding set: " + str(embedding_set_ids))

    embeddings = []
    for embedding_set_id in embedding_set_ids:
        sql = select(models.EmbeddingSet).where(models.EmbeddingSet.id == embedding_set_id)
        embedding_set = (db_session.execute(sql)).scalars().first()
        sql = select(models.Embedding).where(models.Embedding.embedding_set == embedding_set)
        embeddings = embeddings + ((db_session.execute(sql)).scalars().unique().all())

    vectors = [json.loads(emb.data)["data"] for emb in embeddings]
    targets = [json.loads(emb.data)["target"] for emb in embeddings] # load targets to pass them down to Projections

    celery_log.info(f"Fetched data")

    projections = umap_project(vectors)

    celery_log.info(f"Calculated projections")
    
    setType = "context"
    if (targets[0] != None):
        setType = "object"

    # TODO: adding these records actually takes a quite a while, look for opps to speed up
    # create the projection set
    projection_set = models.ProjectionSet(
        embedding_set=embedding_set, project=embedding_set.dataset.project, setType=setType
    )
    db_session.add(projection_set)
    db_session.commit()

    # create the projections
    new_projections = [
        models.Projection(
            x=projection[0], y=projection[1], projection_set=projection_set, embedding=embedding, target=target
        )
        for projection, embedding, target in zip(projections, embeddings, targets)
    ]
    celery_log.info(f"Created Projections")

    db_session.add_all(new_projections)
    db_session.commit()

    celery_log.info(f"Processing embeddings complete")
    return {"message": f"Projections completed"}


@celery.task(base=SqlAlchemyTask, max_retries=3, default_retry_delay=60)
def compute_class_distances(training_dataset_id: int, target_dataset_id: int):

    celery_log.info(
        f"Started computing class distances for training set {training_dataset_id}, test set {target_dataset_id}"
    )

    # Get the unique labels
    sql = (
        select(models.Label)
        .join(models.Label.datapoint)
        .filter(models.Datapoint.dataset_id == training_dataset_id)
    )
    training_labels = (db_session.execute(sql)).scalars().unique().all()

    unique_label_data = {label.data for label in training_labels}
    for label_data in unique_label_data:
        label_name = json.loads(label_data)["categories"][0]["name"]
        celery_log.info(f"Retrieving training embeddings for label {label_name}")

        # Get the embeddings from the training set corresponding to each label
        sql = (
            select(models.Embedding.data)
            .join(models.Embedding.datapoint)
            .join(models.Datapoint.label)
            .filter(
                (models.Datapoint.dataset_id == training_dataset_id)
                & (models.Label.data == label_data)
            )
        )
        training_embeddings = (db_session.execute(sql)).scalars().unique().all()
        training_vectors = np.array([json.loads(emb) for emb in training_embeddings])

        celery_log.info(f"Computing mean and covariance for label {label_name}")

        # Compute the mean and inverse covariance for computing MHB distance
        cov = np.cov(training_vectors.transpose())
        inv_cov = np.linalg.inv(cov)
        mean = np.mean(training_vectors, axis=0)

        celery_log.info(f"Retrieving target datapoints for {label_name}")

        # Get datapoints from the target dataset to evaluate, where inference matches the label.

        datapoint_ids_subquery = (
            select(models.Datapoint.id)
            .join(models.Datapoint.embeddings)
            .join(models.Datapoint.inference)
            .filter(
                (models.Datapoint.dataset_id == target_dataset_id)
                & (models.Inference.data == label_data)
            )
        )

        sql = select(models.Datapoint).filter(models.Datapoint.id.in_(datapoint_ids_subquery))
        target_datapoints = (db_session.execute(sql)).scalars().unique().all()

        # Compute MHB to the corresponding cluster from the training set
        celery_log.info(f"Loading target embedding vectors for {label_name}")
        sql = (
            select(models.Embedding.data)
            .join(models.Embedding.datapoint)
            .filter(models.Datapoint.id.in_(datapoint_ids_subquery))
        )
        target_embeddings = (db_session.execute(sql)).scalars().unique().all()
        target_vectors = np.array([json.loads(emb) for emb in target_embeddings])

        celery_log.info(f"Computing Mahalanobis distances for {label_name}")
        deltas = (target_vectors - mean).transpose()
        distances = np.sqrt(np.sum((deltas * np.matmul(inv_cov, deltas)), axis=0))
        for datapoint, distance in zip(target_datapoints, distances):
            datapoint.metadata_ = json.dumps({"distance_score": distance})

        celery_log.info(f"Writing distances for {label_name}")
        db_session.add_all(target_datapoints)
        db_session.commit()
        db_session.close()

    return {"message": "Class distances computed"}
