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
    targets = [
        json.loads(emb.data)["target"] for emb in embeddings
    ]  # load targets to pass them down to Projections

    celery_log.info(f"Fetched data")

    projections = umap_project(vectors)

    celery_log.info(f"Calculated projections")

    setType = "context"
    if targets[0] != None:
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
            x=projection[0],
            y=projection[1],
            projection_set=projection_set,
            embedding=embedding,
            target=target,
        )
        for projection, embedding, target in zip(projections, embeddings, targets)
    ]
    celery_log.info(f"Created Projections")

    db_session.add_all(new_projections)
    db_session.commit()

    celery_log.info(f"Processing embeddings complete")
    return {"message": f"Projections completed"}


@celery.task(base=SqlAlchemyTask, max_retries=3, default_retry_delay=60)
def compute_class_distances(training_embedding_set_id: int, target_embedding_set_id: int):
    def unpack_annotations(embeddings):
        # Get and unpack inference data
        annotations = [
            json.loads(embedding.datapoint.inference.data)["annotations"]
            for embedding in embeddings
        ]  # Unpack JSON
        annotations = [
            annotation for annotation_list in annotations for annotation in annotation_list
        ]  # Flatten the list

        categories_by_uid = {
            annotation["id"]: annotation["category_id"] for annotation in annotations
        }

        # Unpack embedding data
        embeddings = [json.loads(embedding.data) for embedding in embeddings]

        embedding_vectors_by_category = {}
        for embedding in embeddings:
            data = np.array(embedding["data"])
            category = categories_by_uid[embedding["target"]]
            if category in embedding_vectors_by_category.keys():
                embedding_vectors_by_category[category] = np.append(
                    embedding_vectors_by_category[category], data[np.newaxis, :], axis=0
                )
            else:
                embedding_vectors_by_category[category] = data[np.newaxis, :]

        return embedding_vectors_by_category

    celery_log.info(
        f"Started computing class distances for training set {training_embedding_set_id}, target set {target_embedding_set_id}"
    )

    sql = select(models.Embedding).where(
        models.Embedding.embedding_set_id == training_embedding_set_id
    )
    training_embeddings = (db_session.execute(sql)).scalars().unique().all()
    training_embedding_vectors_by_category = unpack_annotations(training_embeddings)

    inv_covs_by_category = {}
    means_by_category = {}
    for category, embeddings in training_embedding_vectors_by_category.items():
        celery_log.info(f"Computing mean and covariance for label categry {category}")

        # Compute the mean and inverse covariance for computing MHB distance
        print(f"category: {category} samples: {embeddings.shape[0]}")
        if embeddings.shape[0] < (embeddings.shape[1] + 1):
            celery_log.warning(f"not enough samples for stable covariance in category {category}")
            continue
        cov = np.cov(embeddings.transpose())
        try:
            inv_cov = np.linalg.inv(cov)
        except np.linalg.LinAlgError as err:
            celery_log.warning(f"covariance for category {category} is singular")
            continue
        mean = np.mean(embeddings, axis=0)
        inv_covs_by_category[category] = inv_cov
        means_by_category[category] = mean

    sql = (
        select(models.Datapoint)
        .join(models.Embedding)
        .where(models.Embedding.embedding_set_id == target_embedding_set_id)
    )
    target_datapoints = (db_session.execute(sql)).scalars().unique().all()

    # Process each datapoint's inferences individually. This is going to be very slow.
    # This is because there is no way to grab the corresponding metadata off the datapoint.
    # We could instead put it on the embedding directly ?
    for datapoint in target_datapoints:
        inferences = json.loads(datapoint.inference.data)["annotations"]
        embeddings = [json.loads(embedding.data) for embedding in datapoint.embeddings]

        # The last embedding on a datapoint is the context embedding.
        assert len(inferences) == (len(embeddings) - 1), f"{len(inferences)}, {len(embeddings)}"

        for i in range(len(inferences)):
            emb_data = embeddings[i]
            assert inferences[i]["id"] == emb_data["target"]
            category = inferences[i]["category_id"]
            if not category in inv_covs_by_category.keys():
                continue
            mean = means_by_category[category]
            inv_cov = inv_covs_by_category[category]
            delta = np.array(emb_data["data"]) - mean
            squared_mhb = np.sum((delta * np.matmul(inv_cov, delta)), axis=0)
            if squared_mhb < 0:
                celery_log.warning(f"squared distance for category {category} is negative")
                continue
            distance = np.sqrt(squared_mhb)
            inferences[i]["distance_score"] = distance

        datapoint.inference.data = json.dumps({"annotations": inferences})

    db_session.add_all(target_datapoints)
    db_session.commit()
    db_session.close()

    return {"message": "Class distances computed"}
