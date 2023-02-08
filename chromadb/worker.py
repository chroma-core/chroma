import os
import time
import random
import chromadb
from celery import Celery

celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")

@celery.task(name="create_task")
def create_task(task_type):
    time.sleep(int(task_type) * 10)
    return True

@celery.task(name="heavy_offline_analysis")
def heavy_offline_analysis(collection_name):
    db = chromadb.get_db()

    embedding_rows = db.fetch({"collection_name": collection_name})

    uuids = []
    custom_quality_scores = []

    for row in embedding_rows:
        uuids.append(row[get_col_pos("uuid")])
        custom_quality_scores.append(random.random())

    spaces = [collection_name] * len(uuids)

    db.delete_results(collection_name)
    db.add_results(spaces, uuids, custom_quality_scores)

    return "Wrote custom quality scores to database"
