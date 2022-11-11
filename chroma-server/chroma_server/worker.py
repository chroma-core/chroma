import os
import time
import random
from celery import Celery
from chroma_server.db.clickhouse import Clickhouse, get_col_pos

celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")

@celery.task(name="create_task")
def create_task(task_type):
    time.sleep(int(task_type) * 10)
    return True

@celery.task(name="heavy_offline_analysis")
def heavy_offline_analysis(space_key):
    task_db_conn = Clickhouse()
    embedding_rows = task_db_conn.fetch({"space_key": space_key})

    uuids = []
    custom_quality_scores = []
    
    for row in embedding_rows:
        uuids.append(row[get_col_pos("uuid")])
        custom_quality_scores.append(random.random())

    spaces = [space_key] * len(uuids)

    task_db_conn.delete_results(space_key)
    task_db_conn.add_results(spaces, uuids, custom_quality_scores)
    
    return "Wrote custom quality scores to database"
