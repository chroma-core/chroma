from time import sleep
from celery import Celery
from celery.utils.log import get_task_logger

# Initialize celery
celery = Celery('tasks', broker='amqp://guest:guest@127.0.0.1:5672//')

# Create logger - enable to display messages on task logger
celery_log = get_task_logger(__name__)

# Create Order - Run Asynchronously with celery
# Example process of long running task
# def run_projections(name, quantity):

@celery.task
def run_projections(embedding_set_id):
    
    # chroma_data_manager = data_manager.ChromaDataManager()
    # print("app utils: fetching embeddings from data_manager")
    # raw_embeddings = chroma_data_manager.get_embeddings_pages()
    # vectors = [emb["data"] for emb in raw_embeddings]

    print("app utils: projecting to datapoints")
    projections = umap_project(vectors)

    print("app utils: packing datapoints")
    annotated_projections = zip(projections, raw_embeddings)
    datapoints = [{"x": proj[0], "y": proj[1], "metadata": json.dumps({ "class": raw_emb["label"], "type": raw_emb["inference_identifier"],"ml_model_version": "v2"})} for proj, raw_emb in annotated_projections]
    return datapoints


    # Display log    
    celery_log.info(f"Order Complete!")
    return {"message": f"Projections completed"}