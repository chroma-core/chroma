from api import _datapoints
from utils import umap_project

def resolve_datapoints(obj, info):
     payload = {
         "success": True,
         "datapoints": _datapoints
     }
     return payload
