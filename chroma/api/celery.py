from chroma.api.local import LocalAPI
from chroma.worker import heavy_offline_analysis
from celery.result import AsyncResult


class CeleryAPI(LocalAPI):

    def __init__(self, settings, db):
        super().__init__()

