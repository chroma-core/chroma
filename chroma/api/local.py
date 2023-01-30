import time
from chroma.api import API
from chroma.utils.sampling import score_and_store, get_sample
from chroma.server.utils.telemetry.capture import Capture


class LocalAPI(API):
    def __init__(self, settings, db):
        self._db = db
        self._chroma_telemetry = Capture()
        self._chroma_telemetry.capture("server-start")

    def heartbeat(self):
        return int(1000 * time.time_ns())

    def add(
        self,
        model_space,
        embedding,
        input_uri=None,
        dataset=None,
        metadata=None
    ):

        model_space = model_space or self.get_model_space()

        number_of_embeddings = len(embedding)

        if isinstance(model_space, str):
            model_space = [model_space] * number_of_embeddings
        elif len(model_space) == 1:
            model_space = [model_space[0]] * number_of_embeddings
        else:
            model_space = model_space

        if isinstance(dataset, str):
            ds = [dataset] * number_of_embeddings
        elif len(dataset) == 1:
            ds = [dataset[0]] * number_of_embeddings
        else:
            ds = dataset

        if metadata is None:
            metadata = [{} for _ in range(number_of_embeddings)]

        # convert all metadata values to strings : TODO: handle this better
        # this is currently here because clickhouse-driver does not support json
        for m in metadata:
            for k, v in m.items():
                m[k] = str(v)

        # change input_uri to strings if not strings
        input_uri = [str(x) for x in input_uri]

        self._db.add(model_space, embedding, input_uri, ds, metadata)

        return True

    def fetch(self, where={}, sort=None, limit=None, offset=None, page=None, page_size=None):

        if page and page_size:
            offset = (page - 1) * page_size
            limit = page_size

        return self._db.fetch(where, sort, limit, offset)

    def delete(self, where={}):

        where = self.where_with_model_space(where)
        deleted_uuids = self._db.delete(where)
        return deleted_uuids

    def count(self, model_space=None):

        model_space = model_space or self._model_space
        return self._db.count(model_space=model_space)

    def reset(self):

        self._db.reset()
        return True

    def get_nearest_neighbors(self, embedding, n_results=10, where={}):

        where = self.where_with_model_space(where)
        return self._db.get_nearest_neighbors(where, embedding, n_results)

    def raw_sql(self, raw_sql):

        return self._db.raw_sql(raw_sql)

    def create_index(self, model_space=None, dataset_name=None):

        model_space = model_space or self.get_model_space()

        self._db.create_index(
            model_space=model_space, dataset_name=dataset_name
        )
        return True
