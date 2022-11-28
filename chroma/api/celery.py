from chroma.api.local import LocalAPI
from chroma.worker import heavy_offline_analysis
from celery.result import AsyncResult


class CeleryAPI(LocalAPI):

    def __init__(self, settings, db):
        super().__init__()


    def get_status(self, task_id):

        task_result = AsyncResult(task_id)
        result = {
            "task_id": task_id,
            "task_status": task_result.status,
            "task_result": task_result.result
        }

        return result


    def get_results(self, model_space=None, n_results=100):

        model_space = model_space or self._model_space

        if not self._db.has_index(model_space):
            self._db.create_index(model_space)

        results_count = self._db.count_results(model_space)

        if results_count == 0:
            heavy_offline_analysis(model_space)

        return self._db.return_results(model_space, n_results)


