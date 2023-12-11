import multiprocessing


class Parameter(object):
    def __init__(self, name, default_value, is_static=False, description=None) -> None:
        self.name = name
        self.default_value = default_value
        self.is_static = is_static
        self.description = description


class ParameterDict(object):
    """
    ParameterDict is a class that contains all parameters for ChromaDB. Maybe move this to a config file in the future.
    """

    parameter_dict = {
        "space": Parameter(
            "space",
            "l2",
            True,
            "Function used to calculate distance. Can be l2, ip, or cosine",
        ),
        "construction_ef": Parameter(
            "construction_ef",
            100,
            True,
            "How many nearest neighbor will be returned. Larger for better indexing quality but slower indexing time.",
        ),
        "search_ef": Parameter(
            "search_ef",
            10,
            False,
            "Number of nearest neighbors to explore during search.",
        ),
        "M": Parameter("M", 16, True, "Number of links per node created for index."),
        "num_threads": Parameter(
            "num_threads", multiprocessing.cpu_count(), False, "Number of threads"
        ),
        "resize_factor": Parameter(
            "resize_factor", 1.2, True, "Change index max capacity."
        ),
        "batch_size": Parameter("batch_size", 100, True, "batch size for persistence"),
        "sync_threshold": Parameter(
            "sync_threshold", 1000, True, "controls sync frequency"
        ),
    }
