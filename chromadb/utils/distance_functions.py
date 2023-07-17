from typing import Dict, Callable
import numpy as np
import numpy.typing as npt


# These match what the spec of hnswlib is
# This epsilon is used to prevent division by zero and the value is the same
# https://github.com/nmslib/hnswlib/blob/359b2ba87358224963986f709e593d799064ace6/python_bindings/bindings.cpp#L238
NORM_EPS = 1e-30
distance_functions: Dict[str, Callable[[npt.ArrayLike, npt.ArrayLike], float]] = {
    "l2": lambda x, y: np.linalg.norm(x - y) ** 2,  # type: ignore
    "cosine": lambda x, y: 1 - np.dot(x, y) / ((np.linalg.norm(x) + NORM_EPS) * (np.linalg.norm(y) + NORM_EPS)),  # type: ignore
    "ip": lambda x, y: 1 - np.dot(x, y),  # type: ignore
}

l2 = distance_functions["l2"]
cosine = distance_functions["cosine"]
ip = distance_functions["ip"]
