"""
These functions match what the spec of hnswlib is.
"""
import numpy as np
from numpy.typing import ArrayLike


def l2(x: ArrayLike, y: ArrayLike) -> float:
    return np.linalg.norm(x - y) ** 2


def cosine(x: ArrayLike, y: ArrayLike) -> float:
    # This epsilon is used to prevent division by zero, and the value is the same
    # https://github.com/nmslib/hnswlib/blob/359b2ba87358224963986f709e593d799064ace6/python_bindings/bindings.cpp#L238
    NORM_EPS = 1e-30
    return 1 - np.dot(x, y) / (
        (np.linalg.norm(x) + NORM_EPS) * (np.linalg.norm(y) + NORM_EPS)
    )


def ip(x: ArrayLike, y: ArrayLike) -> float:
    return 1 - np.dot(x, y)
