# type: ignore


def approx_equal(a, b, tolerance=1e-6) -> bool:
    return abs(a - b) < tolerance


def vector_approx_equal(a, b, tolerance: float = 1e-6) -> bool:
    if len(a) != len(b):
        return False
    return all([approx_equal(a, b, tolerance) for a, b in zip(a, b)])
