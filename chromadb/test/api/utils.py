def approx_equal(a, b, tolerance=1e-6) -> bool:
    return abs(a - b) < tolerance

def vector_approx_equal(a, b, tolerance: float = 1e-6) -> bool:
    if len(a) != len(b):
        return False
    return all([approx_equal(a, b, tolerance) for a, b in zip(a, b)])

initial_records = {
    "embeddings": [[0, 0, 0], [1.2, 2.24, 3.2], [2.2, 3.24, 4.2]],
    "ids": ["id1", "id2", "id3"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2},
        {"string_value": "three"},
    ],
    "documents": [
        "this document is first",
        "this document is second",
        "this document is third",
    ],
}

new_records = {
    "embeddings": [[3.0, 3.0, 1.1], [3.2, 4.24, 5.2]],
    "ids": ["id1", "id4"],
    "metadatas": [
        {"int_value": 1, "string_value": "one_of_one", "float_value": 1.001},
        {"int_value": 4},
    ],
    "documents": [
        "this document is even more first",
        "this document is new and fourth",
    ],
}


minimal_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
}

bad_dimensionality_records = {
    "embeddings": [[1.1, 2.3], [1.2, 2.24]],  # Different dimensionality
    "ids": ["id1", "id2"],
}

metadata_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2},
    ],
}

records = {
    "embeddings": [[0, 0, 0], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2},
    ],
    "documents": ["this document is first", "this document is second"],
}
