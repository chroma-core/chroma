import pytest
import time
from httpx import AsyncClient

from ..api import app


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_root():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/api/v1")
    assert response.status_code == 200
    # assert response is an integer
    assert isinstance(response.json()["nanosecond heartbeat"], int)
    # assert (
    #     abs(response.json()["nanosecond heartbeat"] - int(1000 * time.time_ns())) < 3_000_000_000
    # )  # a billion nanoseconds = 3s


# async def post_one_record(ac):
#     return await ac.post(
#         "/api/v1/add",
#         json={
#             "embedding_data": [1.02, 2.03, 3.03],
#             "input_uri": "https://example.com",
#             "dataset": "coco",
#             "category_name": "person",
#         },
#     )

# space_key: Union[str, list]
# embedding_data: list
# input_uri: Union[str, list]
# dataset: Union[str, list] = None
# custom_quality_score: Union[float, list] = None
# category_name: Union[str, list] = None
async def post_batch_records(ac):
    return await ac.post(
        "/api/v1/add",
        json={
            "embedding_data": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
            "input_uri": ["https://example.com", "https://example.com"],
            "dataset": ["training", "training"],
            "category_name": ["person", "person"],
            "space_key": ["test_space", "test_space"],
        },
    )


@pytest.mark.anyio
async def test_add_to_db():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await post_batch_records(ac)
    assert response.status_code == 201
    assert response.json() == {"response": "Added records to database"}


@pytest.mark.anyio
async def test_add_to_db_batch():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await post_batch_records(ac)
    print(response.json())
    assert response.status_code == 201
    assert response.json() == {"response": "Added records to database"}


# @pytest.mark.anyio
# async def test_fetch_from_db():
#     async with AsyncClient(app=app, base_url="http://test") as ac:
#         await post_batch_records(ac)
#         response = await ac.get("/api/v1/fetch", params={"limit": 1})
#     assert response.status_code == 200
#     assert len(response.json()) == 1


# @pytest.mark.anyio
# async def test_count_from_db():
#     async with AsyncClient(app=app, base_url="http://test") as ac:
#         await ac.get("/api/v1/reset")  # reset db
#         await post_batch_records(ac)
#         response = await ac.get("/api/v1/count", params={"space_key": "test_space"})
#     assert response.status_code == 200
#     assert response.json() == {"count": 2}


# @pytest.mark.anyio
# async def test_reset_db():
#     async with AsyncClient(app=app, base_url="http://test") as ac:
#         await ac.get("/api/v1/reset")
#         await post_batch_records(ac)
#         response = await ac.get("/api/v1/count", params={"space_key": "test_space"})
#         assert response.json() == {"count": 2}
#         response = await ac.get("/api/v1/reset")
#         assert response.json() == True
#         response = await ac.get("/api/v1/count", params={"space_key": "test_space"})
#         assert response.json() == {"count": 0}


# @pytest.mark.anyio
# async def test_get_nearest_neighbors():
#     async with AsyncClient(app=app, base_url="http://test") as ac:
#         await ac.get("/api/v1/reset")
#         await post_batch_records(ac)
#         await ac.get("/api/v1/process", params={"space_key": "test_space"})
#         response = await ac.post(
#             "/api/v1/get_nearest_neighbors", json={"embedding": [1.1, 2.3, 3.2], "n_results": 1, "space_key": "test_space"}
#         )
#     assert response.status_code == 200
#     assert len(response.json()["ids"]) == 1


# @pytest.mark.anyio
# async def test_get_nearest_neighbors_filter():
#     async with AsyncClient(app=app, base_url="http://test") as ac:
#         await ac.get("/api/v1/reset")
#         await post_batch_records(ac)
#         await ac.get("/api/v1/process")
#         response = await ac.post(
#             "/api/v1/get_nearest_neighbors",
#             json={
#                 "embedding": [1.1, 2.3, 3.2],
#                 "n_results": 1,
#                 "dataset": "training",
#                 "category_name": "monkey",
#                 "space_key": "test_space",
#             },
#         )
#     assert response.status_code == 200
#     assert len(response.json()["ids"]) == 0
