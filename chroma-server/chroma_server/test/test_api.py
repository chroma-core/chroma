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
    assert isinstance(response.json()["nanosecond heartbeat"], int)

async def post_batch_records(ac):
    return await ac.post(
        "/api/v1/add",
        json={
            "embedding": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
            "input_uri": ["https://example.com", "https://example.com"],
            "dataset": ["training", "training"],
            "inference_class": ["knife", "person"],
            "model_space": ["test_space", "test_space"],
            "label_class": ["person", "person"],
        },
    )

async def post_batch_records_minimal(ac):
    return await ac.post(
        "/api/v1/add",
        json={
            "embedding": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
            "input_uri": ["https://example.com", "https://example.com"],
            "dataset": "training",
            "inference_class": ["person", "person"],
            "model_space": "test_space"
        }, #label_class left off on purpose
    )


@pytest.mark.anyio
async def test_add_batch():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        await ac.post("/api/v1/reset")
        response = await post_batch_records(ac)
        assert response.status_code == 201
        assert response.json() == {"response": "Added records to database"}
        response = await ac.get("/api/v1/count", params={"model_space": "test_space"})
        assert response.json() == {"count": 2}

   
@pytest.mark.anyio
async def test_add_batch_minimal():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        await ac.post("/api/v1/reset")
        response = await post_batch_records_minimal(ac)
        assert response.status_code == 201
        assert response.json() == {"response": "Added records to database"} 
        response = await ac.get("/api/v1/count", params={"model_space": "test_space"})
        assert response.json() == {"count": 2}

@pytest.mark.anyio
async def test_fetch_from_db():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        await ac.post("/api/v1/reset")
        await post_batch_records(ac)
        params = {"where": {"model_space": "test_space"}}
        response = await ac.post("/api/v1/fetch", json=params)
    assert response.status_code == 200
    assert len(response.json()['embedding']) == 2

@pytest.mark.anyio
async def test_count_from_db():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        await ac.post("/api/v1/reset")  # reset db
        await post_batch_records(ac)
        response = await ac.get("/api/v1/count", params={"model_space": "test_space"})
    assert response.status_code == 200
    assert response.json() == {"count": 2}

@pytest.mark.anyio
async def test_reset_db():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        await ac.post("/api/v1/reset")
        await post_batch_records(ac)
        response = await ac.get("/api/v1/count", params={"model_space": "test_space"})
        assert response.json() == {"count": 2}
        response = await ac.post("/api/v1/reset")
        assert response.json() == True
        response = await ac.get("/api/v1/count", params={"model_space": "test_space"})
        assert response.json() == {"count": 0}

@pytest.mark.anyio
async def test_get_nearest_neighbors():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        await ac.post("/api/v1/reset")
        await post_batch_records(ac)
        await ac.post("/api/v1/create_index", json={"model_space": "test_space"})
        response = await ac.post(
            "/api/v1/get_nearest_neighbors", json={"embedding": [1.1, 2.3, 3.2], "n_results": 1, "where":{"model_space": "test_space"}}
        )
    assert response.status_code == 200
    assert len(response.json()["ids"]) == 1

@pytest.mark.anyio
async def test_get_nearest_neighbors_filter():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        await ac.post("/api/v1/reset")
        await post_batch_records(ac)
        await ac.post("/api/v1/create_index", json={"model_space": "test_space"})
        response = await ac.post(
            "/api/v1/get_nearest_neighbors",
            json={
                "embedding": [1.1, 2.3, 3.2],
                "n_results": 1,
                "where":{
                    "dataset": "training",
                    "inference_class": "monkey",
                    "model_space": "test_space",
                }
            },
        )
    assert response.status_code == 200
    assert "error" in list(response.json().keys())

@pytest.mark.anyio
async def test_process():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        await ac.post("/api/v1/reset")
        await post_batch_records(ac)
        response = await ac.post("/api/v1/create_index", json={"model_space": "test_space"})
    assert response.status_code == 200

# test delete
@pytest.mark.anyio
async def test_delete():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        await ac.post("/api/v1/reset")
        await post_batch_records(ac)
        response = await ac.get("/api/v1/count", params={"model_space": "test_space"})
        assert response.json() == {"count": 2}
        response = await ac.post("/api/v1/delete", json={"where": {"model_space": "test_space"}})
        response = await ac.get("/api/v1/count", params={"model_space": "test_space"})
        assert response.json() == {"count": 0}

@pytest.mark.anyio
async def test_delete_with_index():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        await ac.post("/api/v1/reset")
        await post_batch_records(ac)
        response = await ac.get("/api/v1/count", params={"model_space": "test_space"})
        assert response.json() == {"count": 2}
        await ac.post("/api/v1/create_index", json={"model_space": "test_space"})
        response = await ac.post(
            "/api/v1/get_nearest_neighbors", json={"embedding": [1.1, 2.3, 3.2], "n_results": 1, "where":{"model_space": "test_space"}}
        )
        assert response.json()['embeddings']['inference_class']['0'] == 'knife'
        response = await ac.post("/api/v1/delete", json={"where": {"model_space": "test_space", "inference_class": "knife"}})
        response = await ac.post(
            "/api/v1/get_nearest_neighbors", json={"embedding": [1.1, 2.3, 3.2], "n_results": 1, "where":{"model_space": "test_space"}}
        )
        assert response.json()['embeddings']['inference_class']['0']  == 'person'

# test calculate results
# @pytest.mark.anyio
# async def test_calculate_results():
#     async with AsyncClient(app=app, base_url="http://test") as ac:
#         await ac.post("/api/v1/reset")
#         await post_batch_records(ac)
#         await ac.post("/api/v1/process", json={"model_space": "test_space"})
#         response = await ac.post(
#             "/api/v1/calculate_results",
#             json={
#                 "model_space": "test_space",
#             },
#         )
#     assert response.status_code == 200
#     assert response.json() == {"ids": [], "distances": []}