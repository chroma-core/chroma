import time
import pytest

@pytest.fixture
def anyio_backend():
    return "asyncio"


# def something(duration=0.000001):
#     """
#     Function that needs some serious benchmarking.
#     """
#     time.sleep(duration*2)
#     # You may return anything you want, like the result of a computation
#     return 123

# @pytest.mark.anyio
# def test_my_stuff(benchmark):
#     # benchmark something
#     result = benchmark(something)

#     # Extra code, to verify that the run completed correctly.
#     # Sometimes you may want to check the result, fast functions
#     # are no good if they return incorrect results :-)
#     assert result == 123

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
            "embedding_data": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
            "input_uri": ["https://example.com", "https://example.com"],
            "dataset": ["training", "training"],
            "category_name": ["person", "person"],
            "space_key": ["test_space", "test_space"],
        },
    )

@pytest.mark.anyio
async def benchmark_add_to_db(benchmark):
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = benchmark(await(post_batch_records(ac)))
