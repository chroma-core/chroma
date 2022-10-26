# use pytest to test chroma_client

from chroma_client import Chroma
import pytest
import time
from httpx import AsyncClient
# from ..api import app # this wont work because i moved the file

@pytest.fixture
def anyio_backend():
    return 'asyncio'

def test_init():
    chroma = Chroma()
    assert chroma._api_url == "http://localhost:8000/api/v1"


# TODO: I am not sure how to test this
# The url AsyncClient makes is not real, so we cant pass it
# @pytest.mark.anyio
# async def test_count():
#     # create a client for app and then use the api url for chroma_client chroma
#     async with AsyncClient(app=app, base_url="http://test") as ac:
#         response = await ac.get("/api/v1")
#         chroma = Chroma(url="http://test/api/v1")
#         response = await chroma.count()
#         raise Exception("response" + response)

