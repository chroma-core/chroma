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
