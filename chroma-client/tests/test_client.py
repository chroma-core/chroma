import chroma_client
import pytest

def test_init():
    assert chroma_client.init() == True
