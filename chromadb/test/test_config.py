import chromadb.config
import pytest
import os
from unittest.mock import patch


class TestDBComponent():
    __test__ = False

    def __init__(self, settings):
        pass

class TestAPIComponent():
    __test__ = False

    def __init__(self, settings):
        self.db = chromadb.config.get_component(settings, 'chroma_db_impl')
        pass



def test_missing_component():
    settings = chromadb.config.Settings(chroma_api_impl="chromadb.no.such.NoSuchComponent")
    with pytest.raises(Exception) as e:
        chromadb.config.get_component(settings, "chroma_api_impl")


def test_get_component():
    settings = chromadb.config.Settings(chroma_api_impl="chromadb.test.test_config.TestAPIComponent",
                                        chroma_db_impl="chromadb.test.test_config.TestDBComponent")
    api = chromadb.config.get_component(settings, "chroma_api_impl")
    assert isinstance(api, chromadb.test.test_config.TestAPIComponent)
    assert isinstance(api.db, chromadb.test.test_config.TestDBComponent)


@patch.dict(os.environ, {}, clear=True)
def test_get_component_with_missing_config():
     settings = chromadb.config.Settings(chroma_api_impl="chromadb.test.test_config.TestConfigurableAPIComponent")
     with pytest.raises(ValueError) as e:
         api = chromadb.config.get_component(settings, "chroma_api_impl")



class TestConfigurableAPIComponent():
    __test__ = False

    host = None

    def __init__(self, settings):
        settings.validate('chroma_server_host')
        self.host = settings.chroma_server_host
        pass


