import os
import pytest
import chromadb
import traceback

def test_ssl_self_signed(client_ssl):
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Skipping test for integration test")
    client_ssl.heartbeat()


def test_ssl_self_signed_without_ssl_verify(client_ssl):
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Skipping test for integration test")
    client_ssl.heartbeat()
    _port = client_ssl._server._settings.chroma_server_http_port
    with pytest.raises(ValueError) as e:
        chromadb.HttpClient(ssl=True, port=_port)
    stack_trace = traceback.format_exception(
        type(e.value), e.value, e.value.__traceback__
    )
    client_ssl.clear_system_cache()
    assert "CERTIFICATE_VERIFY_FAILED" in "".join(stack_trace)
    
# test get_version
def test_get_version(client):
    client.reset()
    version = client.get_version()

    # assert version matches the pattern x.y.z
    import re

    assert re.match(r"\d+\.\d+\.\d+", version)