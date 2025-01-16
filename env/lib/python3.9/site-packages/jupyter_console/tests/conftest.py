import pytest


@pytest.fixture(autouse=True)
def env_setup(monkeypatch):
    monkeypatch.setenv("JUPYTER_CONSOLE_TEST", "1")
