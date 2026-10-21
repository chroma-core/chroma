from chromadb.config import System


def test_system_fixture_resolves_for_every_default_fixture_name(
    system: System,
) -> None:
    """Regression test for #7395.

    `system` (conftest.py) is parametrized over `filtered_fixture_names()`.
    If any of those names doesn't match an actually-registered fixture,
    pytest fails at fixture setup instead of running the test. Since `api`
    and `client` both depend on `system`, a broken name there breaks most
    of the suite for anyone running plain `pytest` without special
    environment variables set, not just this test.
    """
    assert system is not None
