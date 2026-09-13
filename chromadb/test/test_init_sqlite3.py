"""Tests for sqlite3 version check in chromadb/__init__.py.

Regression tests for https://github.com/chroma-core/chroma/issues/7402
"""
import pytest
from unittest.mock import patch, MagicMock


def test_sqlite3_version_check_provides_clear_instructions() -> None:
    """When sqlite3 is too old, error message should provide clear instructions."""
    # This test verifies the error message content
    # We can't easily test the actual import since we have a valid sqlite3,
    # but we can verify the error message format by reading the source code
    import chromadb.__init__ as init_module
    import inspect

    source = inspect.getsource(init_module)

    # Verify the error message includes installation instructions
    assert "pip install pysqlite3-binary" in source, (
        "Error message should include pip install command"
    )
    assert "__import__('pysqlite3')" in source, (
        "Error message should include pysqlite3 import"
    )
    assert "sys.modules['sqlite3']" in source, (
        "Error message should include sys.modules swap"
    )


def test_no_subprocess_pip_install_in_init() -> None:
    """The init module should NOT attempt to pip install anything."""
    import chromadb.__init__ as init_module
    import inspect

    source = inspect.getsource(init_module)

    # Verify no subprocess pip install
    assert "subprocess.check_call" not in source, (
        "Init module should not use subprocess to pip install"
    )
    assert 'sys.executable, "-m", "pip"' not in source, (
        "Init module should not invoke pip via sys.executable"
    )


def test_is_in_colab_still_works() -> None:
    """is_in_colab() should still be available for telemetry."""
    from chromadb import is_in_colab

    result = is_in_colab()
    assert isinstance(result, bool)
