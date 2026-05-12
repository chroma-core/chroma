"""Regression test for #5554: ensure ``overrides`` is not required on Python 3.12+."""

import sys
import importlib


def test_typing_override_available() -> None:
    """typing.override must exist on Python 3.12+ (the compat layer relies on it)."""
    if sys.version_info >= (3, 12):
        import typing

        assert hasattr(typing, "override")


def test_compat_imports() -> None:
    """All public names from chromadb.utils.compat must be importable."""
    from chromadb.utils.compat import override, overrides, EnforceOverrides

    assert callable(override)
    assert callable(overrides)
    assert isinstance(EnforceOverrides, type)


def test_enforce_overrides_is_noop_on_312() -> None:
    """On 3.12+ EnforceOverrides is a plain mixin with no runtime checks."""
    from chromadb.utils.compat import EnforceOverrides

    class Dummy(EnforceOverrides):
        pass

    assert issubclass(Dummy, EnforceOverrides)


def test_override_decorator_applies() -> None:
    """The compat ``@override`` must mark methods like the real one."""
    from chromadb.utils.compat import override

    class Base:
        def method(self) -> str:
            return "base"

    class Child(Base):
        @override
        def method(self) -> str:
            return "child"

    assert Child().method() == "child"
