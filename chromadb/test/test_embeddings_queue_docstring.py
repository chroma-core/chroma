"""Regression tests for ``chromadb.db.mixins.embeddings_queue``.

These tests guard against documentation regressions in the module
itself. They are intentionally lightweight — no database or network
required — so they can run in any environment that has the package
importable.
"""

from __future__ import annotations

import re

from chromadb.db.mixins.embeddings_queue import SqlEmbeddingsQueue


# Match a duplicated word like "the the", "of of", "a a" preceded by a
# word boundary and a space, and case-insensitive. We deliberately keep
# the pattern narrow (two identical words separated by a single space)
# so it does not flag legitimate repeated phrases in other contexts.
_DUP_WORD_RE = re.compile(r"\b(\w+)\s+\1\b", re.IGNORECASE)


def _docstring_has_duplicate_word(docstring: str | None) -> list[str]:
    """Return the list of duplicated words found in *docstring*."""
    if not docstring:
        return []
    return [match.group(1) for match in _DUP_WORD_RE.finditer(docstring)]


def test_sql_embeddings_queue_docstring_has_no_duplicate_words() -> None:
    """The ``SqlEmbeddingsQueue`` docstring must not contain duplicate words.

    Regression test for https://github.com/chroma-core/chroma/issues/7298
    where the original docstring read ``... listen to the the database ...``.
    """
    duplicates = _docstring_has_duplicate_word(SqlEmbeddingsQueue.__doc__)
    assert not duplicates, (
        "SqlEmbeddingsQueue docstring contains duplicated word(s): "
        f"{duplicates!r}"
    )


def test_sql_embeddings_queue_docstring_specific_phrase() -> None:
    """The exact ``the the`` typo from issue #7298 must not reappear."""
    assert SqlEmbeddingsQueue.__doc__ is not None
    assert " the the " not in SqlEmbeddingsQueue.__doc__, (
        "SqlEmbeddingsQueue docstring still contains the 'the the' typo "
        "from issue #7298"
    )
