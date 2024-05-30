import re
from typing import Iterable, Literal, Optional, List, Union, Any
from chromadb.api.types import Chunker, Document, Documents

import logging

logger = logging.getLogger(__name__)


class DefaultTextChunker(Chunker[Documents]):
    def __init__(self, max_chunk_size: int = 1024, chunk_overlap: int = 0):
        self.max_chunk_size = max_chunk_size
        self.chunk_overlap = chunk_overlap

    def _split_text_with_regex(
        self,
        text: str,
        separator: str,
        keep_separator: Union[bool, Literal["start", "end"]],
    ) -> List[str]:
        # Now that we have the separator, split the text
        if separator:
            if keep_separator:
                # The parentheses in the pattern keep the delimiters in the result.
                _splits = re.split(f"({separator})", text)
                splits = (
                    (
                        [
                            _splits[i] + _splits[i + 1]
                            for i in range(0, len(_splits) - 1, 2)
                        ]
                    )
                    if keep_separator == "end"
                    else (
                        [_splits[i] + _splits[i + 1] for i in range(1, len(_splits), 2)]
                    )
                )
                if len(_splits) % 2 == 0:
                    splits += _splits[-1:]
                splits = (
                    (splits + [_splits[-1]])
                    if keep_separator == "end"
                    else ([_splits[0]] + splits)
                )
            else:
                splits = re.split(separator, text)
        else:
            splits = list(text)
        return [s for s in splits if s != ""]

    def _join_docs(self, docs: List[str], separator: str) -> Optional[str]:
        text = separator.join(docs)
        text = text.strip()
        if text == "":
            return None
        else:
            return text

    def _merge_splits(
        self,
        splits: Iterable[str],
        separator: str,
        max_chunk_size: int,
        chunk_overlap: int,
    ) -> List[str]:
        # We now want to combine these smaller pieces into medium size
        # chunks to send to the LLM.
        separator_len = len(separator)

        docs = []
        current_doc: List[str] = []
        total = 0
        for d in splits:
            _len = len(d)
            if (
                total + _len + (separator_len if len(current_doc) > 0 else 0)
                > max_chunk_size
            ):
                if total > max_chunk_size:
                    logger.warning(
                        f"Created a chunk of size {total}, "
                        f"which is longer than the specified {max_chunk_size}"
                    )
                if len(current_doc) > 0:
                    doc = self._join_docs(current_doc, separator)
                    if doc is not None:
                        docs.append(doc)
                    # Keep on popping if:
                    # - we have a larger chunk than in the chunk overlap
                    # - or if we still have any chunks and the length is long
                    while total > chunk_overlap or (
                        total + _len + (separator_len if len(current_doc) > 0 else 0)
                        > max_chunk_size
                        and total > 0
                    ):
                        total -= len(current_doc[0]) + (
                            separator_len if len(current_doc) > 1 else 0
                        )
                        current_doc = current_doc[1:]
            current_doc.append(d)
            total += _len + (separator_len if len(current_doc) > 1 else 0)
        doc = self._join_docs(current_doc, separator)
        if doc is not None:
            docs.append(doc)
        return docs

    def _split_document(
        self,
        document: Document,
        separators: List[str],
        max_chunk_size: int,
        chunk_overlap: int,
        keep_separator: Union[bool, Literal["start", "end"]],
    ) -> Documents:
        """Split incoming text and return chunks."""
        final_chunks = []
        # Get appropriate separator to use
        separator = separators[-1]
        new_separators = []
        for i, _s in enumerate(separators):
            _separator = re.escape(_s)
            if _s == "":
                separator = _s
                break
            if re.search(_separator, document):
                separator = _s
                new_separators = separators[i + 1 :]
                break

        _separator = re.escape(separator)
        splits = self._split_text_with_regex(document, _separator, keep_separator)

        # Now go merging things, recursively splitting longer texts.
        _good_splits = []
        _separator = "" if keep_separator else separator
        for s in splits:
            if len(s) < max_chunk_size:
                _good_splits.append(s)
            else:
                if _good_splits:
                    merged_text = self._merge_splits(
                        splits=_good_splits,
                        separator=_separator,
                        max_chunk_size=max_chunk_size,
                        chunk_overlap=chunk_overlap,
                    )
                    final_chunks.extend(merged_text)
                    _good_splits = []
                if not new_separators:
                    final_chunks.append(s)
                else:
                    other_info = self._split_document(
                        document=s,
                        separators=new_separators,
                        max_chunk_size=max_chunk_size,
                        chunk_overlap=chunk_overlap,
                        keep_separator=keep_separator,
                    )
                    final_chunks.extend(other_info)
        if _good_splits:
            merged_text = self._merge_splits(
                splits=_good_splits,
                separator=_separator,
                max_chunk_size=max_chunk_size,
                chunk_overlap=chunk_overlap,
            )
            final_chunks.extend(merged_text)
        return final_chunks

    def __call__(
        self,
        input: Documents,
        **kwargs: Any,
    ) -> List[Documents]:
        max_chunk_size = kwargs.get("max_chunk_size", None)
        chunk_overlap = kwargs.get("chunk_overlap", None)
        separators = kwargs.get("separators", None)

        if max_chunk_size is None:
            max_chunk_size = self.max_chunk_size
        if chunk_overlap is None:
            chunk_overlap = self.chunk_overlap

        if separators is None:
            separators = ["\n\n", "\n", ".", " ", ""]

        return [
            self._split_document(
                document=doc,
                separators=separators,
                max_chunk_size=max_chunk_size,
                chunk_overlap=chunk_overlap,
                keep_separator="end",
            )
            for doc in input
        ]
