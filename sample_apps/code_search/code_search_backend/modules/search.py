"""

"""

from modules.chunking import CodeChunk
import util

from chromadb.api.models.Collection import Collection
from chromadb.api.types import Where


def semantic_search_using_chroma(
    query: util.Query, collection: Collection
) -> list[CodeChunk]:
    metadata_filters: Where = {
        filter.key: filter.value
        for filter in query.filters
        if isinstance(filter, util.MetadataFilter)
    }
    # regex_filters = [filter.to_filter() for filter in query.filters if isinstance(filter, util.RegexFilter)]
    result = collection.query(
        query_texts=[query.natural_language_query],
        n_results=20,
        where=metadata_filters if metadata_filters else None,
    )
    assert result["documents"] != None and result["metadatas"] != None
    output = []
    for doc, metadata in zip(result["documents"][0], result["metadatas"][0]):
        output.append(
            CodeChunk(
                source_code=doc,
                language=str(metadata["language"]),
                name=str(metadata["name"]),
                file_path=str(metadata["file_path"]),
                start_line=int(metadata["start_line"] or 0),  # TODO: Make this better
            )
        )
    return output


if __name__ == "__main__":
    raise Exception("search.py is not meant to be run directly.")
