"""

"""

from modules.chunking import CodeChunk
import util

import chromadb
from chromadb.api.types import Documents, EmbeddingFunction, Embeddable

code_collection = util.get_chroma_collection()

def semantic_search_using_chroma(query: util.Query) -> list[CodeChunk]:
    metadata_filters = {filter.key: filter.value for filter in query.filters if isinstance(filter, util.MetadataFilter)}
    #regex_filters = [filter.to_filter() for filter in query.filters if isinstance(filter, util.RegexFilter)]
    result = code_collection.query(
        query_texts=[query.natural_language_query],
        n_results=20,
        where=metadata_filters if metadata_filters else None,
    )
    assert result['documents'] != None and result['metadatas'] != None
    output = []
    for doc, metadata in zip(result['documents'][0], result['metadatas'][0]):
        output.append(CodeChunk(doc, metadata['repo'], metadata['path'], metadata['func_name'], metadata['language'], 0))
    return output

if __name__ == '__main__':
    raise Exception("search.py is not meant to be run directly.")
