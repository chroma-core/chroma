from typing import Optional, cast, List
from chromadb.api.models.Collection import Collection
from chromadb.api.types import GetResult, OneOrMany, Include, Where, WhereDocument, ID

def paginated_get(
    collection: Collection,
    ids: Optional[OneOrMany[ID]] = None,
    where: Optional[Where] = None,
    where_document: Optional[WhereDocument] = None,
    include: Include = ["metadatas", "documents"],
) -> GetResult:
    total_documents = collection.count()
    offset = 0
    limit = 100 # TODO(c-gamble): This is arbitrary, pull from config/constant later

    result: GetResult = {
        "ids": [],
        "embeddings": None,
        "documents": None,
        "uris": None,
        "data": None,
        "metadatas": None,
        "included": include,
    }

    if "embeddings" in include:
        result["embeddings"] = []
    if "documents" in include:
        result["documents"] = []
    if "metadatas" in include:
        result["metadatas"] = []
    if "uris" in include:
        result["uris"] = []
    if "data" in include:
        result["data"] = []

    while len(result["ids"]) < total_documents:
        batch = collection.get(
            ids=ids,
            where=where,
            where_document=where_document,
            include=include,
            limit=limit,
            offset=offset,
        )
        result["ids"].extend(batch["ids"])
        for field in include:
            if field == "distances":
                continue  # Skip distances
            batch_field = batch.get(field)
            if batch_field is not None:
                if result[field] is None:
                    raise ValueError(f"Unexpected None result[{field}] during pagination.")
                cast(List, result[field]).extend(batch_field)
        offset += limit

    return result
