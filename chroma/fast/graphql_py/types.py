import strawberry
import base64
import models

from typing import Optional,  List, Generic, TypeVar
from strawberry.types import Info
from strawberry import UNSET

GenericType = TypeVar("GenericType")

@strawberry.type
class Embedding:
    id: strawberry.ID
    name: str
    data: str
    label: str
    identifier: str

    @strawberry.field
    async def projections(self, info: Info) -> list["Projection"]:
        projections = await info.context["projections_by_embedding"].load(self.id)
        return [Projection.marshal(projection) for projection in projections]

    @classmethod
    def marshal(cls, model: models.Embedding) -> "Embedding":
        return cls(
            id=strawberry.ID(str(model.id)), 
            name=model.name,
            data=model.data,
            label=model.label,
            identifier=model.identifier,
        )

@strawberry.type
class Projection:
    id: strawberry.ID
    name: str
    embedding: Optional[Embedding] = None

    @classmethod
    def marshal(cls, model: models.Projection) -> "Projection":
        return cls(
            id=strawberry.ID(str(model.id)),
            name=model.name,
            embedding=Embedding.marshal(model.embedding) if model.embedding else None,
        )

@strawberry.type
class EmbeddingExists:
    message: str = "Embedding with this name already exist"

@strawberry.type
class EmbeddingNotFound:
    message: str = "Couldn't find an embedding with the supplied name"

@strawberry.type
class EmbeddingNameMissing:
    message: str = "Please supply an embedding name"

AddProjectionResponse = strawberry.union("AddProjectionResponse", (Projection, EmbeddingNotFound, EmbeddingNameMissing))
AddEmbeddingResponse = strawberry.union("AddEmbeddingResponse", (Embedding, EmbeddingExists))

@strawberry.type
class Dataset:
    id: strawberry.ID
    name: str

    @classmethod
    def marshal(cls, model: models.Dataset) -> "Dataset":
        return cls(
            id=strawberry.ID(str(model.id)),
            name=model.name,
        )

@strawberry.type
class DatasetNotFound:
    message: str = "Couldn't find an dataset with the supplied name"


AddDatasetResponse = strawberry.union("AddDatasetResponse", (Dataset, DatasetNotFound))


# Pagination
# https://strawberry.rocks/docs/guides/pagination

@strawberry.type
class Connection(Generic[GenericType]):
    """Represents a paginated relationship between two entities

    This pattern is used when the relationship itself has attributes.
    In a Facebook-based domain example, a friendship between two people
    would be a connection that might have a `friendshipStartTime`
    """
    page_info: "PageInfo"
    edges: list["Edge[GenericType]"]

@strawberry.type
class PageInfo:
    """Pagination context to navigate objects with cursor-based pagination

    Instead of classic offset pagination via `page` and `limit` parameters,
    here we have a cursor of the last object and we fetch items starting from that one

    Read more at:
        - https://graphql.org/learn/pagination/#pagination-and-edges
        - https://relay.dev/graphql/connections.htm
    """
    has_next_page: bool
    has_previous_page: bool
    start_cursor: Optional[str]
    end_cursor: Optional[str]

@strawberry.type
class Edge(Generic[GenericType]):
    """An edge may contain additional information of the relationship. This is the trivial case"""
    node: GenericType
    cursor: str


def build_embedding_cursor(embedding: Embedding):
    """Adapt this method to build an *opaque* ID from an instance"""
    #embeddingid = f"{id(embedding)}".encode("utf-8")
    embeddingid = f"{(embedding.id)}".encode("utf-8")
    print("embeddingid " + str(embeddingid))
    return base64.b64encode(embeddingid).decode()


Cursor = str


def get_embeddings(first: int = 10, after: Optional[Cursor] = UNSET) -> Connection[Embedding]:
    """
    A non-trivial implementation should efficiently fetch only
    the necessary embeddings after the offset.
    For simplicity, here we build the list and then slice it accordingly
    """
    if after is not UNSET:
        after = int(base64.b64decode(after).decode())
    else:   
        after = None

    print("after " + str(after) + " first+1 ", str(first + 1))

    # async with models.get_session() as s:
    #     sql = select(models.Embedding).order_by(models.Embedding.name)
    #     db_embeddings = (await s.execute(sql)).scalars().unique().all()
    # return [Embedding.marshal(loc) for loc in db_embeddings]

    # Fetch the requested embeddings plus one, just to calculate `has_next_page`
    embeddings = [
        Embedding(
            name=f"Name {x}",
            id=f"{x}",
            data=f"Data {x}",
            label=f"Label {x}",
            identifier=f"Identifier {x}"
        )
        for x in range(20)
    ][after:first+1]

    edges = [
        Edge(node=Embedding.marshal(embedding), cursor=build_embedding_cursor(embedding))
        for embedding in embeddings
    ]

    return Connection(
        page_info=PageInfo(
            has_previous_page=False,
            has_next_page=len(embeddings) > first,
            start_cursor=edges[0].cursor if edges else None,
            end_cursor=edges[-2].cursor if len(edges) > 1 else None,
        ),
        edges=edges[:-1]  # exclude last one as it was fetched to know if there is a next page
    )