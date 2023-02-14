import chromadb
from chromadb.config import Settings
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction

USE_LOCAL = False

client = None
if USE_LOCAL:
    client = chromadb.Client()
else:
    client = chromadb.Client(
        Settings(
            chroma_api_impl="rest", chroma_server_host="localhost", chroma_server_http_port="8000"
        )
    )
# print(client)

print(client.heartbeat())
client.reset()

collection = client.create_collection(name="test")
# Check type of Collection
assert type(collection) == chromadb.api.models.Collection.Collection
print(collection)
print(collection.name)
assert collection.count() == 0

getcollection = client.get_collection(name="test")
# Check type of get Collection
assert type(getcollection) == chromadb.api.models.Collection.Collection
print(getcollection)


# Test list, delete collections #
collections_list = client.list_collections()
assert len(collections_list) == 1
assert type(collections_list[0]) == chromadb.api.models.Collection.Collection

collection2 = client.create_collection(name="test2")
assert len(client.list_collections()) == 2
client.delete_collection(name="test2")
assert len(client.list_collections()) == 1
client.create_collection(name="test2")
client.delete_collection(name="test2")
assert len(client.list_collections()) == 1
print(client.list_collections())
# Check type of list_collections

# collection.create_index # wipes out the index you have (if you have one) and creates a fresh one
# collection = client.update_collection(oldName="test", newName="test2") # this feels a little odd to me (Jeff) -> collection.update(name="test2")

# add many
collection.add(
    embeddings=[
        [1.1, 2.3, 3.2],
        [4.5, 6.9, 4.4],
        [1.1, 2.3, 3.2],
        [4.5, 6.9, 4.4],
        [1.1, 2.3, 3.2],
        [4.5, 6.9, 4.4],
        [1.1, 2.3, 3.2],
        [4.5, 6.9, 4.4],
    ],
    metadatas=[
        {"uri": "img1.png", "style": "style1"},
        {"uri": "img2.png", "style": "style2"},
        {"uri": "img3.png", "style": "style1"},
        {"uri": "img4.png", "style": "style1"},
        {"uri": "img5.png", "style": "style1"},
        {"uri": "img6.png", "style": "style1"},
        {"uri": "img7.png", "style": "style1"},
        {"uri": "img8.png", "style": "style1"},
    ],
    documents=["doc1", "doc2", "doc3", "doc4", "doc5", "doc6", "doc7", "doc8"],
    ids=["id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8"],
)

# add one
collection.add(
    embeddings=[1.5, 2.9, 3.4],
    metadatas={"uri": "img9.png", "style": "style1"},
    documents="doc1000101",
    ids="uri9",
)

print(collection.peek(5))
print(collection.count())  # NIT: count count take a where filter too
assert collection.count() == 9


### Test get by ids ###
get_ids_result = collection.get(
    ids=["id1", "id2"],
)
print("\nGET ids\n", get_ids_result)
assert len(get_ids_result["embeddings"]) == 2

### Test get where clause ###
get_where_result = collection.get(
    where={"style": "style1", "uri": "img1.png"},
)
print("\nGet where\n", get_where_result)
assert len(get_where_result["ids"]) == 1

### Test get both ###
get_both_result = collection.get(
    ids=["id1", "id3"],
    where={"style": "style1"},
)
print("\nGet both\n", get_both_result)
assert len(get_both_result["documents"]) == 2

# NIT: verify supports multiple at once is actually working
print(
    "\nquery",
    collection.query(
        query_embeddings=[[1.1, 2.3, 3.2], [5.1, 4.3, 2.2]],
        # OR // COULD BE an AND and return a tuple
        # query_texts="doc10",
        n_results=2,
        # where={"style": "style2"},
    ),
)

### Test delete Partial ##
collection.delete(  # propagates to the index
    ids=["id1"],
)
assert collection.count() == 8

### Test delete Partial ##
collection.delete(  # propagates to the index
    where={"style": "style2"},
)
assert collection.count() == 7

### Test delete All ##
collection.delete()
assert collection.count() == 0

client.delete_collection(name="test")
assert len(client.list_collections()) == 0

# Test embedding function
collection = client.create_collection(
    name="test", embedding_function=SentenceTransformerEmbeddingFunction()
)

# Add docs without embeddings (call emb function)
collection.add(
    metadatas=[
        {"uri": "img1.png", "style": "style1"},
        {"uri": "img2.png", "style": "style2"},
        {"uri": "img3.png", "style": "style1"},
        {"uri": "img4.png", "style": "style1"},
        {"uri": "img5.png", "style": "style1"},
        {"uri": "img6.png", "style": "style1"},
        {"uri": "img7.png", "style": "style1"},
        {"uri": "img8.png", "style": "style1"},
    ],
    documents=["doc1", "doc2", "doc3", "doc4", "doc5", "doc6", "doc7", "doc8"],
    ids=["id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8"],
)

# Add single doc without embeddings (call emb function)
collection.add(metadatas={"uri": "img9.png", "style": "style1"}, documents="doc9", ids="id9")

print(collection.peek(5))
assert collection.count() == 9

# Query with only text docs
# print(
#     "query",
#     collection.query(
#         query_texts=["doc1", "doc2"],
#         n_results=2,
#     ),
# )


### TEST UPDATE ###
collection = client.create_collection(
    "test_update", embedding_function=(lambda documents: [[0.1, 1.1, 1.2]] * len(documents))
)
assert collection.count() == 0

collection.add(
    embeddings=[
        [1.1, 2.3, 3.2],
        [4.5, 6.9, 4.4],
        [1.1, 2.3, 3.2],
        [4.5, 6.9, 4.4],
        [1.1, 2.3, 3.2],
        [4.5, 6.9, 4.4],
        [1.1, 2.3, 3.2],
        [4.5, 6.9, 4.4],
    ],
    metadatas=[
        {"uri": "img1.png", "style": "style1"},
        {"uri": "img2.png", "style": "style2"},
        {"uri": "img3.png", "style": "style1"},
        {"uri": "img4.png", "style": "style1"},
        {"uri": "img5.png", "style": "style1"},
        {"uri": "img6.png", "style": "style1"},
        {"uri": "img7.png", "style": "style1"},
        {"uri": "img8.png", "style": "style1"},
    ],
    documents=["doc1", "doc2", "doc3", "doc4", "doc5", "doc6", "doc7", "doc8"],
    ids=["id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8"],
)

# Test update all fields again
collection.update(
    ids=["id1", "id2"],
    embeddings=[[0.0, 0.0, 0.5], [2.0, 0.0, 2.0]],
    metadatas=[
        {"uri": "img1.1.png", "style": "style1"},
        {"uri": "img2.1.png", "style": "style1"},
    ],
    documents=["cod1", "cod2"],
)

results = collection.get(ids=["id1", "id2"])
assert results["documents"][0] == "cod1"
assert results["metadatas"][0]["uri"] == "img1.1.png"
assert results["documents"][1] == "cod2"
assert results["metadatas"][1]["uri"] == "img2.1.png"


# Test update just document, embedding should get computed via function
collection.update(
    ids=["id1"],
    documents=["cod1"],
)

item1 = collection.get(ids="id1")
assert item1["metadatas"][0]["uri"] == "img1.1.png"
assert item1["embeddings"][0][0] == 0.1

# Test update just metadata
collection.update(
    ids="id1",
    metadatas={"uri": "img1.2.png", "style": "style1"},
)

item1 = collection.get(ids="id1")
assert item1["metadatas"][0]["uri"] == "img1.2.png"
assert item1["embeddings"][0][0] == 0.1
assert item1["documents"][0] == "cod1"
collection.delete()

### Test default embedding function ###
# Create collection with no embedding function
client.delete_collection(name="test")
collection = client.create_collection(name="test")

# Add docs without embeddings (call emb function)
collection.add(
    metadatas=[
        {"uri": "img1.png", "style": "style1"},
        {"uri": "img2.png", "style": "style2"},
        {"uri": "img3.png", "style": "style1"},
        {"uri": "img4.png", "style": "style1"},
        {"uri": "img5.png", "style": "style1"},
        {"uri": "img6.png", "style": "style1"},
        {"uri": "img7.png", "style": "style1"},
        {"uri": "img8.png", "style": "style1"},
    ],
    documents=["doc1", "doc2", "doc3", "doc4", "doc5", "doc6", "doc7", "doc8"],
    ids=["id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8"],
)

# Query with only text docs
print(
    "query",
    collection.query(
        query_texts=["doc1", "doc2"],
        n_results=2,
    ),
)


# collection.upsert( # always succeeds
#     embeddings=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], [4.5, 6.9, 4.4]],
#     metadatas=[{"uri": "img1.png", "style": "style1"}, {"uri": "img2.png", "style": "style1"}, {"uri": "img3.png", "style": "style1"}, {"uri": "img4.png", "style": "style1"}, {"uri": "img5.png", "style": "style1"}, {"uri": "img6.png", "style": "style1"}, {"uri": "img7.png", "style": "style1"}, {"uri": "img8.png", "style": "style1"}],
#     documents=["doc1", "doc2", "doc3", "doc4", "doc5", "doc6", "doc7", "doc8"],
#     ids=["id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8"],
# )
