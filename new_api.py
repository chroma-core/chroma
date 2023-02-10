import chromadb
from chromadb.config import Settings

# client = chromadb.Client()
client = chromadb.Client(
    Settings(chroma_api_impl="rest", chroma_server_host="localhost", chroma_server_http_port="8000")
)
# print(client)

print(client.heartbeat())
client.reset()

collection = client.create_collection(name="test")
print(collection)

getcollection = client.get_collection(name="test")
print(getcollection)

print(collection.count())

assert collection.count() == 0
assert len(client.list_collections()) == 1

collection2 = client.create_collection(name="test2")
assert len(client.list_collections()) == 2
client.delete_collection(name="test2")
assert len(client.list_collections()) == 1
print(client.list_collections())

# collection.create_index # wipes out the index you have (if you have one) and creates a fresh one
# collection = client.update_collection(oldName="test", newName="test2") # this feels a little odd to me (Jeff) -> collection.update(name="test2")
# client.delete_collection(name="test")

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
assert get_ids_result.shape[0] == 2

### Test get where clause ###
get_where_result = collection.get(
    where={"style": "style1", "uri": "img1.png"},
)
print("\nGet where\n", get_where_result)
assert get_where_result.shape[0] == 1

### Test get both ###
get_both_result = collection.get(
    ids=["id1", "id3"],
    where={"style": "style1"},
)
print("\nGet both\n", get_both_result)
assert get_both_result.shape[0] == 2

# NIT: verify supports multiple at once is actually working
print(
    "query",
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

### TODO: Test delete Filter ##

# collection.upsert( # always succeeds
#     embeddings=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], [4.5, 6.9, 4.4]],
#     metadatas=[{"uri": "img1.png", "style": "style1"}, {"uri": "img2.png", "style": "style1"}, {"uri": "img3.png", "style": "style1"}, {"uri": "img4.png", "style": "style1"}, {"uri": "img5.png", "style": "style1"}, {"uri": "img6.png", "style": "style1"}, {"uri": "img7.png", "style": "style1"}, {"uri": "img8.png", "style": "style1"}],
#     documents=["doc1", "doc2", "doc3", "doc4", "doc5", "doc6", "doc7", "doc8"],
#     ids=["id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8"],
# )

# collection.delete( # propagates to the index
#     ids=["id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8"],
# 	# where/filter?
# )

# collection.update( # fails if id doesnt exist
#     ids=["id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8"],
#     # THE BELOW IS OPTIONAL
#     embeddings=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], [4.5, 6.9, 4.4]],
#     metadatas=[{"uri": "img1.png", "style": "style1"}, {"uri": "img2.png", "style": "style1"}, {"uri": "img3.png", "style": "style1"}, {"uri": "img4.png", "style": "style1"}, {"uri": "img5.png", "style": "style1"}, {"uri": "img6.png", "style": "style1"}, {"uri": "img7.png", "style": "style1"}, {"uri": "img8.png", "style": "style1"}],
#     documents=["doc1", "doc2", "doc3", "doc4", "doc5", "doc6", "doc7", "doc8"],
#     # flags -- probably cut this for now to avoid dirty state between the index and items
#     # update_embeddings=True,
#     # update_index=False
# )
