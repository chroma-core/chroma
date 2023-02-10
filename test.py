import chromadb
from chromadb.config import Settings

chroma = chromadb.Client(Settings(chroma_api_impl="rest",
                              chroma_server_host="localhost",
                              chroma_server_http_port="8000"))
# chroma = chromadb.Client()

chroma.reset()

print("heartbeat", chroma.heartbeat())

print("create", chroma.create_collection("test", {"test": "test"}))

# print("list", chroma.list_collections())

# print("get", chroma.update_collection("test", {"test": "another"}))

# print("get", chroma.get_collection("test"))

# print("delete", chroma.delete_collection("test"))

# print("list", chroma.list_collections())

print("count", chroma.count("test"))

print("add", chroma.add(
    collection_name="test",
    embedding=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], [4.5, 6.9, 4.4], [1.1, 2.3, 3.2], [4.5, 6.9, 4.4]], 
))
print("add", chroma.add(
    collection_name="test",
    embedding=[[5.1, 4.3, 3.2], [6.5, 5.9, 4.4]], 
    metadata=[{"uri": "img11.png", "style": "style1"}, {"uri": "img10.png", "style": "style1"}]
))
print("add", chroma.add(
    collection_name="test",
    embedding=[[11.0, 12.0, 13.0]], 
    metadata=[{"uri": "img12.png", "style": "style1"}]
))
# print("add", chroma.add(
#     collection_name="test",
#     embedding=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4]], 
#     metadata=[{"apples": "bananas"}, {"apples": "oranges"}]
# ))




# print("create_index", chroma.create_index("test"))

# print("fetch", chroma.fetch("test", limit=2))

print("ann", chroma.search("test", [11.1, 12.1, 13.1], n_results=1))

# print("delete", chroma.delete("test"))

# print("count", chroma.count("test"))

# update the embedding for where metadata.uri == "img12.png"
# we search for the first embedding that matches the metadata
# and update it with the new embedding
print("add", chroma.update(
    collection_name="test",
    embedding=[[5.1, 4.3, 3.2], [6.5, 5.9, 4.4]], 
    metadata=[{"uri": "img12.png"}, {"uri": "img10.png"}]
))