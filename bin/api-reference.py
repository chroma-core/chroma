# Sanity check script to ensure that the Chroma client can connect
# and is capable of recieving data.
import chromadb
from chromadb.config import Settings

# run in in-memory mode
chroma_api = chromadb.Client()

# uncomment to run in client-server mode
# chroma_api = chroma.Client(Settings(chroma_api_impl="rest",
#                               chroma_server_host="localhost",
#                               chroma_server_http_port="8000") )


chroma_api.set_collection_name("sample_space")
print("Getting heartbeat to verify the server is up")
print(chroma_api.heartbeat())
chroma_api.reset()

print("Logging embeddings into the database")
# chroma_api.add(
#     embedding= [[1, 2, 3, 4, 5], [5, 4, 3, 2, 1], [10, 9, 8, 7, 6]],
#     input_uri= ["/images/1", "/images/2", "/images/3"],
#     dataset= "train",
#     inference_class= ["spoon", "knife", "fork"],
#     collection_name= "sample_space"
# )
chroma_api.add(
    embedding=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4]], 
    input_uri=["/images/3.png", "/images/5.png"], 
    inference_class=["bicycle", "car"],
	dataset=["training","training"],
	collection_name="sample_space"
)

# add_training, add_production, and add_triage simply set the dataset name for you
chroma_api.add_training(
    embedding=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4]], 
    input_uri=["/images/3.png", "/images/5.png"], 
    inference_class=["bicycle", "car"],
	label_class=["bicycle", "car"],
	collection_name="sample_space"
)
chroma_api.add_production(
    embedding=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4]], 
    input_uri=["/images/3.png", "/images/5.png"], 
    inference_class=["bicycle", "car"],
	collection_name="sample_space"
)
chroma_api.add_triage(
    embedding=[[1.1, 2.3, 3.2], [4.5, 6.9, 4.4]], 
    input_uri=["/images/3.png", "/images/5.png"], 
    inference_class=["bicycle", "car"],
	collection_name="sample_space"
)

print("count")
print(chroma_api.count())

# # print("fetch", chroma_api.fetch())
# print("Generating the index")
# print(chroma_api.create_index())

# print("Running a nearest neighbor search")
# print(chroma_api.get_nearest_neighbors([1, 2, 3, 4, 5], 1))

print(chroma_api.fetch(page=1, page_size=10, where={"collection_name": "sample_space"}))

print("Success! Everything worked!")

