# Sanity check script to ensure that the Chroma client can connect
# and is capable of recieving data.
import chroma
from chroma.config import Settings

# run in in-memory mode
chroma_api = chroma.get_api()

# uncomment to run in client-server mode
# chroma_api = chroma.get_api(Settings(chroma_api_impl="rest",
#                               chroma_server_host="localhost",
#                               chroma_server_http_port="8000") )


chroma_api.set_model_space("sample_space")
print("Getting heartbeat to verify the server is up")
print(chroma_api.heartbeat())

print("Logging embeddings into the database")
chroma_api.add(
    embedding= [[1, 2, 3, 4, 5], [5, 4, 3, 2, 1], [10, 9, 8, 7, 6]],
    input_uri= ["/images/1", "/images/2", "/images/3"],
    dataset= "train",
    inference_class= ["spoon", "knife", "fork"],
    model_space= "sample_space"
)

print("count")
print(chroma_api.count())

# print("fetch", chroma_api.fetch())
print("Generating the index")
print(chroma_api.create_index())

print("Running a nearest neighbor search")
print(chroma_api.get_nearest_neighbors([1, 2, 3, 4, 5], 1))

print("Success! Everything worked!")
