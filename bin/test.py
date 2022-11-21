# Sanity check script to ensure that the Chroma client can connect
# and is capable of recieving data.

from chroma_client import Chroma

chroma = Chroma()
chroma.set_model_space('sample_space')
print("Getting heartbeat to verify the server is up")
print(chroma.heartbeat())

print("Logging embeddings into the database")
chroma.add(
    [[1,2,3,4,5], [5,4,3,2,1], [10,9,8,7,6]], 
    ["/images/1", "/images/2", "/images/3"], 
    ["training", "training", "training"], 
    ['spoon', 'knife', 'fork']
)

print("count")
print(chroma.count())

# print("fetch", chroma.fetch())
print("Generating the index")
print(chroma.create_index())

print("Running a nearest neighbor search")
print(chroma.get_nearest_neighbors([1,2,3,4,5], 1))

print("Success! Everything worked!")
