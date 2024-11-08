import chromadb
import random
import time
import tqdm

cube_width = 100

client = chromadb.HttpClient()
name = "cubic"
collection = client.get_or_create_collection(name)

collection_count = collection.count()
print(collection_count)
# if collection_count > 0:
#     client.delete_collection(name)
#     collection = client.create_collection(name)

# print("Shuffling 1M xyz coordinates")
# embeddings = [[i // cube_width ** 2, i // cube_width % cube_width, i % cube_width] for i in range(cube_width ** 3)]
# random.shuffle(embeddings)

# for start in tqdm.tqdm(range(0, len(embeddings), cube_width)):
#     ids = range(start, start + cube_width)
#     id_str = [f"id_{id}" for id in ids]
#     embeds = embeddings[start:start+cube_width]
#     metas = [{"i": i, "x": e[0], "y": e[1], "z": e[2]} for i, e in zip(ids, embeds)]
#     collection.add(ids=id_str, embeddings=embeds, metadatas=metas)

# def bench(query):
#     def timed():
#         start = time.time()
#         query()
#         end = time.time()
#         print(f"Runtime: {end - start}s")
#     return timed

# def random_query_embedding():
#     return [random.randint(20, 80), random.randint(20, 80), random.randint(20, 80)]

# print(collection._model["version"])

# collection.query(query_embeddings=[random_query_embedding()], n_results=6)

# query_embeddings = list()
# for _ in range(1):
#     query_embeddings.append(random_query_embedding())
    
# @bench
# def simple_query():
#     collection.query(query_embeddings=query_embeddings, n_results=6)

# simple_query()
