import numpy as np
import os


def normalized(a, axis=-1, order=2):
    l2 = np.atleast_1d(np.linalg.norm(a, order, axis))
    l2[l2 == 0] = 1
    return a / np.expand_dims(l2, axis)


N = 100000
dummy_data_multiplier = 3
N_queries = 1000
d = 8
K = 5

np.random.seed(1)

print("Generating data...")
batches_dummy = [
    normalized(np.float32(np.random.random((N, d))))
    for _ in range(dummy_data_multiplier)
]
batch_final = normalized(np.float32(np.random.random((N, d))))
queries = normalized(np.float32(np.random.random((N_queries, d))))
print("Computing distances...")
dist = np.dot(queries, batch_final.T)
topk = np.argsort(-dist)[:, :K]
print("Saving...")

try:
    os.mkdir("data")
except OSError as e:
    pass

for idx, batch_dummy in enumerate(batches_dummy):
    batch_dummy.tofile("data/batch_dummy_%02d.bin" % idx)
batch_final.tofile("data/batch_final.bin")
queries.tofile("data/queries.bin")
np.int32(topk).tofile("data/gt.bin")
with open("data/config.txt", "w") as file:
    file.write("%d %d %d %d %d" % (N, dummy_data_multiplier, N_queries, d, K))
