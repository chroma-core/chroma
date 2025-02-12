# Testing recall

Selecting HNSW parameters for a specific use case highly impacts the search quality. One way to test the quality of the constructed index is to compare the HNSW search results to the actual results (i.e., the actual `k` nearest neighbors).
For that cause, the API enables creating a simple "brute-force" index in which vectors are stored as is, and searching for the `k` nearest neighbors to a query vector requires going over the entire index.
Comparing between HNSW and brute-force results may help with finding the desired HNSW parameters for achieving a satisfying recall, based on the index size and data dimension.

### Brute force index API
`hnswlib.BFIndex(space, dim)` creates a non-initialized index in space `space` with integer dimension `dim`.

`hnswlib.BFIndex` methods:

`init_index(max_elements)` initializes the index with no elements.

max_elements defines the maximum number of elements that can be stored in the structure.

`add_items(data, ids)` inserts the data (numpy array of vectors, shape:`N*dim`) into the structure.
`ids` are optional N-size numpy array of integer labels for all elements in data.

`delete_vector(label)` delete the element associated with the given `label` so it will be omitted from search results.

`knn_query(data, k = 1)` make a batch query for `k `closest elements for each element of the
`data` (shape:`N*dim`). Returns a numpy array of (shape:`N*k`).

`load_index(path_to_index, max_elements = 0)` loads the index from persistence to the uninitialized index.

`save_index(path_to_index)` saves the index from persistence.

### measuring recall example

```python
import hnswlib
import numpy as np

dim = 32
num_elements = 100000
k = 10
nun_queries = 10

# Generating sample data
data = np.float32(np.random.random((num_elements, dim)))

# Declaring index
hnsw_index = hnswlib.Index(space='l2', dim=dim)  # possible options are l2, cosine or ip
bf_index = hnswlib.BFIndex(space='l2', dim=dim)

# Initing both hnsw and brute force indices
# max_elements - the maximum number of elements (capacity). Will throw an exception if exceeded
# during insertion of an element.
# The capacity can be increased by saving/loading the index, see below.
#
# hnsw construction params:
# ef_construction - controls index search speed/build speed tradeoff
#
# M - is tightly connected with internal dimensionality of the data. Strongly affects the memory consumption (~M)
# Higher M leads to higher accuracy/run_time at fixed ef/efConstruction

hnsw_index.init_index(max_elements=num_elements, ef_construction=200, M=16)
bf_index.init_index(max_elements=num_elements)

# Controlling the recall for hnsw by setting ef:
# higher ef leads to better accuracy, but slower search
hnsw_index.set_ef(200)

# Set number of threads used during batch search/construction in hnsw
# By default using all available cores
hnsw_index.set_num_threads(1)

print("Adding batch of %d elements" % (len(data)))
hnsw_index.add_items(data)
bf_index.add_items(data)

print("Indices built")

# Generating query data
query_data = np.float32(np.random.random((nun_queries, dim)))

# Query the elements and measure recall:
labels_hnsw, distances_hnsw = hnsw_index.knn_query(query_data, k)
labels_bf, distances_bf = bf_index.knn_query(query_data, k)

# Measure recall
correct = 0
for i in range(nun_queries):
    for label in labels_hnsw[i]:
        for correct_label in labels_bf[i]:
            if label == correct_label:
                correct += 1
                break

print("recall is :", float(correct)/(k*nun_queries))
```
