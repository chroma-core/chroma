# Configuring Chroma Collections

You can configure the embedding space of a collection by setting special keys on a collection's metadata. These configurations will help you customize your Chroma collections for different data, accuracy and performance requirements.

* `hnsw:space` defines the distance function of the embedding space. The default is `l2` (squared L2 norm), and other possible values are `cosine` (cosine similarity), and `ip` (inner product).

| Distance          | parameter |                                                                                                                                                   Equation |
| ----------------- | :-------: |-----------------------------------------------------------------------------------------------------------------------------------------------------------:|
| Squared L2        |   `l2`    |                                                                                                {% Latex %} d =  \\sum\\left(A_i-B_i\\right)^2 {% /Latex %} |
| Inner product     |   `ip`    |                                                                                     {% Latex %} d = 1.0 - \\sum\\left(A_i \\times B_i\\right) {% /Latex %} |
| Cosine similarity | `cosine`  | {% Latex %} d = 1.0 - \\frac{\\sum\\left(A_i \\times B_i\\right)}{\\sqrt{\\sum\\left(A_i^2\\right)} \\cdot \\sqrt{\\sum\\left(B_i^2\\right)}} {% /Latex %} |

* `hnsw:construction_ef` determines the size of the candidate list used to select neighbors during index creation. A higher value improves index quality at the cost of more memory and time, while a lower value speeds up construction with reduced accuracy. The default value is `100`.
* `hnsw:search_ef` determines the size of the dynamic candidate list used while searching for the nearest neighbors. A higher value improves recall and accuracy by exploring more potential neighbors but increases query time and computational cost, while a lower value results in faster but less accurate searches. The default value is `10`.
* `hnsw:M` is the maximum number of neighbors (connections) that each node in the graph can have during the construction of the index. A higher value results in a denser graph, leading to better recall and accuracy during searches but increases memory usage and construction time. A lower value creates a sparser graph, reducing memory usage and construction time but at the cost of lower search accuracy and recall. The default value is `16`.
* `hnsw:num_threads` specifies the number of threads to use during index construction or search operations. The default value is `multiprocessing.cpu_count()` (available CPU cores).

Here is an example of how you can create a collection and configure it with custom HNSW settings:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
collection = client.create_collection(
    name="my_collection", 
    embedding_function=emb_fn,
    metadata={
        "hnsw:space": "cosine",
        "hnsw:search_ef": 100
    }
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
let collection = await client.createCollection({
    name: "my_collection",
    embeddingFunction: emb_fn,
    metadata: {
        "hnsw:space": "cosine",
        "hnsw:search_ef": 100
    }
});
```
{% /Tab %}

{% /TabbedCodeBlock %}

You can learn more in our [Embeddings section](../embeddings/embedding-functions).