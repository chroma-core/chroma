# Configuring Chroma Collections

You can configure the embedding space, HNSW index parameters, and embedding function of a collection by setting the collection configuration. These configurations will help you customize your Chroma collections for different data, accuracy and performance requirements.

The `space` parameter defines the distance function of the embedding space. The default is `l2` (squared L2 norm), and other possible values are `cosine` (cosine similarity), and `ip` (inner product).

| Distance          | parameter |                                                                                                                                                   Equation |
| ----------------- | :-------: |-----------------------------------------------------------------------------------------------------------------------------------------------------------:|
| Squared L2        |   `l2`    |                                                                                                {% Latex %} d =  \\sum\\left(A_i-B_i\\right)^2 {% /Latex %} |
| Inner product     |   `ip`    |                                                                                     {% Latex %} d = 1.0 - \\sum\\left(A_i \\times B_i\\right) {% /Latex %} |
| Cosine similarity | `cosine`  | {% Latex %} d = 1.0 - \\frac{\\sum\\left(A_i \\times B_i\\right)}{\\sqrt{\\sum\\left(A_i^2\\right)} \\cdot \\sqrt{\\sum\\left(B_i^2\\right)}} {% /Latex %} |

## HNSW Index Configuration

The HNSW index parameters include:

* `ef_construction` determines the size of the candidate list used to select neighbors during index creation. A higher value improves index quality at the cost of more memory and time, while a lower value speeds up construction with reduced accuracy. The default value is `100`.
* `ef_search` determines the size of the dynamic candidate list used while searching for the nearest neighbors. A higher value improves recall and accuracy by exploring more potential neighbors but increases query time and computational cost, while a lower value results in faster but less accurate searches. The default value is `100`.
* `max_neighbors` is the maximum number of neighbors (connections) that each node in the graph can have during the construction of the index. A higher value results in a denser graph, leading to better recall and accuracy during searches but increases memory usage and construction time. A lower value creates a sparser graph, reducing memory usage and construction time but at the cost of lower search accuracy and recall. The default value is `16`.
* `num_threads` specifies the number of threads to use during index construction or search operations. The default value is `multiprocessing.cpu_count()` (available CPU cores).
* `batch_size` controls the number of vectors to process in each batch during index operations. The default value is `100`.
* `sync_threshold` determines when to synchronize the index with persistent storage. The default value is `1000`.
* `resize_factor` controls how much the index grows when it needs to be resized. The default value is `1.2`.

## Embedding Function Configuration

By default, Chroma uses the `DefaultEmbeddingFunction` which is based on the Sentence Transformers `all-MiniLM-L6-v2` model. You can configure a different embedding function for your collection using the collection configuration. For example, you can use Cohere's embedding models.

**Note:** Using embedding functions like Cohere often requires an API key. By default, Chroma looks for the key in the `CHROMA_COHERE_API_KEY` environment variable. It is recommended to set your API key using this environment variable. Chroma will securely store the *name* of the environment variable in its configuration, not the actual API key itself, ensuring your key remains private.

If your API key is stored in a different environment variable, you can specify its name using the `api_key_env_var` parameter when creating the embedding function.

Here's how to configure the Cohere embedding function:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Make sure you have set the CHROMA_COHERE_API_KEY environment variable
import os
assert os.environ.get("CHROMA_COHERE_API_KEY"), "CHROMA_COHERE_API_KEY environment variable not set"

from chromadb.utils.embedding_functions.cohere_embedding_function import CohereEmbeddingFunction

# Create the Cohere embedding function (API key is read from environment variable)
# By default, it reads from 'CHROMA_COHERE_API_KEY'
cohere_ef = CohereEmbeddingFunction(model_name="embed-english-light-v2.0")

# Example: If your key is in 'MY_COHERE_KEY'
# cohere_ef = CohereEmbeddingFunction(
#     model_name="embed-english-light-v2.0",
#     api_key_env_var="MY_COHERE_KEY"
# )

# Create collection with the embedding function in configuration
collection = client.create_collection(
    name="my_collection_cohere",
    configuration={
        "embedding_function": cohere_ef
    }
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Make sure you have set the COHERE_API_KEY environment variable
if (!process.env.COHERE_API_KEY) {
  throw new Error("COHERE_API_KEY environment variable not set");
}

import { CohereEmbeddingFunction } from "chromadb";

// Create the Cohere embedding function (API key is read from environment variable)
// By default, it reads from 'CHROMA_COHERE_API_KEY'
const cohereEf = new CohereEmbeddingFunction({
    modelName: "embed-english-light-v2.0"
});

// Example: If your key is in 'MY_COHERE_KEY'
// const cohereEf = new CohereEmbeddingFunction({
//     modelName: "embed-english-light-v2.0",
//     apiKeyEnvVar: "MY_COHERE_KEY" // Note: parameter name might differ slightly in TS
// });

// Create collection with the embedding function in configuration
let collection = await client.createCollection({
    name: "my_collection_cohere",
    configuration: {
        embedding_function: cohereEf
    }
});
```
{% /Tab %}

{% /TabbedCodeBlock %}

You can learn more about available embedding functions in our [Embeddings section](../embeddings/embedding-functions).

## Complete Configuration Example

Here is an example showing how to configure both the HNSW index and embedding function together using Cohere:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Make sure you have set the COHERE_API_KEY environment variable
import os
assert os.environ.get("COHERE_API_KEY"), "COHERE_API_KEY environment variable not set"

from chromadb.utils.embedding_functions.cohere_embedding_function import CohereEmbeddingFunction

# Create the Cohere embedding function
cohere_ef = CohereEmbeddingFunction(model_name="embed-english-light-v2.0")

# Create collection with both HNSW and embedding function configuration
collection = client.create_collection(
    name="my_collection_complete",
    configuration={
        "hnsw": {
            "space": "cosine", # Cohere models often use cosine space
            "ef_search": 100,
            "ef_construction": 100,
            "max_neighbors": 16,
            "num_threads": 4
        },
        "embedding_function": cohere_ef
    }
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Make sure you have set the COHERE_API_KEY environment variable
if (!process.env.COHERE_API_KEY) {
  throw new Error("COHERE_API_KEY environment variable not set");
}

import { CohereEmbeddingFunction } from "chromadb";

// Create the Cohere embedding function
const cohereEf = new CohereEmbeddingFunction({
    modelName: "embed-english-light-v2.0"
});

// Create collection with both HNSW and embedding function configuration
let collection = await client.createCollection({
    name: "my_collection_complete",
    configuration: {
        hnsw: {
            space: "cosine", // Cohere models often use cosine space
            ef_search: 100,
            ef_construction: 100,
            max_neighbors: 16,
            num_threads: 4
        },
        embedding_function: cohereEf
    }
});
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Fine-Tuning HNSW Parameters

We use an HNSW (Hierarchical Navigable Small World) index to perform approximate nearest neighbor (ANN) search for a given embedding. In this context, **Recall** refers to how many of the true nearest neighbors were retrieved.

Increasing `ef_search` normally improves recall, but slows down query time. Similarly, increasing `ef_construction` improves recall, but increases the memory usage and runtime when creating the index.

Choosing the right values for your HNSW parameters depends on your data, embedding function, and requirements for recall, and performance. You may need to experiment with different construction and search values to find the values that meet your requirements.

For example, for a dataset with 50,000 embeddings of 2048 dimensions, generated by
```python
embeddings = np.random.randn(50000, 2048).astype(np.float32).tolist()
```

we set up two Chroma collections:
* The first is configured with `ef_search: 10`. When querying using a specific embedding from the set (with `id = 1`), the query takes `0.00529` seconds, and we get back embeddings with distances:

```
[3629.019775390625, 3666.576904296875, 3684.57080078125]
```

* The second collection is configured with `ef_search: 100` and `ef_construction: 1000`. When issuing the same query, this time it takes `0.00753` seconds (about 42% slower), but with better results as measured by their distance:

```
[0.0, 3620.593994140625, 3623.275390625]
```
In this example, when querying with the test embedding (`id=1`), the first collection failed to find the embedding itself, despite it being in the collection (where it should have appeared as a result with a distance of `0.0`). The second collection, while slightly slower, successfully found the query embedding itself (shown by the `0.0` distance) and returned closer neighbors overall, demonstrating better accuracy at the cost of performance.
