# Configuring Chroma Collections

Chroma collections have a `configuration` that determines how their embeddings index is constructed and used. We use default values for these index configurations that should give you great performance for most use cases out-of-the-box. 

The [embedding function](../embeddings/embedding-functions) you choose to use in your collection also affects its index construction, and is included in the configuration.

When you create a collection, you can customize these index configuration values for different data, accuracy and performance requirements. Some query-time configurations can also be customized after the collection's creation using the `.modify` function. 

{% CustomTabs %}

{% Tab label="Single Node" %}

## HNSW Index Configuration

In Single Node Chroma collections, we use an HNSW (Hierarchical Navigable Small World) index to perform approximate nearest neighbor (ANN) search.

{% Accordion %}

{% AccordionItem label="What is an HNSW index?" %}

An HNSW (Hierarchical Navigable Small World) index is a graph-based data structure designed for efficient approximate nearest neighbor search in high-dimensional vector spaces. It works by constructing a multi-layered graph where each layer contains a subset of the data points, with higher layers being sparser and serving as "highways" for faster navigation. The algorithm builds connections between nearby points at each layer, creating "small-world" properties that allow for efficient search complexity. During search, the algorithm starts at the top layer and navigates toward the query point in the embedding space, then moves down through successive layers, refining the search at each level until it finds the final nearest neighbors.

{% /AccordionItem %}

{% /Accordion %}

The HNSW index parameters include:

* `space` defines the distance function of the embedding space, and hence how similarity is defined. The default is `l2` (squared L2 norm), and other possible values are `cosine` (cosine similarity), and `ip` (inner product).

| Distance          | parameter |                                                                                                                                                   Equation |                                                                          Intuition                                                                          |
| ----------------- | :-------: |-----------------------------------------------------------------------------------------------------------------------------------------------------------:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------:|
| Squared L2        |   `l2`    |                                                                                                {% Latex %} d =  \\sum\\left(A_i-B_i\\right)^2 {% /Latex %} |                       measures absolute geometric distance between vectors, making it suitable when you want true spatial proximity.                        |
| Inner product     |   `ip`    |                                                                                     {% Latex %} d = 1.0 - \\sum\\left(A_i \\times B_i\\right) {% /Latex %} |             focuses on vector alignment and magnitude, often used for recommendation systems where larger values indicate stronger preferences              |
| Cosine similarity | `cosine`  | {% Latex %} d = 1.0 - \\frac{\\sum\\left(A_i \\times B_i\\right)}{\\sqrt{\\sum\\left(A_i^2\\right)} \\cdot \\sqrt{\\sum\\left(B_i^2\\right)}} {% /Latex %} | measures only the angle between vectors (ignoring magnitude), making it ideal for text embeddings or cases where you care about direction rather than scale |

{% Banner type="note" %}
You should make sure that the `space` you choose is supported by your collection's embedding function. Every Chroma embedding function specifies its default space and a list of supported spaces.
{% /Banner %}

* `ef_construction` determines the size of the candidate list used to select neighbors during index creation. A higher value improves index quality at the cost of more memory and time, while a lower value speeds up construction with reduced accuracy. The default value is `100`.
* `ef_search` determines the size of the dynamic candidate list used while searching for the nearest neighbors. A higher value improves recall and accuracy by exploring more potential neighbors but increases query time and computational cost, while a lower value results in faster but less accurate searches. The default value is `100`. This field can be modified after creation.
* `max_neighbors` is the maximum number of neighbors (connections) that each node in the graph can have during the construction of the index. A higher value results in a denser graph, leading to better recall and accuracy during searches but increases memory usage and construction time. A lower value creates a sparser graph, reducing memory usage and construction time but at the cost of lower search accuracy and recall. The default value is `16`.
* `num_threads` specifies the number of threads to use during index construction or search operations. The default value is `multiprocessing.cpu_count()` (available CPU cores). This field can be modified after creation.
* `batch_size` controls the number of vectors to process in each batch during index operations. The default value is `100`. This field can be modified after creation.
* `sync_threshold` determines when to synchronize the index with persistent storage. The default value is `1000`. This field can be modified after creation.
* `resize_factor` controls how much the index grows when it needs to be resized. The default value is `1.2`. This field can be modified after creation.

For example, here we create a collection with customized values for `space` and `ef_construction`:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
collection = client.create_collection(
    name="my-collection",
    embedding_function=OpenAIEmbeddingFunction(model_name="text-embedding-3-small"),
    configuration={
        "hnsw": {
            "space": "cosine",
            "ef_construction": 200
        }
    }
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
collection = await client.createCollection({
    name: "my-collection",
    embeddingFunction: new OpenAIEmbeddingFunction({ modelName: "text-embedding-3-small" }),
    configuration: {
        hnsw: {
            space: "cosine",
            ef_construction: 200
        }
    }
})
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Fine-Tuning HNSW Parameters

In the context of approximate nearest neighbors search, **recall** refers to how many of the true nearest neighbors were retrieved.

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


{% /Tab %}

{% Tab label="Distributed and Chroma Cloud" %}

## SPANN Index Configuration

In Distributed Chroma and Chroma Cloud collections, we use a SPANN (Spacial Approximate Nearest Neighbors) index to perform approximate nearest neighbor (ANN) search.

{% Video link="https://www.youtube.com/embed/1QdwYWd3S1g" title="SPANN Video" / %}

{% Accordion %}

{% AccordionItem label="What is a SPANN index?" %}

A SPANN index is a data structure used to efficiently find approximate nearest neighbors in large sets of high-dimensional vectors. It works by dividing the set into broad clusters (so we can ignore most of the data during search) and then building efficient, smaller indexes within each cluster for fast local lookups. This two-level approach helps reduce both memory use and search time, making it practical to search billions of vectors stored even on hard drives or separate machines in a distributed system.

{% /AccordionItem %}

{% /Accordion %}

{% Banner type="note" %}
We currently don't allow customization or modification of SPANN configuration. If you set these values they will be ignored by the server.
{% /Banner %}

The SPANN index parameters include:

* `space` defines the distance function of the embedding space, and hence how similarity is defined. The default is `l2` (squared L2 norm), and other possible values are `cosine` (cosine similarity), and `ip` (inner product).

| Distance          | parameter |                                                                                                                                                   Equation |                                                                          Intuition                                                                          |
| ----------------- | :-------: |-----------------------------------------------------------------------------------------------------------------------------------------------------------:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------:|
| Squared L2        |   `l2`    |                                                                                                {% Latex %} d =  \\sum\\left(A_i-B_i\\right)^2 {% /Latex %} |                       measures absolute geometric distance between vectors, making it suitable when you want true spatial proximity.                        |
| Inner product     |   `ip`    |                                                                                     {% Latex %} d = 1.0 - \\sum\\left(A_i \\times B_i\\right) {% /Latex %} |             focuses on vector alignment and magnitude, often used for recommendation systems where larger values indicate stronger preferences              |
| Cosine similarity | `cosine`  | {% Latex %} d = 1.0 - \\frac{\\sum\\left(A_i \\times B_i\\right)}{\\sqrt{\\sum\\left(A_i^2\\right)} \\cdot \\sqrt{\\sum\\left(B_i^2\\right)}} {% /Latex %} | measures only the angle between vectors (ignoring magnitude), making it ideal for text embeddings or cases where you care about direction rather than scale |

* `search_nprobe`: The default value is 64. 
* `write_nprobe`: The default value is 64. 
* `ef_construction`: The default value is 200. 
* `ef_search`: The default value is 200. 
* `max_neighbors`: The default value is 64. 
* `reassign_neighbor_count`: The default value is 64. 
* `split_threshold`: The default value is 200. 
* `merge_threshold`: The default value is 100.

{% /Tab %}

{% /CustomTabs %}

## Embedding Function Configuration

The embedding function you choose when creating a collection, along with the parameters you instantiate it with, is persisted in the collection's configuration. This allows us to reconstruct it correctly when you use collection across different clients. 

You can set your embedding function as an argument to the "create" methods, or directly in the configuration:

{% Tabs %}

{% Tab label="python" %}

Install the `openai` and `cohere` packages:

{% TabbedUseCaseCodeBlock language="Terminal" %}

{% Tab label="pip" %}
```terminal
pip install openai cohere
```
{% /Tab %}

{% Tab label="poetry" %}
```terminal
poetry add openai cohere
```
{% /Tab %}

{% Tab label="uv" %}
```terminal
uv pip install openai cohere
```
{% /Tab %}

{% /TabbedUseCaseCodeBlock %}

Creating collections with embedding function and custom configuration:

```python
import os
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction, CohereEmbeddingFunction

# Using the `embedding_function` argument
openai_collection = client.create_collection(
    name="my_openai_collection",
    embedding_function=OpenAIEmbeddingFunction(
        model_name="text-embedding-3-small"
    ),
    configuration={"hnsw": {"space": "cosine"}}
)

# Setting `embedding_function` in the collection's `configuration`
cohere_collection = client.get_or_create_collection(
    name="my_cohere_collection",
    configuration={
        "embedding_function": CohereEmbeddingFunction(
            model_name="embed-english-light-v2.0",
            truncate="NONE"
        ),
        "hnsw": {"space": "cosine"}
    }
)
```

**Note:** Many embedding functions require API keys to interface with the third party embeddings providers. The Chroma embedding functions will automatically look for the standard environment variable used to store a provider's API key. For example, the Chroma `OpenAIEmbeddingFunction` will set its `api_key` argument to the value of the `OPENAI_API_KEY` environment variable if it is set.

If your API key is stored in an environment variable with a non-standard name, you can configure your embedding function to use your custom environment variable by setting the `api_key_env_var` argument. In order for the embedding function to operate correctly, you will have to set this variable in every environment where you use your collection.

```python
cohere_ef = CohereEmbeddingFunction(
    api_key_env_var="MY_CUSTOM_COHERE_API_KEY",
    model_name="embed-english-light-v2.0",
    truncate="NONE",
)
```

{% /Tab %}

{% Tab label="typescript" %}

Install the `@chroma-core/openai` and `@chroma-core/cohere` packages:

{% TabbedUseCaseCodeBlock language="Terminal" %}

{% Tab label="npm" %}
```terminal
npm install @chroma-core/openai @chroma-core/cohere
```
{% /Tab %}

{% Tab label="pnpm" %}
```terminal
pnpm add @chroma-core/openai @chroma-core/cohere
```
{% /Tab %}

{% Tab label="yarn" %}
```terminal
yarn add @chroma-core/openai @chroma-core/cohere
```
{% /Tab %}

{% Tab label="bun" %}
```terminal
bun add @chroma-core/openai @chroma-core/cohere
```
{% /Tab %}

{% /TabbedUseCaseCodeBlock %}

Creating collections with embedding function and custom configuration:

```typescript
import { OpenAIEmbeddingFunction } from "@chroma-core/openai";
import { CohereEmbeddingFunction } from "@chroma-core/cohere"

// Using the `embedding_function` argument
openAICollection = client.createCollection({
    name: "my_openai_collection",
    embedding_function: new OpenAIEmbeddingFunction({
        model_name: "text-embedding-3-small"
    }),
    configuration: { hnsw: { space: "cosine" } }
});

// Setting `embedding_function` in the collection's `configuration`
cohereCollection = client.getOrCreate_collection({
    name: "my_cohere_collection",
    configuration: {
        embeddingFunction: new CohereEmbeddingFunction({
            modelName: "embed-english-light-v2.0",
            truncate: "NONE"
        }),
        hnsw: { space: "cosine" }
    }
})
```

**Note:** Many embedding functions require API keys to interface with the third party embeddings providers. The Chroma embedding functions will automatically look for the standard environment variable used to store a provider's API key. For example, the Chroma `OpenAIEmbeddingFunction` will set its `api_key` argument to the value of the `OPENAI_API_KEY` environment variable if it is set.

If your API key is stored in an environment variable with a non-standard name, you can configure your embedding function to use your custom environment variable by setting the `apiKeyEnvVar` argument. In order for the embedding function to operate correctly, you will have to set this variable in every environment where you use your collection.

```typescript
cohere_ef = CohereEmbeddingFunction({
    apiKeyEnvVar: "MY_CUSTOM_COHERE_API_KEY",
    modelName: "embed-english-light-v2.0",
    truncate: "NONE",
})
```

{% /Tab %}

{% /Tabs %}
