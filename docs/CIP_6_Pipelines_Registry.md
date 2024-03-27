# CIP: Pipelines, The Registry and Storage

## Status

Current Status: `Under Discussion`

## Motivation

The top-line motivation behind this CIP is to provide a generic system for preprocessing data before
it is indexed by Chroma. This system should be flexible enough to allow users to
define their own preprocessing pipelines, while also providing a set of default
pipelines that can be used out of the box. The abstractions proposed in this CIP are born out of observing
several problems the Chroma community has encountered while using the current system.

1. Embedding Function Persistence

Today, Chroma allows users to implicitly transform the data they provide to add() by embedding it. For example,
users can call something akin to:

```python
collection = client.create_collection("my_collection", embedding_function=OpenAIEmbeddingFunction())
collection.add(ids, documents=["text_to_embed"])
```

One key problem with this approach is that this association between the embedding_function
and the collection is not persisted. So users are forced to remember which embedding_function
they used when they want to query the collection. This is not ideal. Concretely, this means
users do something like this:

```python
collection = client.create_collection("my_collection", embedding_function=OpenAIEmbeddingFunction())
collection.add(ids, documents=["text_to_embed"])

# ... some time later

collection = client.get_collection("my_collection", embedding_function=OpenAIEmbeddingFunction())
collection.query(documents=["text_to_query"])
```

This mechanism is error-prone, confusing and verbose. We would prefer a system that
allows for persisting the association between the embedding_function and the collection.

2. Chunking

When using Chroma, most users first chunk their documents into sizes that are appropriate
for the respective embedding model they are using. This chunking strategy is handled outside of
Chroma, and is often done in a way that is specific to the embedding model. Oftentimes users want to
maintain the association between the chunks and the original document. Today, users accomplish this by manually
chunking their documents outside of Chroma and then passing the chunks to Chroma, with the association
between the chunks and the original document being maintained in the metadata of the document
or by the user themselves. Users often forget to maintain this association, find it cumbersome
to do so, or want to iterate on this in a versioned way.

3. Loading & Storing documents

Users often want to load documents from a variety of sources. For example, users may want to load
documents from a database, a file system, or a remote service, these documents may be in a variety
of formats, such as text, images, or audio. Today, users are responsible for loading these documents
themselves and passing them to Chroma. Many users find this process to be cumbersome, and would
prefer if Chroma could handle this for them.

In addition, today, Chroma only supports storing text documents. For other modalities of data,
users are responsible for storing the data themselves and passing Chroma a reference to the data.
This is not ideal, as it forces users to maintain the association between the data and the reference themselves.

4. Server-side processing

Today, the one form of preprocessing that Chroma can handle for you is the embedding of text documents. This
embedding is done client-side, and is done in a synchronous fashion. This means that the client
is responsible for embedding the documents and sending the embeddings to the server. This is not ideal,
as it means that the client is responsible for the computational cost of embedding the documents. It also
limits the types of preprocessing that can be done, as the preprocessing must be done client-side and be constrained by
the computational resources available to the client.

In the planned distributed version of Chroma, the proxy layer
that communicates with the client and the processing layer could theoretically be on different machines. This means
that the processing layer could have access to more computational resources than the client. In addition, the processing
could be done in parallel, which would allow for faster processing.

5. Support for index customization and multiple indices

Chroma currently implicitly creates two segments for every collection, one for the embeddings - the vector segment
and one for the metadata - the metadata segment. In order to parameterize the index, users must pass in a set of
parameters to the metadata of the collection itself, which is then used to create the index. For example

```python
collection = client.create_collection("my_collection", embedding_function=OpenAIEmbeddingFunction(), metadata={"hnsw:ef_construction": 200})
```

This is error-prone, as simple typos go unchecked and are interpreted silently as generic metadata, and also is not discoverable.
Additionally, this means that users cannot create custom additional indices for their collections. For example, it may make sense
that a user wants to not only control the parameters of the vector segment but also create a custom index for the metadata segment, create
alternative vector segments with different parameters,

6. Composition with Language Model Generation

In the future, we may want to support the full RAG loop within Chroma. This means that we would want to support being able to take the output of retrieval and use it as input to a language model. This would require us to be able to compose the embedding function with a language model. This is not possible today, as the embedding function is not a first class citizen in Chroma.

## Proposal

We introduce three concepts in order to solve these problems - Pipelines, The Registry, and Storage

### Pipelines

A pipeline is a set of steps that are applied to the inputs to a chroma insert call. Broadly, everything including the segments written to are a part of the pipeline. The pipeline is responsible for loading the document, preprocessing the document, and writing the document to the segments. Pipeline steps are composable functions that can be chained together to form a pipeline. Each pipeline step is registered with a name, and can be referenced by that name. This registration can be thought of as similar to SQL functions.

#### Pipeline Step Registration - The Registry

For example, if you wanted to create a pipeline that chunked a document, you could do something like this:

First, register The "Chunk" pipeline step with the name "Chunk" as follows:

```python
@pipeline_step(name="Chunk")
def chunk(data: str) -> List[str]:
    # Chunk the data
    return chunked_data
```
 <!-- NOTE: ALTERNATIVE CONSIDERED = DO A CLASS BASED APPROACH, WHERE THE CLASS IS THE PIPELINE STEP -->

Then you can create a pipeline that chunks a document as follows:
```python
to_add = Chunk(data)
```

Calling Chunk() does not actually execute the pipeline step, instead it returns a reference to the pipeline step. This reference is then passed to the add step, which is responsible for executing the pipeline step. This is done to allow for the pipeline step to be executed on the server-side. A pipeline can always be executed locally by calling the run() method on the pipeline.

The assumption then, is that you provide the server with the relevant pipeline steps to execute. The exact mechanism for this is later defined. For local execution, the pipeline steps are executed on the client-side.

#### Pipeline Step Composition

Pipeline steps can be composed, and return a reference to the composed pipeline step. For example, if you wanted to create a pipeline that chunked a document and then embedded the chunks, you could do something like this:

```python
to_add = Embed(Chunk(data))
```

Or perhaps you want to chunk, embed and then quantize the embeddings:

```python
to_add = Quantize(Embed(Chunk(data)))
```

NOTE: This syntax could be confusing as the order of operations is reversed. We could consider alternative designs.

#### Default Pipeline and Pipeline Persistence

It is important to preserve the existing behavior of our API where users can simply pass text to the add step and have it be embedded. To do this, we will create a default pipeline that is used when no pipeline is provided. This default pipeline will simply embed the text. This default pipeline will be persisted with the collection, and will be used when no pipeline is provided. This means that users can create a collection with a default pipeline, and then query the collection without having to provide the pipeline. This is a significant improvement over the current system, where users must remember which embedding function they used when they created the collection.

Pipelines can be persisted by serializing the pipeline steps and storing them in the metadata of the collection. This means that when a collection is loaded, the pipeline steps can be deserialized and used to recreate the pipeline. This is a significant improvement over the current system, where users must remember which embedding function they used when they created the collection.

For example, the default pipeline would simply Embed the text, and would be persisted as follows:

```python
collection = client.create_collection("my_collection", pipeline=DefaultEmbeddingFunctionPipelineStep())

# ... some time later

collection = client.get_collection("my_collection") # The default pipeline is automatically loaded
```

#### Query Pipelines

In order to support embedding the data automatically for the user on query, we propose the introduction of the query_pipeline, which is the default pipeline run on the query data. This pipeline is persisted with the collection, and is used when no pipeline is provided. This means that users can create a collection with a default query pipeline, and then query the collection without having to provide the query pipeline.

For example, the default query pipeline would simply Embed the text, and would be defined as follows:

```python
collection = client.create_collection("my_collection", pipeline=DefaultEmbeddingFunctionPipelineStep(), query_pipeline=DefaultEmbeddingFunctionPipelineStep()) # The defaults would not actually have to be specified, as they would be the default values. Here we are specifying them for clarity.
collection.add(ids, data=["text_to_embed"]) # The content of the data is embedding using the pipeline and then added to the collection
collection.query(data=["text_to_query"]) # The content of the data is embedding using the query pipeline and then queried against the collection
```

This allows for the pipelines at insert time, and the pipelines at query time to be different. This is useful for a variety of reasons. For example, if you wanted to chunk the data at insert time, but not at query time, you could do something like this:

```python
collection = client.create_collection("my_collection", pipeline=Embed(Chunk()), query_pipeline=Embed())
```

#### The I/0 of a pipeline stage
The input to a pipeline step is the set of parameters provided to an insert call in Chroma - ids, documents, metadata and embeddings. The size of each of these fields must match or be empty. This way, pipeline stages can both pass through data to subsequent stages, or they can populate them themselves. The output of the final stage of a pipeline must be a valid input to the insert call in Chroma, i.e it must contain ids, documents, metadata and embeddings. For example, imagine a pipeline that consumes documents, chunks them, and then embeds them.

```python
data = ["large_document_1", "large_document_2"]
pipeline = Embed(Chunk())
collection = client.create_collection("my_collection", pipeline=pipeline)
collection.add(data=data, ids=[0, 1])

# Input to pipeline would be {ids: [0, 1], documents: ["large_document_1", "large_document_2"], metadata: None, embeddings: None}
# Output of Chunk could be {ids: [A, B, C, D], documents: ["chunk_1", "chunk_2", "chunk_3", "chunk_4"], metadata: [{"source_id": 0}, {"source_id": 0}, {"source_id": 1}, {"source_id": 1}], embeddings: None}
# Output of Embed, the final stage could be {ids: [0, 1], documents: ["chunk_1", "chunk_2", "chunk_3", "chunk_4"], metadata: [{"source_id": 0}, {"source_id": 0}, {"source_id": 1}, {"source_id": 1}], embeddings: [embedding_1, embedding_2, embedding_3, embedding_4]}
```


#### Pipeline overrides
While defined at the collection level, pipelines can also be overridden at the time of an insert or query call. This allows for flexibility in the pipeline that is used for a specific insert or query call. For example, if you wanted to chunk the data initially, but later insert a custom chunk, you could do something like this:

```python
pipeline = Embed(Chunk())
collection = client.create_collection("my_collection", pipeline=pipeline)
# Add initial data
collection.add(data=["large_document_1", "large_document_2"], ids=[0, 1])

# Now we just want to add a chunk
collection.add(data=["custom_chunk"], ids=[2], pipeline=Embed())
# Or you can pass a pipeline to data
collection.add(data=Embed({"data": "custom_chunk_2", ids: [3]}))
```
Note that you could also do the opposite, where you do not want to chunk the data initially, but later want to chunk the data. For example:

```python
collection = client.create_collection("my_collection", pipeline=DefaultEmbeddingFunctionPipelineStep())
# Add initial data
collection.add(data=["small_chunk_1", "small_chunk_2"], ids=[0, 1])
# Now add large documents with chunking
collection.add(data=["large_document_1", "large_document_2"], ids=[2, 3], pipeline=DefaultEmbeddingFunctionPipelineStep(Chunk()))
```

You could also run the pipeline manually and then pass the output to the add step. For example:

```python
collection = client.create_collection("my_collection", pipeline=DefaultEmbeddingFunctionPipelineStep())
# Add initial data
collection.add(data=["small_chunk_1", "small_chunk_2"], ids=[0, 1])
# Now add large documents with chunking but manually run the pipeline
pipeline = DefaultEmbeddingFunctionPipelineStep(Chunk(["large_document_1", "large_document_2"]))
data = pipeline.run()
collection.add(data=data, ids=[2, 3])
```

#### Server-side registration of pipeline steps
In order to register pipeline steps on the server, you will have to provide a python file that contains the pipeline steps, registered with the
@pipeline_step decorator. For now, these are defined statically with each deploy of the server. In the future, we could allow for dynamic registration of pipeline steps.

#### Future work: Output pipelines
While not explicitly a part of this CIP, in the future, we could also add Output pipelines that are run on the output
of a query. This would allow things like re-ranking, or filtering of the results. For example, if you wanted to re-rank the results of a query, you could do something like this:

```python
collection = client.create_collection("my_collection", pipeline=DefaultEmbeddingFunctionPipelineStep(), output_pipeline=ReRank())
# Add initial data
collection.add(data=["small_chunk_1", "small_chunk_2"], ids=[0, 1])
# Query the collection
collection.query(data=["query"], n_results=100) # ReRank will run before the results are returned
```

NOTE: Generically, one can view the existing behavior of chroma as a pipeline that runs metadata filtering followed by vector search. In the future, we could allow exposing this basic pipeline to the user as the default.

This abstraction would also allow for the full RAG loop to be implemented in Chroma. For example, if you wanted to run a query, and then generate a response using a language model, you could do something like this:

```python
collection.query(data=["query"], n_results=100, output_pipeline=Generate(Composite(prompt)))
```

This pipeline first runs a partially initialized composite step on the results of the query, and then runs the generate step on the output of the composite step. This allows for the full RAG loop to be implemented in Chroma. The composite step combines the prompt and the retrieved documents, and the generate step generates the response.

#### Future work: Parallel pipeline execution
Not all pipeline stages need to executed sequentially. In the future, we can add support for parallel pipeline execution by changing how the pipeline is defined.

## Storage Pipelines
Chroma right now implicitly stores your documents, however we also want to support multimodal embeddings and part of this CIP is to propose we deprecate documents= in favor of data=. The contract then becomes that the data you input (or that is generated by your pipeline) is stored by default and can be retrieved by the user in some form. We also want to support storing mixed modalities in a collection. For example, a CLIP based collection should be able
to store both text and images.

We propose to add Storage Pipeline steps capable of storing the inputted data into a Storage layer, and then storing a string reference to that data in the metadata of the document. This is already how the document is currently stored and backed by the metadata segment. The metadata segment stores a KV pair "chroma:document" -> the document contents. For example, say you had an image in memory and wanted to store it in Chroma, you could do something like this:

```python
collection = client.create_collection("my_collection", pipeline=DefaultEmbeddingFunctionPipelineStep(ImageStoragePipelineStep()))
collection.add(data=[image], ids=[0]) # image is a PIL image, or numpy array etc

# The ImageStoragePipelineStep would store the image in the storage layer, and then pass through the url to the image instead.
# The metadata segment would then store the KV pair "chroma:document" -> "https://storage.com/image_1"
```

We can ship two StoragePipelines to start

1. LocalStoragePipelineStep - Stores the data in a local file system
2. S3StoragePipelineStep - Stores the data in S3

For now, we would just return the stored string to the user, since they can decide if they want to load it or not. In the future, we could add a LoadFromStorage output pipeline step that would load the data from the storage layer and return it to the user.

For example, if you wanted to load the data from the storage layer and return it to the user, you could do something like this:

```python
collection = client.create_collection("my_collection", pipeline=DefaultEmbeddingFunctionPipelineStep(ImageStoragePipelineStep()), query_pipeline=DefaultEmbeddingFunctionPipelineStep(), output_pipeline=LoadFromStorage())
collection.add(data=[image], ids=[0]) # image is a PIL image, or numpy array etc
collection.query(data=[image], n_results=100) # LoadFromStorage will run before the results are returned and will load the data from the storage layer and return it to the user in memory
```

#### Future work: More Storage Pipelines
We can in the future add more storage pipelines

## Commentary: Mental Models
Two mental models to share that led to the development of this CIP are:

1. Collections are just a description input pipelines, query pipelines and output pipelines. Today, the pipeline runs an EF and stores it in two segments.
Everything that runs before writing to log is the input pipeline, everything that runs after vector search is the output pipeline, and the query pipeline merely transforms the query.
2. We were heavily influenced by SQL semantics. For example, the pipeline steps are similar to SQL functions, output pipelines can be seen as performing joins, etc.

Note that this CIP does not address problem #5 - we will address that separately in a future CIP. However, all the other problems can be solved with the abstractions proposed here.

## **Compatibility, Deprecation, and Migration Plan**
1. We will deprecate the documents= parameter in the api over several releases in favor of data=. This will allow us to support other modalities of data, such as images, audio, etc.
2. We will wrap all embedding functions into a pipeline step.
3. We will add a default pipeline to the collection.
4. We will add a default query pipeline to the collection.

## **Test Plan**

A rigorous test suite with hypothesis will be created.
