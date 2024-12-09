---
id: 'roboflow'
name: Roboflow
---

# Roboflow

You can use [Roboflow Inference](https://inference.roboflow.com) with Chroma to calculate multi-modal text and image embeddings with CLIP. through the `RoboflowEmbeddingFunction` class. Inference can be used through the Roboflow cloud, or run on your hardware.

## Roboflow Cloud Inference

To run Inference through the Roboflow cloud, you will need an API key. [Learn how to retrieve a Roboflow API key](https://docs.roboflow.com/api-reference/authentication#retrieve-an-api-key).

You can pass it directly on creation of the `RoboflowEmbeddingFunction`:

```python
from chromadb.utils.embedding_functions import RoboflowEmbeddingFunction

roboflow_ef = RoboflowEmbeddingFunction(api_key=API_KEY)
```

Alternatively, you can set your API key as an environment variable:

```bash
export ROBOFLOW_API_KEY=YOUR_API_KEY
```

Then, you can create the `RoboflowEmbeddingFunction` without passing an API key directly:

```python
from chromadb.utils.embedding_functions import RoboflowEmbeddingFunction

roboflow_ef = RoboflowEmbeddingFunction()
```

## Local Inference

You can run Inference on your own hardware.

To install Inference, you will need Docker installed. Follow the [official Docker installation instructions](https://docs.docker.com/engine/install/) for guidance on how to install Docker on the device on which you are working.

Then, you can install Inference with pip:

```bash
pip install inference inference-cli
```

With Inference installed, you can start an Inference server. This server will run in the background. The server will accept HTTP requests from the `RoboflowEmbeddingFunction` to calculate CLIP text and image embeddings for use in your application:

To start an Inference server, run:

```bash
inference server start
```

Your Inference server will run at `http://localhost:9001`.

Then, you can create the `RoboflowEmbeddingFunction`:

```python
from chromadb.utils.embedding_functions import RoboflowEmbeddingFunction

roboflow_ef = RoboflowEmbeddingFunction(api_key=API_KEY, server_url="http://localhost:9001")
```

This function will calculate embeddings using your local Inference server instead of the Roboflow cloud.

For a full tutorial on using Roboflow Inference with Chroma, refer to the [Roboflow Chroma integration tutorial](https://github.com/chroma-core/chroma/blob/main/examples/use_with/roboflow/embeddings.ipynb).
