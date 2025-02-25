---
id: deepeval
name: DeepEval
---

# DeepEval

[DeepEval](https://docs.confident-ai.com/docs/integrations-chroma) is the open-source LLM evaluation framework. It provides 20+ research-backed metrics to help you evaluate and pick the best hyperparameters for your LLM system.

When building a RAG system, you can use DeepEval to pick the best parameters for your **Choma retriever** for optimal retrieval performance and accuracy: `n_results`, `distance_function`, `embedding_model`, `chunk_size`, etc.

{% Banner type="tip" %}
For more information on how to use DeepEval, see the [DeepEval docs](https://docs.confident-ai.com/docs/getting-started).
{% /Banner %}

## Getting Started

### Step 1: Installation

```CLI
pip install deepeval
```

### Step 2: Preparing a Test Case

Prepare a query, generate a response using your RAG pipeline, and store the retrieval context from your Chroma retriever to create an `LLMTestCase` for evaluation.

```python
...

def chroma_retriever(query):
    query_embedding = model.encode(query).tolist() # Replace with your embedding model
    res = collection.query(
        query_embeddings=[query_embedding],
        n_results=3
    )
    return res["metadatas"][0][0]["text"]

query = "How does Chroma work?"
retrieval_context = search(query)
actual_output = generate(query, retrieval_context)  # Replace with your LLM function

test_case = LLMTestCase(
    input=query,
    retrieval_context=retrieval_context,
    actual_output=actual_output
)
```

### Step 3: Evaluation

Define retriever metrics like `Contextual Precision`, `Contextual Recall`, and `Contextual Relevancy` to evaluate test cases. Recall ensures enough vectors are retrieved, while relevancy reduces noise by filtering out irrelevant ones.

{% Banner type="tip" %}
Balancing recall and relevancy is key. `distance_function` and `embedding_model` affects recall, while `n_results` and `chunk_size` impact relevancy.  
{% /Banner %}

```python
from deepeval.metrics import (
    ContextualPrecisionMetric,
    ContextualRecallMetric,
    ContextualRelevancyMetric
)
from deepeval import evaluate
...

evaluate(
    [test_case],
    [
        ContextualPrecisionMetric(),
        ContextualRecallMetric(),
        ContextualRelevancyMetric(),
    ],
)
```

### 4. Visualize and Optimize

To visualize evaluation results, log in to the [Confident AI (DeepEval platform)](https://www.confident-ai.com/) by running:

```
deepeval login
```

When logged in, running `evaluate` will automatically send evaluation results to Confident AI, where you can visualize and analyze performance metrics, identify failing retriever hyperparameters, and optimize your Chroma retriever for better accuracy.

![](https://github.com/confident-ai/deepeval/raw/main/assets/demo.gif)

{% Banner type="tip" %}
To learn more about how to use the platform, please see [this Quickstart Guide](https://docs.confident-ai.com/confident-ai/confident-ai-introduction).
{% /Banner %}

## Support

For any question or issue with integration you can reach out to the DeepEval team on [Discord](https://discord.com/invite/a3K9c8GRGt).
