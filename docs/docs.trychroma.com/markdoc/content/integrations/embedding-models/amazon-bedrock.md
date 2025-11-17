---
id: amazon-bedrock
name: Amazon Bedrock
---

# Amazon Bedrock

Chroma provides a convenient wrapper around Amazon Bedrock's embedding API. This embedding function runs remotely on Amazon Bedrock's servers, and requires AWS credentials configured via boto3.

{% Tabs %}

{% Tab label="python" %}

This embedding function relies on the `boto3` python package, which you can install with `pip install boto3`.

```python
import boto3
from chromadb.utils.embedding_functions import AmazonBedrockEmbeddingFunction

session = boto3.Session(profile_name="profile", region_name="us-east-1")
bedrock_ef = AmazonBedrockEmbeddingFunction(
    session=session,
    model_name="amazon.titan-embed-text-v1"
)

texts = ["Hello, world!", "How are you?"]
embeddings = bedrock_ef(texts)
```

You can pass in an optional `model_name` argument, which lets you choose which Amazon Bedrock embedding model to use. By default, Chroma uses `amazon.titan-embed-text-v1`.

{% /Tab %}

{% /Tabs %}

{% Banner type="tip" %}
Visit Amazon Bedrock [documentation](https://docs.aws.amazon.com/bedrock/) for more information on available models and configuration.
{% /Banner %}
