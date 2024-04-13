import logging

from chromadb.api.types import (
    Documents,
    EmbeddingFunction,
    Embeddings,
)

from typing import Any
import json

logger = logging.getLogger(__name__)



class AmazonBedrockEmbeddingFunction(EmbeddingFunction[Documents]):
    def __init__(
        self,
        session: "boto3.Session",  # noqa: F821 # Quote for forward reference
        model_name: str = "amazon.titan-embed-text-v1",
        **kwargs: Any,
    ):
        """Initialize AmazonBedrockEmbeddingFunction.

        Args:
            session (boto3.Session): The boto3 session to use.
            model_name (str, optional): Identifier of the model, defaults to "amazon.titan-embed-text-v1"
            **kwargs: Additional arguments to pass to the boto3 client.

        Example:
            >>> import boto3
            >>> session = boto3.Session(profile_name="profile", region_name="us-east-1")
            >>> bedrock = AmazonBedrockEmbeddingFunction(session=session)
            >>> texts = ["Hello, world!", "How are you?"]
            >>> embeddings = bedrock(texts)
        """

        self._model_name = model_name

        self._client = session.client(
            service_name="bedrock-runtime",
            **kwargs,
        )

    def __call__(self, input: Documents) -> Embeddings:
        accept = "application/json"
        content_type = "application/json"
        embeddings = []
        for text in input:
            input_body = {"inputText": text}
            body = json.dumps(input_body)
            response = self._client.invoke_model(
                body=body,
                modelId=self._model_name,
                accept=accept,
                contentType=content_type,
            )
            embedding = json.load(response.get("body")).get("embedding")
            embeddings.append(embedding)
        return embeddings