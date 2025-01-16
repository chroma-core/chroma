import logging
from typing import Any, List, Union

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings, Images

logger = logging.getLogger(__name__)


def create_langchain_embedding(langchain_embdding_fn: Any):  # type: ignore
    try:
        from langchain_core.embeddings import Embeddings as LangchainEmbeddings
    except ImportError:
        raise ValueError(
            "The langchain_core python package is not installed. Please install it with `pip install langchain-core`"
        )

    class ChromaLangchainEmbeddingFunction(
        LangchainEmbeddings, EmbeddingFunction[Union[Documents, Images]]  # type: ignore
    ):
        """
        This class is used as bridge between langchain embedding functions and custom chroma embedding functions.
        """

        def __init__(self, embedding_function: LangchainEmbeddings) -> None:
            """
            Initialize the ChromaLangchainEmbeddingFunction

            Args:
                embedding_function : The embedding function implementing Embeddings from langchain_core.
            """
            self.embedding_function = embedding_function

        def embed_documents(self, documents: Documents) -> List[List[float]]:
            return self.embedding_function.embed_documents(documents)  # type: ignore

        def embed_query(self, query: str) -> List[float]:
            return self.embedding_function.embed_query(query)  # type: ignore

        def embed_image(self, uris: List[str]) -> List[List[float]]:
            if hasattr(self.embedding_function, "embed_image"):
                return self.embedding_function.embed_image(uris)  # type: ignore
            else:
                raise ValueError(
                    "The provided embedding function does not support image embeddings."
                )

        def __call__(self, input: Documents) -> Embeddings:  # type: ignore
            """
            Get the embeddings for a list of texts or images.

            Args:
                input (Documents | Images): A list of texts or images to get embeddings for.
                Images should be provided as a list of URIs passed through the langchain data loader

            Returns:
                Embeddings: The embeddings for the texts or images.

            Example:
                >>> langchain_embedding = ChromaLangchainEmbeddingFunction(embedding_function=OpenAIEmbeddings(model="text-embedding-3-large"))
                >>> texts = ["Hello, world!", "How are you?"]
                >>> embeddings = langchain_embedding(texts)
            """
            # Due to langchain quirks, the dataloader returns a tuple if the input is uris of images
            if input[0] == "images":
                return self.embed_image(list(input[1]))  # type: ignore

            return self.embed_documents(list(input))  # type: ignore

    return ChromaLangchainEmbeddingFunction(embedding_function=langchain_embdding_fn)
