from chromadb.api.types import Documents, EmbeddingFunction, Embeddings
from typing import Any, cast, List
import numpy as np


class LlamaCppEmbeddingFunction(EmbeddingFunction):
    def __init__(
        self,
        model_path: str = "",
        model_file_name: str = "",
        pooling_method: str = "mean",
        **kwargs: Any,
    ) -> None:
        """
        Initialize the LlamaCppEmbeddingFunction. This function will embed documents using the Llama-CPP-Python library.

        Example:
            To use a huggingface model: LlamaCppEmbeddingFunction(model_path="username/repo_name", huggingface_filename="*q8_0.gguf")
            To use a local model: LlamaCppEmbeddingFunction(model_path="path/to/model")
        Args:
            model_path (str): Path to the model file or the name of the huggingface id to use i.e. "username/repo_name".
            hugingface_filename (str): The name of the file to download from the HuggingFace model. i.e. "*q8_0.gguf".
            pooling_method (str): The pooling method to use. Options are "mean", "max".
            kwargs: Additional arguments to pass to the Llama constructor.
                * n_ctx (int): The context size.
                * n_threads (int): The number of cpu threads to use.
                * n_gpu_layers (int): The number of layers to run on the GPU.
        """
        # import external libraries
        try:
            from llama_cpp import Llama
        except ImportError:
            raise ValueError(
                "The llama_cpp python package is not installed. Please install it with `pip install llama-cpp-python`"
            )
        try:
            from torch import cuda
        except ImportError:
            raise ValueError(
                "The torch python package is not installed. Please install it with `pip install torch`"
            )
        try:
            from huggingface_hub import hf_hub_download

            if hf_hub_download is None:
                raise ImportError
        except ImportError:
            raise ValueError(
                "The huggingface-hub python package is not installed. Please install it with `pip install huggingface_hub`"
            )

        self.model_path = model_path

        # Check if verbose is in kwargs, if not set to False
        if "verbose" not in kwargs:
            kwargs["verbose"] = False
        # Force embedding to be True
        kwargs["embedding"] = True
        # Check if the computer has a GPU, if not set n_gpu_layers to 0
        if cuda.is_available():
            if "n_gpu_layers" not in kwargs:
                kwargs["n_gpu_layers"] = 1
        else:
            kwargs["n_gpu_layers"] = 0

        try:
            if model_path and model_file_name:
                self.llm_embedding = Llama.from_pretrained(
                    repo_id=model_path, filename=model_file_name, **kwargs
                )
            elif model_path:
                self.llm_embedding = Llama(model_path, **kwargs)
            else:
                raise ValueError(
                    "Please provide either a model path or a HuggingFace repo id and filename."
                )
        except Exception as e:
            raise Exception(f"Error initializing LlamaCppEmbeddingFunction: {e}")
            return

        if pooling_method in ["mean", "max"]:
            self.pooling_method = pooling_method
        else:
            raise ValueError("Invalid pooling method. Please choose 'mean', 'max'.")

        # Check if pooling is required
        test_embeddings = self.llm_embedding.embed(["This is a test sentence."])
        np_test_embeddings = np.array(test_embeddings)
        # If the np array is 3d, then pooling is required
        if np_test_embeddings.ndim == 3:
            self.need_pooling = True
        elif np_test_embeddings.ndim == 2:
            self.need_pooling = False
        else:
            raise ValueError(
                "The embedding is not 2d or 3d. Please check the embedding output."
            )

    def mean_pooling(self, document_embeddings: List[np.ndarray]) -> np.array:
        """
        Perform mean pooling on the document embedding.

        Args:
            document_embedding (np.array): The document embedding to pool.

        Returns:
            np.array: The pooled document embedding.
        """
        return np.array(
            [
                np.mean(document_embedding, axis=0)
                for document_embedding in document_embeddings
            ]
        )

    def max_pooling(self, document_embeddings: List[np.ndarray]) -> np.array:
        """
        Perform max pooling on the document embedding.

        Args:
            document_embedding (np.array): The document embedding to pool.

        Returns:
            np.array: The pooled document embedding.
        """
        return np.array(
            [
                np.max(document_embedding, axis=0)
                for document_embedding in document_embeddings
            ]
        )

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for the given input documents.

        Args:
            input (Documents): A list of documents to embed.

        Returns:
            Embeddings: A list of embeddings for the input documents.
        """
        llama_embeddings = self.llm_embedding.embed(list(input))
        if not self.need_pooling:
            # Create embeddings
            # Convert to numpy array
            llama_embeddings = np.array(llama_embeddings)

            # embed the documents somehow
            return cast(Embeddings, llama_embeddings.tolist())
        if self.need_pooling:
            # Create embeddings
            if self.pooling_method == "mean":
                # Convert to numpy array
                llama_embeddings = self.mean_pooling(llama_embeddings)
            elif self.pooling_method == "max":
                # Convert to numpy array
                llama_embeddings = self.max_pooling(llama_embeddings)
            else:
                raise ValueError("Invalid pooling method. Please choose 'mean', 'max'")

            return cast(Embeddings, llama_embeddings.tolist())
