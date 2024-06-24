from chromadb.api.types import Documents, EmbeddingFunction, Embeddings
from typing import Any, cast

class LlamaCppEmbeddingFunction(EmbeddingFunction):
    
    def __init__(self, model_path: str = "", huggingface_repo_id: str = "", huggingface_filename: str = "", **kwargs: Any) -> None:
        """
        Initialize the LlamaCppEmbeddingFunction. This function will embed documents using the Llama-CPP-Python library.

        Args:
            model_path (str): Path to the model file
            huggingface_repo_id (str): The name of the HuggingFace id to use. i.e. "username/repo_name".
            hugingface_filename (str): The name of the file to download from the HuggingFace model. i.e. "*q8_0.gguf".
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
        except ImportError:
            raise ValueError(
                "The huggingface-hub python package is not installed. Please install it with `pip install huggingface_hub`"
            )
        self.model_path = model_path

        # Check if verbose is in kwargs, if not set to False
        if 'verbose' not in kwargs:
            kwargs['verbose'] = False
        # Force embedding to be True
        kwargs['embedding'] = True
        # Check if the computer has a GPU, if not set n_gpu_layers to 0
        if cuda.is_available():
            if 'n_gpu_layers' not in kwargs:
                kwargs['n_gpu_layers'] = 1
        else:
            kwargs['n_gpu_layers'] = 0

        try:
            if huggingface_repo_id and huggingface_filename:
                self.llm_embedding = Llama.from_pretrained(repo_id=huggingface_repo_id, filename=huggingface_filename, **kwargs)
            elif huggingface_repo_id and not huggingface_filename:
                raise ValueError("Please provide a filename to download from the HuggingFace model.")
            elif not huggingface_repo_id and huggingface_filename:
                raise ValueError("Please provide a HuggingFace repo id to download the model from.")
            elif model_path:
                self.llm_embedding = Llama(model_path, **kwargs)
            else:
                raise ValueError("Please provide either a model path or a HuggingFace repo id and filename.")
        except Exception as e:
            raise Exception(f"Error initializing LlamaCppEmbeddingFunction: {e}")

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for the given input documents.

        Args:
            input (Documents): A list of documents to embed.

        Returns:
            Embeddings: A list of embeddings for the input documents.
        """
        try:
            import numpy as np
        except ImportError:
            raise ValueError(
                "The numpy python package is not installed. Please install it with `pip install numpy`"
            )
        # Create embeddings
        llama_embeddings = [embedding['embedding'] for embedding in self.llm_embedding.create_embedding(list(input))['data']]
        # Convert to numpy array
        llama_embeddings = np.array(llama_embeddings)

        # embed the documents somehow
        return cast(
            Embeddings,
           llama_embeddings.tolist()
        )
