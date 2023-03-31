from chromadb.api.types import Documents, EmbeddingFunction, Embeddings
import onnxruntime as ort
import numpy as np
from tokenizers import Tokenizer
from pathlib import Path
import os
import tarfile
import requests
from tqdm import tqdm
from typing import List, cast, Optional

class SentenceTransformerEmbeddingFunction(EmbeddingFunction):
    # If you have a beefier machine, try "gtr-t5-large".
    # for a full list of options: https://huggingface.co/sentence-transformers, https://www.sbert.net/docs/pretrained_models.html
    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ValueError(
                "The sentence_transformers python package is not installed. Please install it with `pip install sentence_transformers`"
            )
        self._model = SentenceTransformer(model_name)

    def __call__(self, texts: Documents) -> Embeddings:
        return self._model.encode(list(texts), convert_to_numpy=True).tolist()


class OpenAIEmbeddingFunction(EmbeddingFunction):
    def __init__(self, api_key: str, model_name: str = "text-embedding-ada-002"):
        try:
            import openai
        except ImportError:
            raise ValueError(
                "The openai python package is not installed. Please install it with `pip install openai`"
            )

        openai.api_key = api_key
        self._client = openai.Embedding
        self._model_name = model_name

    def __call__(self, texts: Documents) -> Embeddings:
        # replace newlines, which can negatively affect performance.
        texts = [t.replace("\n", " ") for t in texts]
        # Call the OpenAI Embedding API in parallel for each document
        return [
            result["embedding"]
            for result in self._client.create(
                input=texts,
                engine=self._model_name,
            )["data"]
        ]


class CohereEmbeddingFunction(EmbeddingFunction):
    def __init__(self, api_key: str, model_name: str = "large"):
        try:
            import cohere
        except ImportError:
            raise ValueError(
                "The cohere python package is not installed. Please install it with `pip install cohere`"
            )

        self._client = cohere.Client(api_key)
        self._model_name = model_name

    def __call__(self, texts: Documents) -> Embeddings:
        # Call Cohere Embedding API for each document.
        return [
            embeddings for embeddings in self._client.embed(texts=texts, model=self._model_name)
        ]


class HuggingFaceEmbeddingFunction(EmbeddingFunction):
    def __init__(self, api_key: str, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        try:
            import requests
        except ImportError:
            raise ValueError(
                "The requests python package is not installed. Please install it with `pip install requests`"
            )
        self._api_url = (
            f"https://api-inference.huggingface.co/pipeline/feature-extraction/{model_name}"
        )
        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {api_key}"})

    def __call__(self, texts: Documents) -> Embeddings:
        # Call HuggingFace Embedding API for each document
        return self._session.post(
            self._api_url, json={"inputs": texts, "options": {"wait_for_model": True}}
        ).json()


class InstructorEmbeddingFunction(EmbeddingFunction):
    # If you have a GPU with at least 6GB try model_name = "hkunlp/instructor-xl" and device = "cuda"
    # for a full list of options: https://github.com/HKUNLP/instructor-embedding#model-list
    def __init__(self, model_name: str = "hkunlp/instructor-base", device="cpu"):
        try:
            from InstructorEmbedding import INSTRUCTOR
        except ImportError:
            raise ValueError(
                "The InstructorEmbedding python package is not installed. Please install it with `pip install InstructorEmbedding`"
            )
        self._model = INSTRUCTOR(model_name, device=device)

    def __call__(self, texts: Documents) -> Embeddings:
        return self._model.encode(texts).tolist()


#### CHROMA DEFAULT ONNX EMEBEDDING FUNCTION ####
# In order to remove dependencies on sentence-transformers, which in turn depends on pytorch and sentence-piece we have
# created a default ONNX embedding function that implements the same functionality as "all-MiniLM-L6-v2" from sentence-transformers.
# visit https://github.com/chroma-core/onnx-embedding for the source code to generate and verify the ONNX model.

# Borrow'ed from https://gist.github.com/yanqd0/c13ed29e29432e3cf3e7c38467f42f51
# Download with tqdm to preserve the sentence-transformers experience
def download(url: str, fname: Path, chunk_size=1024):
    resp = requests.get(url, stream=True)
    total = int(resp.headers.get('content-length', 0))
    with open(fname, 'wb') as file, tqdm(
        desc=str(fname),
        total=total,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for data in resp.iter_content(chunk_size=chunk_size):
            size = file.write(data)
            bar.update(size)

# Use pytorches default epsilon for division by zero
# https://pytorch.org/docs/stable/generated/torch.nn.functional.normalize.html
def normalize(v):
    norm = np.linalg.norm(v)
    if norm == 0:
        norm = 1e-12
    return v / norm


class DefaultOnnxEmbeddingFunction(EmbeddingFunction):

    MODEL_NAME = "all-MiniLM-L6-v2"
    DOWNLOAD_PATH = Path.home() / ".cache" / "chroma" / "onnx_models" / MODEL_NAME
    EXTRACTED_FOLDER_NAME = "onnx"
    ARCHIVE_FILENAME = "onnx.tar.gz"
    MODEL_DOWNLOAD_URL = "https://chroma-onnx-models.s3.amazonaws.com/all-MiniLM-L6-v2/onnx.tar.gz"
    tokenizer: Optional[Tokenizer] = None
    model: Optional[ort.InferenceSession] = None

    def forward(self, documents: List[str], batch_size: int = 32):
        # We need to cast to the correct type because the type checker doesn't know that init_model_and_tokenizer will set the values
        self.tokenizer = cast(Tokenizer, self.tokenizer)
        self.model = cast(ort.InferenceSession, self.model)
        all_embeddings = []
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            encoded = [self.tokenizer.encode(d) for d in batch]
            input_ids = np.array([e.ids for e in encoded])
            attention_mask = np.array([e.attention_mask for e in encoded])
            onnx_input = {
                "input_ids": np.array(input_ids, dtype=np.int64),
                "attention_mask": np.array(attention_mask, dtype=np.int64),
                "token_type_ids": np.array([np.zeros(len(e), dtype=np.int64) for e in input_ids], dtype=np.int64),
            }
            model_output = self.model.run(None, onnx_input)
            last_hidden_state = model_output[0]
            # Perform mean pooling with attention weighting
            input_mask_expanded = np.broadcast_to(np.expand_dims(attention_mask, -1), last_hidden_state.shape)
            embeddings = np.sum(last_hidden_state * input_mask_expanded, 1) / np.clip(input_mask_expanded.sum(1), a_min=1e-9, a_max=None)
            embeddings = normalize(embeddings).astype(np.float32)
            all_embeddings.append(embeddings)
        return np.concatenate(all_embeddings)

    def init_model_and_tokenizer(self):
        if(self.model == None and self.tokenizer == None):
            self.tokenizer = Tokenizer.from_file(str(self.DOWNLOAD_PATH / self.EXTRACTED_FOLDER_NAME / "tokenizer.json"))
            # max_seq_length = 256, for some reason sentence-transformers uses 256 even though the HF config has a max length of 128
            # https://github.com/UKPLab/sentence-transformers/blob/3e1929fddef16df94f8bc6e3b10598a98f46e62d/docs/_static/html/models_en_sentence_embeddings.html#LL480
            self.tokenizer.enable_truncation(max_length=256)
            self.tokenizer.enable_padding(pad_id=0, pad_token="[PAD]", length=256)
            self.model = ort.InferenceSession(str(self.DOWNLOAD_PATH / self.EXTRACTED_FOLDER_NAME / "model.onnx"))

    def __call__(self, texts: Documents) -> Embeddings:
        # Only download the model when it is actually used
        self.download_model_if_not_exists()
        self.init_model_and_tokenizer()
        return self.forward(texts).tolist()

    def download_model_if_not_exists(self):
        # Model is not downloaded yet
        if not os.path.exists(self.DOWNLOAD_PATH / self.ARCHIVE_FILENAME):
            os.makedirs(self.DOWNLOAD_PATH, exist_ok=True)
            download(self.MODEL_DOWNLOAD_URL, self.DOWNLOAD_PATH / self.ARCHIVE_FILENAME)
            with tarfile.open(self.DOWNLOAD_PATH / self.ARCHIVE_FILENAME, "r:gz") as tar:
                tar.extractall(self.DOWNLOAD_PATH)
