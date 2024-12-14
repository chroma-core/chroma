import importlib
import logging
from typing import Optional, Union, cast

from chromadb.api.types import (
    Document,
    Documents,
    Embedding,
    EmbeddingFunction,
    Embeddings,
    Image,
    Images,
    is_document,
    is_image,
)
from chromadb.errors import InvalidArgumentError


logger = logging.getLogger(__name__)


class OpenCLIPEmbeddingFunction(EmbeddingFunction[Union[Documents, Images]]):
    def __init__(
        self,
        model_name: str = "ViT-B-32",
        checkpoint: str = "laion2b_s34b_b79k",
        device: Optional[str] = "cpu",
    ) -> None:
        try:
            import open_clip
        except ImportError:
            raise InvalidArgumentError(
                "The open_clip python package is not installed. Please install it with `pip install open-clip-torch`. https://github.com/mlfoundations/open_clip"
            )
        try:
            self._torch = importlib.import_module("torch")
        except ImportError:
            raise InvalidArgumentError(
                "The torch python package is not installed. Please install it with `pip install torch`"
            )

        try:
            self._PILImage = importlib.import_module("PIL.Image")
        except ImportError:
            raise InvalidArgumentError(
                "The PIL python package is not installed. Please install it with `pip install pillow`"
            )

        model, _, preprocess = open_clip.create_model_and_transforms(
            model_name=model_name, pretrained=checkpoint
        )
        self._model = model
        self._model.to(device)
        self._preprocess = preprocess
        self._tokenizer = open_clip.get_tokenizer(model_name=model_name)

    def _encode_image(self, image: Image) -> Embedding:
        pil_image = self._PILImage.fromarray(image)
        with self._torch.no_grad():
            image_features = self._model.encode_image(
                self._preprocess(pil_image).unsqueeze(0)
            )
            image_features /= image_features.norm(dim=-1, keepdim=True)
            return cast(Embedding, image_features.squeeze().cpu().numpy())

    def _encode_text(self, text: Document) -> Embedding:
        with self._torch.no_grad():
            text_features = self._model.encode_text(self._tokenizer(text))
            text_features /= text_features.norm(dim=-1, keepdim=True)
            return cast(Embedding, text_features.squeeze().cpu().numpy())

    def __call__(self, input: Union[Documents, Images]) -> Embeddings:
        embeddings: Embeddings = []
        for item in input:
            if is_image(item):
                embeddings.append(self._encode_image(cast(Image, item)))
            elif is_document(item):
                embeddings.append(self._encode_text(cast(Document, item)))
        return embeddings
