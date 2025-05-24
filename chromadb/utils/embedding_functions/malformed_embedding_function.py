from chromadb.api.types import EmbeddingFunction, Embeddable, Embeddings
from typing import Dict, Any


class MalformedEmbeddingFunction(EmbeddingFunction[Embeddable]):
    def __init__(
        self,
        malformed_ef_name: str,
        config: Dict[str, Any],
    ):
        self.malformed_ef_name: str = malformed_ef_name
        self.config: Dict[str, Any] = config
        self.message: str = f"This is a malformed embedding function for {malformed_ef_name} with config {config}. \
            Please pass the correct embedding function to get_collection or get_or_create_collection."

    def __call__(self, input: Embeddable) -> Embeddings:
        raise NotImplementedError(self.message)

    @staticmethod
    def name() -> str:
        return "malformed_ef"

    def get_config(self) -> Dict[str, Any]:
        return {
            "malformed_ef_name": self.malformed_ef_name,
            "config": self.config,
        }

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "MalformedEmbeddingFunction":
        return MalformedEmbeddingFunction(
            malformed_ef_name=config["malformed_ef_name"],
            config=config["config"],
        )
