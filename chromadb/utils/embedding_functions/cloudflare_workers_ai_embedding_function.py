import logging
from typing import Optional, Dict, cast

import httpx

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings

logger = logging.getLogger(__name__)


class CloudflareWorkersAIEmbeddingFunction(EmbeddingFunction[Documents]):
    # Follow API Quickstart for Cloudflare Workers AI
    # https://developers.cloudflare.com/workers-ai/
    # Information about the text embedding modules in Google Vertex AI
    # https://developers.cloudflare.com/workers-ai/models/embedding/
    def __init__(
        self,
        api_token: str,
        account_id: Optional[str] = None,
        model_name: Optional[str] = "@cf/baai/bge-base-en-v1.5",
        gateway_url: Optional[
            str
        ] = None,  # use Cloudflare AI Gateway instead of the usual endpoint
        # right now endpoint schema supports up to 100 docs at a time
        # https://developers.cloudflare.com/workers-ai/models/bge-small-en-v1.5/#api-schema (Input JSON Schema)
        max_batch_size: Optional[int] = 100,
        headers: Optional[Dict[str, str]] = None,
    ):
        if not gateway_url and not account_id:
            raise ValueError("Please provide either an account_id or a gateway_url.")
        if gateway_url and account_id:
            raise ValueError(
                "Please provide either an account_id or a gateway_url, not both."
            )
        if gateway_url is not None and not gateway_url.endswith("/"):
            gateway_url += "/"
        self._api_url = (
            f"{gateway_url}{model_name}"
            if gateway_url is not None
            else f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/{model_name}"
        )
        self._session = httpx.Client()
        self._session.headers.update(headers or {})
        self._session.headers.update({"Authorization": f"Bearer {api_token}"})
        self._max_batch_size = max_batch_size

    def __call__(self, texts: Documents) -> Embeddings:
        # Endpoint accepts up to 100 items at a time. We'll reject anything larger.
        # It would be up to the user to split the input into smaller batches.
        if self._max_batch_size and len(texts) > self._max_batch_size:
            raise ValueError(
                f"Batch too large {len(texts)} > {self._max_batch_size} (maximum batch size)."
            )

        print("URI", self._api_url)

        response = self._session.post(f"{self._api_url}", json={"text": texts})
        response.raise_for_status()
        _json = response.json()
        if "result" in _json and "data" in _json["result"]:
            return cast(Embeddings, _json["result"]["data"])
        else:
            raise ValueError(f"Error calling Cloudflare Workers AI: {response.text}")
