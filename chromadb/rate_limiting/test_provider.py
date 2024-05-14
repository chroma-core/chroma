from typing import Optional

from overrides import overrides

from chromadb.config import System
from chromadb.rate_limiting import RateLimitingProvider


class RateLimitingTestProvider(RateLimitingProvider):
    def __init__(self, system: System):
        super().__init__(system)

    @overrides
    def is_allowed(self, key: str, quota: int, point: Optional[int] = 1) -> bool:
        pass
