from typing import Optional

from overrides import overrides

from chromadb.quota import QuotaProvider, Resource


class QuotaProviderForTest(QuotaProvider):
    def __init__(self, system) -> None:
        super().__init__(system)

    @overrides
    def get_for_subject(
        self, resource: Resource, subject: Optional[str] = "", tier: Optional[str] = ""
    ) -> Optional[int]:
        pass
