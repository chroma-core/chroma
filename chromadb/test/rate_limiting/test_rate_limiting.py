from typing import Optional
from unittest.mock import patch

from chromadb.config import System, Settings, Component
from chromadb.quota import QuotaEnforcer, Resource
import pytest

from chromadb.rate_limiting import rate_limit


class RateLimitingGym(Component):
    def __init__(self, system: System):
        super().__init__(system)
        self.system = system

    @rate_limit(subject="bar", resource=Resource.DOCUMENT_SIZE)
    def bench(self, foo: str, bar: str) -> str:
        return foo

def mock_get_for_subject(self, resource: Resource, subject: Optional[str] = "", tier: Optional[str] = "") -> Optional[
    int]:
    """Mock function to simulate quota retrieval."""
    return 10

@pytest.fixture(scope="module")
def rate_limiting_gym() -> QuotaEnforcer:
    settings = Settings(
        chroma_quota_provider_impl="chromadb.quota.test_provider.QuotaProviderForTest",
        chroma_rate_limiting_provider_impl="chromadb.rate_limiting.test_provider.RateLimitingTestProvider"
    )
    system = System(settings)
    return RateLimitingGym(system)


@patch('chromadb.quota.test_provider.QuotaProviderForTest.get_for_subject', mock_get_for_subject)
@patch('chromadb.rate_limiting.test_provider.RateLimitingTestProvider.is_allowed', lambda self, key, quota, point=1: False)
def test_rate_limiting_should_raise(rate_limiting_gym: RateLimitingGym):
    with pytest.raises(Exception) as exc_info:
        rate_limiting_gym.bench("foo", "bar")
    assert Resource.DOCUMENT_SIZE.value in str(exc_info.value.resource)

@patch('chromadb.quota.test_provider.QuotaProviderForTest.get_for_subject', mock_get_for_subject)
@patch('chromadb.rate_limiting.test_provider.RateLimitingTestProvider.is_allowed', lambda self, key, quota, point=1: True)
def test_rate_limiting_should_not_raise(rate_limiting_gym: RateLimitingGym):
    assert rate_limiting_gym.bench(foo="foo", bar="bar") is "foo"

@patch('chromadb.quota.test_provider.QuotaProviderForTest.get_for_subject', mock_get_for_subject)
@patch('chromadb.rate_limiting.test_provider.RateLimitingTestProvider.is_allowed', lambda self, key, quota, point=1: True)
def test_rate_limiting_should_not_raise(rate_limiting_gym: RateLimitingGym):
    assert rate_limiting_gym.bench("foo", "bar") is "foo"