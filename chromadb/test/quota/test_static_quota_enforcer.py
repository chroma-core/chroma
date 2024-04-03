import random
import string
from typing import Optional, List, Tuple, Any
from unittest.mock import patch

from chromadb.config import System, Settings
from chromadb.quota import QuotaEnforcer, Resource
import pytest


def generate_random_string(size: int) -> str:
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

def mock_get_for_subject(self, resource: Resource, subject: Optional[str] = "", tier: Optional[str] = "") -> Optional[
    int]:
    """Mock function to simulate quota retrieval."""
    return 10

def mock_get_for_subject_none_key_length(self, resource: Resource, subject: Optional[str] = "", tier: Optional[str] = "") -> Optional[
    int]:
    """Mock function to simulate quota retrieval."""
    if resource==Resource.METADATA_KEY_LENGTH:
        return None
    else:
        return 10

def mock_get_for_subject_none_value_length(self, resource: Resource, subject: Optional[str] = "", tier: Optional[str] = "") -> Optional[
    int]:
    """Mock function to simulate quota retrieval."""
    if resource==Resource.METADATA_VALUE_LENGTH:
        return None
    else:
        return 10

def mock_get_for_subject_none_key_value_length(self, resource: Resource, subject: Optional[str] = "", tier: Optional[str] = "") -> Optional[
    int]:
    """Mock function to simulate quota retrieval."""
    if resource==Resource.METADATA_KEY_LENGTH or resource==Resource.METADATA_VALUE_LENGTH:
        return None
    else:
        return 10

def run_static_checks(enforcer: QuotaEnforcer, test_cases: List[Tuple[Any, Optional[str]]], data_key: str):
    """Generalized function to run static checks on different types of data."""
    for test_case in test_cases:
        data, expected_error = test_case if len(test_case) == 2 else (test_case[0], None)
        args = {data_key: [data]}
        if expected_error:
            with pytest.raises(Exception) as exc_info:
                enforcer.static_check(**args)
            assert expected_error in str(exc_info.value.resource)
        else:
            enforcer.static_check(**args)



@pytest.fixture(scope="module")
def enforcer() -> QuotaEnforcer:
    settings = Settings(
        chroma_quota_provider_impl =  "chromadb.quota.test_provider.QuotaProviderForTest"
    )
    system = System(settings)
    return system.require(QuotaEnforcer)

@patch('chromadb.quota.test_provider.QuotaProviderForTest.get_for_subject', mock_get_for_subject)
def test_static_enforcer_metadata(enforcer):
    test_cases = [
        ({generate_random_string(20): generate_random_string(5)}, "METADATA_KEY_LENGTH"),
        ({generate_random_string(5): generate_random_string(5)}, None),
        ({generate_random_string(5): generate_random_string(20)}, "METADATA_VALUE_LENGTH"),
        ({generate_random_string(5): generate_random_string(5)}, None)
    ]
    run_static_checks(enforcer, test_cases, 'metadatas')

@patch('chromadb.quota.test_provider.QuotaProviderForTest.get_for_subject', mock_get_for_subject_none_key_length)
def test_static_enforcer_metadata_none_key_length(enforcer):
    test_cases = [
        ({generate_random_string(20): generate_random_string(5)}, None),
        ({generate_random_string(5): generate_random_string(5)}, None),
        ({generate_random_string(5): generate_random_string(20)}, "METADATA_VALUE_LENGTH"),
        ({generate_random_string(5): generate_random_string(5)}, None)
    ]
    run_static_checks(enforcer, test_cases, 'metadatas')

@patch('chromadb.quota.test_provider.QuotaProviderForTest.get_for_subject', mock_get_for_subject_none_value_length)
def test_static_enforcer_metadata_none_value_length(enforcer):
    test_cases = [
        ({generate_random_string(20): generate_random_string(5)}, "METADATA_KEY_LENGTH"),
        ({generate_random_string(5): generate_random_string(5)}, None),
        ({generate_random_string(5): generate_random_string(20)}, None),
        ({generate_random_string(5): generate_random_string(5)}, None)
    ]
    run_static_checks(enforcer, test_cases, 'metadatas')

@patch('chromadb.quota.test_provider.QuotaProviderForTest.get_for_subject', mock_get_for_subject_none_key_value_length)
def test_static_enforcer_metadata_none_key_value_length(enforcer):
    test_cases = [
        ({generate_random_string(20): generate_random_string(5)}, None),
        ({generate_random_string(5): generate_random_string(5)}, None),
        ({generate_random_string(5): generate_random_string(20)}, None),
        ({generate_random_string(5): generate_random_string(5)}, None)
    ]
    run_static_checks(enforcer, test_cases, 'metadatas')

@patch('chromadb.quota.test_provider.QuotaProviderForTest.get_for_subject', mock_get_for_subject)
def test_static_enforcer_documents(enforcer):
    test_cases = [
        (generate_random_string(20), "DOCUMENT_SIZE"),
        (generate_random_string(5), None)
    ]
    run_static_checks(enforcer, test_cases, 'documents')

@patch('chromadb.quota.test_provider.QuotaProviderForTest.get_for_subject', mock_get_for_subject)
def test_static_enforcer_embeddings(enforcer):
    test_cases = [
        (random.sample(range(1, 101), 100), "EMBEDDINGS_DIMENSION"),
        (random.sample(range(1, 101), 5), None)
    ]
    run_static_checks(enforcer, test_cases, 'embeddings')

# Should not raise an error if no quota provider is present
def test_enforcer_without_quota_provider():
    test_cases = [
        (random.sample(range(1, 101), 1), None),
        (random.sample(range(1, 101), 5), None)
    ]
    settings = Settings()
    system = System(settings)
    enforcer = system.require(QuotaEnforcer)
    run_static_checks(enforcer, test_cases, 'embeddings')
