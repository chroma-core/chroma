from typing import List, Hashable

import hypothesis.strategies as st
import onnxruntime
import pytest
from hypothesis import given

from chromadb.utils.embedding_functions import ONNXMiniLM_L6_V2


def unique_by(x: Hashable) -> Hashable:
    return x


@given(
    providers=st.lists(
        st.sampled_from(onnxruntime.get_all_providers()).filter(
            lambda x: x not in onnxruntime.get_available_providers()
        ),
        unique_by=unique_by,
        min_size=1,
    )
)
def test_unavailable_provider_multiple(providers: List[str]) -> None:
    with pytest.raises(ValueError) as e:
        ef = ONNXMiniLM_L6_V2(preferred_providers=providers)
        ef(["test"])
    assert "Preferred providers must be subset of available providers" in str(e.value)


@given(
    providers=st.lists(
        st.sampled_from(onnxruntime.get_all_providers()).filter(
            lambda x: x in onnxruntime.get_available_providers()
        ),
        min_size=1,
        unique_by=unique_by,
    )
)
def test_available_provider(providers: List[str]) -> None:
    ef = ONNXMiniLM_L6_V2(preferred_providers=providers)
    ef(["test"])


def test_warning_no_providers_supplied() -> None:
    ef = ONNXMiniLM_L6_V2()
    ef(["test"])


@given(
    providers=st.lists(
        st.sampled_from(onnxruntime.get_all_providers()).filter(
            lambda x: x in onnxruntime.get_available_providers()
        ),
        min_size=1,
    ).filter(lambda x: len(x) > len(set(x)))
)
def test_provider_repeating(providers: List[str]) -> None:
    with pytest.raises(ValueError) as e:
        ef = ONNXMiniLM_L6_V2(preferred_providers=providers)
        ef(["test"])
    assert "Preferred providers must be unique" in str(e.value)
