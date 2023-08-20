from typing import List

import hypothesis.strategies as st
import onnxruntime
import pytest
from hypothesis import given

from chromadb.utils.embedding_functions import ONNXMiniLM_L6_V2


@given(
    providers=st.lists(
        st.sampled_from(onnxruntime.get_all_providers()).filter(
            lambda x: x not in onnxruntime.get_available_providers()
        ),
        min_size=1,
    )
)
def test_unavailable_provider_multiple(providers: List[str]) -> None:
    print(providers)
    with pytest.raises(ValueError) as e:
        ef = ONNXMiniLM_L6_V2(preferred_providers=providers)
        ef(["test"])


@given(
    providers=st.lists(
        st.sampled_from(onnxruntime.get_all_providers()).filter(
            lambda x: x in onnxruntime.get_available_providers()
        ),
        min_size=1,
    )
)
def test_available_provider(providers: List[str]) -> None:
    ef = ONNXMiniLM_L6_V2(preferred_providers=providers)
    ef(["test"])


def test_warning_no_providers_supplied() -> None:
    ef = ONNXMiniLM_L6_V2()
    ef(["test"])
