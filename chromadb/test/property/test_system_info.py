import pytest
import requests

from typing import List, Dict
from chromadb.api.fastapi import FastAPI
from chromadb.api import API
from hypothesis import given, strategies as st

dicts_list = [
    {"python_version": ["python_version"]},
    {"os_info": ["os", "os_version", "os_release"]},
    {"memory_info": ["memory_info"]},
    {"cpu_info": ["cpu_info"]},
    {"disk_info": ["disk_info"]},
    {"network_info": ["network_info"]},
    {"env_vars": ["env_vars"]},
]


@given(
    flags_dict=st.iterables(
        elements=st.sampled_from(dicts_list), min_size=1, max_size=len(dicts_list)
    )
)
def test_dictionary_of_pairs(api: API, flags_dict: List[Dict[str, List[str]]]) -> None:
    if not isinstance(api, FastAPI):
        pytest.skip("Not a FastAPI instance")

    flags = ""
    check_response_flags = []
    for di in flags_dict:
        flags += f"{list(di.keys())[0]}=True&"
        check_response_flags.extend(list(di.values())[0])
    resp = requests.get(f"{api._api_url}/system-info?{flags[:-1]}")
    assert resp.status_code == 200
    _json = resp.json()
    _text = resp.text
    assert _json is not None

    for flag in check_response_flags:
        assert flag in _text
