import json

import requests

from typing import List, Dict
from chromadb.api.fastapi import FastAPI
from chromadb.api import API
from chromadb.api.types import SystemInfoFlags
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
def test_flags(api_obs: API, flags_dict: List[Dict[str, List[str]]]) -> None:
    _d_flags = {}
    check_response_flags = []
    for di in flags_dict:
        _d_flags[list(di.keys())[0]] = True
        check_response_flags.extend(list(di.values())[0])
    _sys_flags = SystemInfoFlags(**_d_flags)
    params = {field: getattr(_sys_flags, field) for field in _sys_flags._fields}
    if not isinstance(api_obs, FastAPI):
        _env = api_obs.env(system_info_flags=_sys_flags)
        assert _env is not None
        _text = json.dumps(_env)
        for flag in check_response_flags:
            assert flag in _text
    elif api_obs.get_settings().chroma_server_env_endpoint_enabled:
        resp = requests.get(f"{api_obs._api_url}/env", params=params)
        assert resp.status_code == 200
        _json = resp.json()
        _text = resp.text
        assert _json is not None
        for flag in check_response_flags:
            assert flag in _text
    else:
        resp = requests.get(f"{api_obs._api_url}/env", params=params)
        assert resp.status_code == 404
