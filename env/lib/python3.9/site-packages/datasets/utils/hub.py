from functools import partial

from huggingface_hub import hf_hub_url
from huggingface_hub.utils import get_session, hf_raise_for_status


hf_dataset_url = partial(hf_hub_url, repo_type="dataset")


def check_auth(hf_api, repo_id, token=None):
    headers = hf_api._build_hf_headers(token=token)
    path = f"{hf_api.endpoint}/api/datasets/{repo_id}/auth-check"
    r = get_session().get(path, headers=headers)
    hf_raise_for_status(r)
