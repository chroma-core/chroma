import pytest

from chromadb.auth.token_authn import TokenAuthenticationServerProvider
from chromadb.config import Settings


class _FakeSystem:
    """The provider only reads `settings` from the system during construction."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings


def _provider(credentials: str) -> TokenAuthenticationServerProvider:
    return TokenAuthenticationServerProvider(
        _FakeSystem(Settings(chroma_server_authn_credentials=credentials))
    )


@pytest.mark.parametrize(
    "credentials",
    [
        "foo: bar",  # a YAML mapping without `users`
        "users: not-a-list",
        "- just\n- a top-level list",
    ],
)
def test_credentials_without_a_users_list_name_the_problem(credentials: str) -> None:
    with pytest.raises(
        ValueError, match="top-level 'users' key holding a list of users"
    ):
        _provider(credentials)


def test_multi_user_credentials_still_load() -> None:
    provider = _provider(
        "users:\n"
        "- id: alice\n"
        "  tokens: [alice-token-1]\n"
        "- id: bob\n"
        "  tokens: [bob-token-2]"
    )

    assert set(provider._token_user_mapping) == {"alice-token-1", "bob-token-2"}


def test_a_single_credential_is_still_taken_as_an_anonymous_token() -> None:
    provider = _provider("single-token")

    assert list(provider._token_user_mapping) == ["single-token"]
