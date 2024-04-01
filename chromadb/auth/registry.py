import importlib
import logging
import pkgutil
from typing import Union, Dict, Type, Callable  # noqa: F401

from chromadb.auth import (
    ServerAuthProvider,
    ServerAuthConfigurationProvider,
    ServerAuthCredentialsProvider,
    ClientAuthProvider,
    ServerAuthorizationProvider,
)
from chromadb.utils import get_class

logger = logging.getLogger(__name__)
ProviderTypes = Union[
    "ClientAuthProvider",
    "ServerAuthProvider",
    "ServerAuthConfigurationProvider",
    "ServerAuthCredentialsProvider",
    "ServerAuthorizationProvider",
]

_provider_registry = {
    "client_auth_providers": {},
    "client_auth_credentials_providers": {},
    "server_auth_providers": {},
    "server_auth_config_providers": {},
    "server_auth_credentials_providers": {},
    "server_authz_providers": {},
    "server_authz_config_providers": {},
}  # type: Dict[str, Dict[str, Type[ProviderTypes]]]


def register_classes_from_package(package_name: str) -> None:
    package = importlib.import_module(package_name)
    for _, module_name, _ in pkgutil.iter_modules(package.__path__):
        full_module_name = f"{package_name}.{module_name}"
        _ = importlib.import_module(full_module_name)


def register_provider(
    short_hand: str,
) -> Callable[[Type[ProviderTypes]], Type[ProviderTypes]]:
    def decorator(cls: Type[ProviderTypes]) -> Type[ProviderTypes]:
        logger.debug("Registering provider: %s", short_hand)
        global _provider_registry
        if issubclass(cls, ClientAuthProvider):
            _provider_registry["client_auth_providers"][short_hand] = cls
        elif issubclass(cls, ServerAuthProvider):
            _provider_registry["server_auth_providers"][short_hand] = cls
        elif issubclass(cls, ServerAuthConfigurationProvider):
            _provider_registry["server_auth_config_providers"][short_hand] = cls
        elif issubclass(cls, ServerAuthCredentialsProvider):
            _provider_registry["server_auth_credentials_providers"][short_hand] = cls
        elif issubclass(cls, ServerAuthorizationProvider):
            _provider_registry["server_authz_providers"][short_hand] = cls
        else:
            raise ValueError(
                "Only ClientAuthProvider, ClientAuthConfigurationProvider, "
                "ServerAuthProvider, "
                "ServerAuthConfigurationProvider, and ServerAuthCredentialsProvider, "
                "ServerAuthorizationProvider, "
                "can be registered."
            )
        return cls

    return decorator


def resolve_provider(
    class_or_name: str, cls: Type[ProviderTypes]
) -> Type[ProviderTypes]:
    register_classes_from_package("chromadb.auth")
    global _provider_registry
    if issubclass(cls, ClientAuthProvider):
        _key = "client_auth_providers"
    elif issubclass(cls, ServerAuthProvider):
        _key = "server_auth_providers"
    elif issubclass(cls, ServerAuthConfigurationProvider):
        _key = "server_auth_config_providers"
    elif issubclass(cls, ServerAuthCredentialsProvider):
        _key = "server_auth_credentials_providers"
    elif issubclass(cls, ServerAuthorizationProvider):
        _key = "server_authz_providers"
    else:
        raise ValueError(
            "Only ClientAuthProvider, "
            "ServerAuthProvider, "
            "ServerAuthConfigurationProvider, and ServerAuthCredentialsProvider, "
            "ServerAuthorizationProvider,"
            "can be registered."
        )
    if class_or_name in _provider_registry[_key]:
        return _provider_registry[_key][class_or_name]
    else:
        return get_class(class_or_name, cls)  # type: ignore
