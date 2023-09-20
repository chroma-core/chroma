import importlib
import logging
import pkgutil
from typing import Union, Dict, Type, Callable  # noqa: F401

from chromadb.auth import (
    ClientAuthConfigurationProvider,
    ClientAuthCredentialsProvider,
    ClientAuthProtocolAdapter,
    ServerAuthProvider,
    ServerAuthConfigurationProvider,
    ServerAuthCredentialsProvider,
    ClientAuthProvider,
)
from chromadb.utils import get_class

logger = logging.getLogger(__name__)
ProviderTypes = Union[
    "ClientAuthProvider",
    "ClientAuthConfigurationProvider",
    "ClientAuthCredentialsProvider",
    "ServerAuthProvider",
    "ServerAuthConfigurationProvider",
    "ServerAuthCredentialsProvider",
    "ClientAuthProtocolAdapter",
]

_provider_registry = {
    "client_auth_providers": {},
    "client_auth_config_providers": {},
    "client_auth_credentials_providers": {},
    "client_auth_protocol_adapters": {},
    "server_auth_providers": {},
    "server_auth_config_providers": {},
    "server_auth_credentials_providers": {},
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
        elif issubclass(cls, ClientAuthConfigurationProvider):
            _provider_registry["client_auth_config_providers"][short_hand] = cls
        elif issubclass(cls, ClientAuthCredentialsProvider):
            _provider_registry["client_auth_credentials_providers"][short_hand] = cls
        elif issubclass(cls, ClientAuthProtocolAdapter):
            _provider_registry["client_auth_protocol_adapters"][short_hand] = cls
        elif issubclass(cls, ServerAuthProvider):
            _provider_registry["server_auth_providers"][short_hand] = cls
        elif issubclass(cls, ServerAuthConfigurationProvider):
            _provider_registry["server_auth_config_providers"][short_hand] = cls
        elif issubclass(cls, ServerAuthCredentialsProvider):
            _provider_registry["server_auth_credentials_providers"][short_hand] = cls
        else:
            raise ValueError(
                "Only ClientAuthProvider, ClientAuthConfigurationProvider, "
                "ClientAuthCredentialsProvider, ServerAuthProvider, "
                "ServerAuthConfigurationProvider, and ServerAuthCredentialsProvider, ClientAuthProtocolAdapter "
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
    elif issubclass(cls, ClientAuthConfigurationProvider):
        _key = "client_auth_config_providers"
    elif issubclass(cls, ClientAuthCredentialsProvider):
        _key = "client_auth_credentials_providers"
    elif issubclass(cls, ClientAuthProtocolAdapter):
        _key = "client_auth_protocol_adapters"
    elif issubclass(cls, ServerAuthProvider):
        _key = "server_auth_providers"
    elif issubclass(cls, ServerAuthConfigurationProvider):
        _key = "server_auth_config_providers"
    elif issubclass(cls, ServerAuthCredentialsProvider):
        _key = "server_auth_credentials_providers"
    else:
        raise ValueError(
            "Only ClientAuthProvider, ClientAuthConfigurationProvider, "
            "ClientAuthCredentialsProvider, ServerAuthProvider, "
            "ServerAuthConfigurationProvider, and ServerAuthCredentialsProvider,ClientAuthProtocolAdapter "
            "can be registered."
        )
    if class_or_name in _provider_registry[_key]:
        return _provider_registry[_key][class_or_name]
    else:
        return get_class(class_or_name, cls)  # type: ignore
