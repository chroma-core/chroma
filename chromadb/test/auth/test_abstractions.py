from typing import Optional

import pytest
from overrides import override

from chromadb.config import System, Settings, get_fqn
from chromadb.auth import register_configuration_provider, ServerAuthConfigurationProvider, \
    ServerAuthConfigurationProviderFactory


@register_configuration_provider('CONF_10', 'CONF_x', precedence=10)
class ConfigurationProvider3(ServerAuthConfigurationProvider):
    @override
    def get_configuration(self) -> Optional[str]:
        return "this is config ConfigurationProvider3"

    @classmethod
    @override
    def get_type(cls) -> str:
        return 'file'


def test_register_configuration() -> None:
    @register_configuration_provider('CONF_1', 'CONF_3')
    class ConfigurationProvider1(ServerAuthConfigurationProvider):
        @override
        def get_configuration(self) -> Optional[str]:
            return "this is config ConfigurationProvider1"

        @classmethod
        @override
        def get_type(cls) -> str:
            return 'env'

    @register_configuration_provider('CONF_2', 'CONF_4', precedence=2)
    class ConfigurationProvider2(ServerAuthConfigurationProvider):
        @override
        def get_configuration(self) -> Optional[str]:
            return "this is config ConfigurationProvider2"

        @classmethod
        @override
        def get_type(cls) -> str:
            return 'file'

    class UnregisteredConfigurationProvider1(ServerAuthConfigurationProvider):
        @override
        def get_configuration(self) -> Optional[str]:
            return "this is config UnregisteredConfigurationProvider1"

        @classmethod
        @override
        def get_type(cls) -> str:
            return 'file'

    class TestSettings(Settings):
        CONF_1: Optional[int] = None
        CONF_3: Optional[int] = None

    print(ServerAuthConfigurationProviderFactory.providers)
    assert 'CONF_1' in next(iter(ServerAuthConfigurationProviderFactory.providers['env'].keys()))
    assert next(iter(ServerAuthConfigurationProviderFactory.providers['env'].values()))[1] == 1
    # assert 'CONF_2' in next(iter(ServerAuthConfigurationProviderFactory.providers['file'].keys()))
    # assert next(iter(ServerAuthConfigurationProviderFactory.providers['file'].values()))[1] == 2
    _s = System(TestSettings(CONF_1=1, CONF_3=2))
    assert ServerAuthConfigurationProviderFactory.get_provider(_s) is not None
    assert isinstance(ServerAuthConfigurationProviderFactory.get_provider(_s), ConfigurationProvider1)
    assert isinstance(ServerAuthConfigurationProviderFactory.get_provider(_s, provider_class=ConfigurationProvider2),
                      ConfigurationProvider2)
    assert isinstance(
        ServerAuthConfigurationProviderFactory.get_provider(_s, provider_class=get_fqn(ConfigurationProvider3)),
        ConfigurationProvider3)
    with pytest.raises(ValueError):
        ServerAuthConfigurationProviderFactory.get_provider(_s, provider_class=UnregisteredConfigurationProvider1)
