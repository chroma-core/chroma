import pytest
from chromadb.api.configuration import (
    ConfigurationInternal,
    ConfigurationDefinition,
    StaticParameterError,
    ConfigurationParameter,
)


class TestConfiguration(ConfigurationInternal):
    definitions = {
        "static_str_value": ConfigurationDefinition(
            name="static_str_value",
            validator=lambda value: isinstance(value, str),
            is_static=True,
            default_value="default",
        ),
        "int_value": ConfigurationDefinition(
            name="int_value",
            validator=lambda value: isinstance(value, int),
            is_static=False,
            default_value=0,
        ),
    }


def test_default_values() -> None:
    default_test_configuration = TestConfiguration()
    assert default_test_configuration.get_parameter("static_str_value") is not None
    assert (
        default_test_configuration.get_parameter("static_str_value").value
        == TestConfiguration.definitions["static_str_value"].default_value
    )
    assert default_test_configuration.get_parameter("static_str_value") is not None
    assert (
        default_test_configuration.get_parameter("int_value").value
        == TestConfiguration.definitions["int_value"].default_value
    )


def test_set_values() -> None:
    test_configuration = TestConfiguration()

    with pytest.raises(StaticParameterError):
        test_configuration.set_parameter("static_str_value", "new_value")
    test_configuration.set_parameter("int_value", 1)
    assert test_configuration.get_parameter("int_value").value == 1


def test_get_invalid_parameter() -> None:
    test_configuration = TestConfiguration()
    with pytest.raises(ValueError):
        test_configuration.get_parameter("invalid_name")


def test_validation() -> None:
    valid_parameters = [
        ConfigurationParameter(name="static_str_value", value="valid_value"),
        ConfigurationParameter(name="int_value", value=1),
    ]
    valid_test_configuration = TestConfiguration(parameters=valid_parameters)
    assert (
        valid_test_configuration.get_parameter("static_str_value").value
        == "valid_value"
    )
    assert valid_test_configuration.get_parameter("int_value").value == 1

    invalid_parameter_values = [
        ConfigurationParameter(name="static_str_value", value=1.0)
    ]
    with pytest.raises(ValueError):
        TestConfiguration(parameters=invalid_parameter_values)

    invalid_parameter_names = [
        ConfigurationParameter(name="invalid_name", value="some_value")
    ]
    with pytest.raises(ValueError):
        TestConfiguration(parameters=invalid_parameter_names)
