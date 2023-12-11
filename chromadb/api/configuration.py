from abc import abstractmethod
from typing import ClassVar, Dict, List, Optional, Protocol, Union
from multiprocessing import cpu_count

# TODO: move out of API


class StaticParameterError(Exception):
    """Represents an error that occurs when a static parameter is set."""

    pass


class ParameterValidator(Protocol):
    """Represents an abstract parameter validator."""

    @abstractmethod
    def __call__(self, value: Union[str, int, float, bool]) -> bool:
        """Returns whether the given value is valid."""
        raise NotImplementedError()


ParameterValue = Union[str, int, float, bool]


class ConfigurationDefinition:
    """Represents the definition of a configuration."""

    name: str
    validator: ParameterValidator
    is_static: bool
    default_value: ParameterValue

    def __init__(
        self,
        name: str,
        validator: ParameterValidator,
        is_static: bool,
        default_value: ParameterValue,
    ):
        self.name = name
        self.validator = validator
        self.is_static = is_static
        self.default_value = default_value


class ConfigurationParameter:
    """Represents a parameter of a configuration."""

    name: str
    value: ParameterValue

    def __init__(self, name: str, value: ParameterValue):
        self.name = name
        self.value = value

    def __repr__(self) -> str:
        return f"ConfigurationParameter({self.name}, {self.value})"


class Configuration:
    """Represents an abstract configuration."""

    # The internal data structure used to store the parameters
    # All expected parameters must be present with defaults or None values at initialization
    parameter_map: Dict[str, ConfigurationParameter]
    definitions: ClassVar[Dict[str, ConfigurationDefinition]]

    def __init__(self, parameters: Optional[List[ConfigurationParameter]] = None):
        """Initializes a new instance of the Configuration class. Respecting defaults and
        validators."""
        self.parameter_map = {}
        if parameters is not None:
            for parameter in parameters:
                if parameter.name not in self.definitions:
                    raise ValueError(f"Invalid parameter name: {parameter.name}")

                definition = self.definitions[parameter.name]
                if not isinstance(parameter.value, type(definition.default_value)):
                    raise ValueError(f"Invalid parameter value: {parameter.value}")

                validator = definition.validator
                if not validator(parameter.value):
                    raise ValueError(f"Invalid parameter value: {parameter.value}")
                self.parameter_map[parameter.name] = parameter
        # Apply the defaults for any missing parameters
        for name, definition in self.definitions.items():
            if name not in self.parameter_map:
                self.parameter_map[name] = ConfigurationParameter(
                    name=name, value=definition.default_value
                )

    def __repr__(self) -> str:
        return f"Configuration({self.parameter_map.values()})"

    def get_parameters(self) -> List[ConfigurationParameter]:
        """Returns the parameters of the configuration."""
        return list(self.parameter_map.values())

    def get_parameter(self, name: str) -> Optional[ConfigurationParameter]:
        """Returns the parameter with the given name or None if it does not exist."""
        return self.parameter_map.get(name, None)

    def set_parameter(self, name: str, value: Union[str, int, float, bool]) -> None:
        """Sets the parameter with the given name to the given value."""
        if name not in self.definitions:
            raise ValueError(f"Invalid parameter name: {name}")
        definition = self.definitions[name]
        parameter = self.parameter_map[name]
        if definition.is_static:
            raise StaticParameterError(f"Cannot set static parameter: {name}")
        if not definition.validator(value):
            raise ValueError(f"Invalid value for parameter {name}: {value}")
        parameter.value = value


class CollectionConfiguration(Configuration):
    """The configuration for a collection."""

    definitions = {
        "space": ConfigurationDefinition(
            name="space",
            validator=lambda value: isinstance(value, str)
            and value in ["l2", "ip", "cosine"],
            is_static=True,
            default_value="l2",
        ),
        "ef_construction": ConfigurationDefinition(
            name="ef_construction",
            validator=lambda value: isinstance(value, int) and value >= 1,
            is_static=True,
            default_value=100,
        ),
        "ef_search": ConfigurationDefinition(
            name="ef_search",
            validator=lambda value: isinstance(value, int) and value >= 1,
            is_static=False,
            default_value=10,
        ),
        "num_threads": ConfigurationDefinition(
            name="num_threads",
            validator=lambda value: isinstance(value, int) and value >= 1,
            is_static=False,
            default_value=cpu_count(),  # By default use all cores available
        ),
        "M": ConfigurationDefinition(
            name="M",
            validator=lambda value: isinstance(value, int) and value >= 1,
            is_static=True,
            default_value=16,
        ),
        "resize_factor": ConfigurationDefinition(
            name="resize_factor",
            validator=lambda value: isinstance(value, float) and value >= 1,
            is_static=True,
            default_value=1.2,
        ),
        "batch_size": ConfigurationDefinition(
            name="batch_size",
            validator=lambda value: isinstance(value, int) and value >= 1,
            is_static=True,
            default_value=1000,
        ),
        "sync_threashold": ConfigurationDefinition(
            name="sync_threashold",
            validator=lambda value: isinstance(value, int) and value >= 1,
            is_static=True,
            default_value=100,
        ),
    }
