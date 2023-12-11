from __future__ import annotations

from typing import Union, Optional, Dict, Any, List

from multimethod import overload
from pydantic import BaseModel

from chromadb.api.parameters import ParameterDict


class ParameterOverride(BaseModel):
    """
    ParameterOverride is a class that represents a parameter override for a Configurable object.
    """

    name: str
    value: Union[str, int, float, bool]
    persisted: bool

    def __init__(
        self, name: str, value: Union[str, int, float, bool], persisted: bool = False
    ) -> None:
        super().__init__(name=name, value=value, persisted=persisted)


class Configurable(object):
    """
    Object is a Configurable if it needs to set Parameter.
    """

    # keep tracks of a list of parameter overrides for this Configurable
    configurable: dict[str, ParameterOverride] = {}

    def get_parameter(self, name: str) -> Union[str, int, float, bool]:
        """
        Get the value of a parameter.
        """
        if name not in ParameterDict.parameter_dict.keys():
            raise ValueError(f'Parameter "{name}" does not exist.')
        if name in self.configurable:
            return self.configurable[name].value
        else:
            return ParameterDict.parameter_dict[name].default_value

    def set_parameter(self, parameter_override: ParameterOverride) -> None:
        """
        Set the value of a parameter.
        """
        self.__set_parameter(parameter_override.name, parameter_override)

    def __set_parameter(
        self,
        name: str,
        parameter_override: ParameterOverride,
        on_creation: bool = False,
    ) -> None:
        """
        Set the value of a parameter.
        """
        _validate_set_parameter(name, parameter_override.value, on_creation)
        self.configurable[name] = parameter_override

    def add_not_persisted_to_metadata(
        self, metadata: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        Add the parameter overrides to the metadata.
        """
        if metadata is None:
            if len(self.configurable) == 0:
                return None
            else:
                metadata = {}

        for name, parameter_override in self.configurable.items():
            if not parameter_override.persisted:
                metadata[name] = parameter_override.value
        return metadata

    def set_configurable(self, configurable: Configurable) -> None:
        """
        Set the parameter overrides of this Configurable to the parameter overrides of another Configurable.
        """
        for parameter_overrides in configurable.configurable.values():
            self.set_parameter(parameter_overrides)

    @overload
    def __init__(self, **kwargs) -> None:
        configurable: {str, ParameterOverride} = kwargs["configurable"]
        self.configurable = {}
        for parameter in configurable:
            self.__set_parameter(parameter.name, parameter, True)

    @overload
    def __init__(self, configurable: Optional[Configurable]) -> None:
        if configurable is not None:
            self.configurable = configurable.configurable

    @overload
    def __init__(self, parameters: List[ParameterOverride]) -> None:
        self.configurable = {}
        for parameter in parameters:
            self.__set_parameter(parameter.name, parameter, True)


def _get_configurable_from_metadata(
    metadata: Optional[Dict[str, Any]],
) -> Configurable:
    """
    Get the parameter overrides from the metadata.
    """
    parameter_overrides = []
    if metadata is None:
        return Configurable(parameter_overrides)
    for name, value in metadata.items():
        if name in ParameterDict.parameter_dict.keys():
            # right now all the callers are from persisted metadata, so we set persisted to True
            parameter_overrides.append(ParameterOverride(name, value, True))
    return Configurable(parameter_overrides)


def _validate_set_parameter(
    name: str, value: Union[str, int, float, None], on_creation: bool = False
) -> None:
    if name not in ParameterDict.parameter_dict.keys():
        raise ValueError(f'Parameter "{name}" does not exist.')
    if not on_creation and ParameterDict.parameter_dict[name].is_static:
        raise ValueError(f'Trying to set static parameter "{name}" after creation.')
    if not isinstance(value, bool) and not isinstance(value, (str, int, float)):
        raise ValueError(f'Parameter "{name}" is not a valid type.')
