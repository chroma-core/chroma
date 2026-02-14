from typing import TYPE_CHECKING, Optional, Dict, Any
from uuid import UUID
import json

if TYPE_CHECKING:
    from chromadb.api import ServerAPI  # noqa: F401


class AttachedFunction:
    """Represents a function attached to a collection."""

    def __init__(
        self,
        client: "ServerAPI",
        id: UUID,
        name: str,
        function_name: str,
        input_collection_id: UUID,
        output_collection: str,
        params: Optional[Dict[str, Any]],
        tenant: str,
        database: str,
    ):
        """Initialize an AttachedFunction.

        Args:
            client: The API client
            id: Unique identifier for this attached function
            name: Name of this attached function instance
            function_name: The function name (e.g., "record_counter", "statistics")
            input_collection_id: ID of the input collection
            output_collection: Name of the output collection
            params: Function-specific parameters
            tenant: The tenant name
            database: The database name
        """
        self._client = client
        self._id = id
        self._name = name
        self._function_name = function_name
        self._input_collection_id = input_collection_id
        self._output_collection = output_collection
        self._params = params
        self._tenant = tenant
        self._database = database

    @property
    def id(self) -> UUID:
        """The unique identifier of this attached function."""
        return self._id

    @property
    def name(self) -> str:
        """The name of this attached function instance."""
        return self._name

    @property
    def function_name(self) -> str:
        """The function name."""
        return self._function_name

    @property
    def input_collection_id(self) -> UUID:
        """The ID of the input collection."""
        return self._input_collection_id

    @property
    def output_collection(self) -> str:
        """The name of the output collection."""
        return self._output_collection

    @property
    def params(self) -> Optional[Dict[str, Any]]:
        """The function parameters."""
        return self._params

    @staticmethod
    def _normalize_params(params: Optional[Any]) -> Dict[str, Any]:
        """Normalize params to a consistent dict format.

        Handles None, empty strings, JSON strings, and dicts.
        """
        if params is None:
            return {}
        if isinstance(params, str):
            try:
                result = json.loads(params) if params else {}
                return result if isinstance(result, dict) else {}
            except json.JSONDecodeError:
                return {}
        if isinstance(params, dict):
            return params
        return {}

    def __repr__(self) -> str:
        return (
            f"AttachedFunction(id={self._id}, name='{self._name}', "
            f"function_name='{self._function_name}', "
            f"input_collection_id={self._input_collection_id}, "
            f"output_collection='{self._output_collection}')"
        )

    def __eq__(self, other: object) -> bool:
        """Compare two AttachedFunction objects for equality."""
        if not isinstance(other, AttachedFunction):
            return False

        # Normalize params: handle None, {}, and JSON strings
        self_params = self._normalize_params(self._params)
        other_params = self._normalize_params(other._params)

        return (
            self._id == other._id
            and self._name == other._name
            and self._function_name == other._function_name
            and self._input_collection_id == other._input_collection_id
            and self._output_collection == other._output_collection
            and self_params == other_params
            and self._tenant == other._tenant
            and self._database == other._database
        )

    def __hash__(self) -> int:
        """Return hash of the AttachedFunction."""
        # Normalize params using the same logic as __eq__
        normalized_params = self._normalize_params(self._params)
        params_tuple = (
            tuple(sorted(normalized_params.items())) if normalized_params else ()
        )

        return hash(
            (
                self._id,
                self._name,
                self._function_name,
                self._input_collection_id,
                self._output_collection,
                params_tuple,
                self._tenant,
                self._database,
            )
        )
