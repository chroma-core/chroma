from typing import TYPE_CHECKING, Optional, Dict, Any
from uuid import UUID

if TYPE_CHECKING:
    from chromadb.api import ServerAPI  # noqa: F401


class AttachedFunction:
    """Represents a function attached to a collection."""

    def __init__(
        self,
        client: "ServerAPI",
        id: UUID,
        name: str,
        function_id: str,
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
            function_id: The function identifier (e.g., "record_counter")
            input_collection_id: ID of the input collection
            output_collection: Name of the output collection
            params: Function-specific parameters
            tenant: The tenant name
            database: The database name
        """
        self._client = client
        self._id = id
        self._name = name
        self._function_id = function_id
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
    def function_id(self) -> str:
        """The function identifier."""
        return self._function_id

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

    def detach(self, delete_output_collection: bool = False) -> bool:
        """Detach this function and prevent any further runs.

        Args:
            delete_output_collection: Whether to also delete the output collection. Defaults to False.

        Returns:
            bool: True if successful

        Example:
            >>> success = attached_fn.detach(delete_output_collection=True)
        """
        return self._client.detach_function(
            attached_function_id=self._id,
            delete_output=delete_output_collection,
            tenant=self._tenant,
            database=self._database,
        )

    def __repr__(self) -> str:
        return (
            f"AttachedFunction(id={self._id}, name='{self._name}', "
            f"function_id='{self._function_id}', "
            f"input_collection_id={self._input_collection_id}, "
            f"output_collection='{self._output_collection}')"
        )
