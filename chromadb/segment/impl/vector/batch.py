from typing import Dict, List, cast
from collections import OrderedDict
from chromadb.types import LogRecord, Operation, SeqId, Vector


class Batch:
    """Used to model the set of changes as an atomic operation"""

    _ids_to_records: Dict[str, LogRecord]
    _deleted_ids: OrderedDict[str, None]
    _written_ids: OrderedDict[str, None]
    _upsert_add_ids: OrderedDict[str, None]  # IDs that are being added in an upsert
    add_count: int
    update_count: int
    max_seq_id: SeqId

    def __init__(self) -> None:
        self._ids_to_records = {}
        self._deleted_ids = OrderedDict()
        self._written_ids = OrderedDict()
        self._upsert_add_ids = OrderedDict()
        self.add_count = 0
        self.update_count = 0
        self.max_seq_id = 0

    def __len__(self) -> int:
        """Get the number of changes in this batch"""
        return len(self._written_ids) + len(self._deleted_ids)

    def get_deleted_ids(self) -> List[str]:
        """Get the list of deleted embeddings in this batch"""
        return list(self._deleted_ids.keys())

    def get_written_ids(self) -> List[str]:
        """Get the list of written embeddings in this batch"""
        return list(self._written_ids.keys())

    def get_written_vectors(self, ids: List[str]) -> List[Vector]:
        """Get the list of vectors to write in this batch"""
        return [
            cast(Vector, self._ids_to_records[id]["record"]["embedding"]) for id in ids
        ]

    def get_record(self, id: str) -> LogRecord:
        """Get the record for a given ID"""
        return self._ids_to_records[id]

    def is_deleted(self, id: str) -> bool:
        """Check if a given ID is deleted"""
        return id in self._deleted_ids

    @property
    def delete_count(self) -> int:
        return len(self._deleted_ids)

    def apply(self, record: LogRecord, exists_already: bool = False) -> None:
        """Apply an embedding record to this batch. Records passed to this method are assumed to be validated for correctness.
        For example, a delete or update presumes the ID exists in the index. An add presumes the ID does not exist in the index.
        The exists_already flag should be set to True if the ID does exist in the index, and False otherwise.
        """

        id = record["record"]["id"]
        if record["record"]["operation"] == Operation.DELETE:
            # If the ID was previously written, remove it from the written set
            # And update the add/update/delete counts
            if id in self._written_ids:
                del self._written_ids[id]
                if self._ids_to_records[id]["record"]["operation"] == Operation.ADD:
                    self.add_count -= 1
                elif (
                    self._ids_to_records[id]["record"]["operation"] == Operation.UPDATE
                ):
                    self.update_count -= 1
                    self._deleted_ids[id] = None
                elif (
                    self._ids_to_records[id]["record"]["operation"] == Operation.UPSERT
                ):
                    if id in self._upsert_add_ids:
                        self.add_count -= 1
                        del self._upsert_add_ids[id]
                    else:
                        self.update_count -= 1
                        self._deleted_ids[id] = None
            elif id not in self._deleted_ids:
                self._deleted_ids[id] = None

            # Remove the record from the batch
            if id in self._ids_to_records:
                del self._ids_to_records[id]

        else:
            self._ids_to_records[id] = record
            self._written_ids[id] = None

            # If the ID was previously deleted, remove it from the deleted set
            # And update the delete count
            if id in self._deleted_ids:
                del self._deleted_ids[id]

            # Update the add/update counts
            if record["record"]["operation"] == Operation.UPSERT:
                if not exists_already:
                    self.add_count += 1
                    self._upsert_add_ids[id] = None
                else:
                    self.update_count += 1
            elif record["record"]["operation"] == Operation.ADD:
                self.add_count += 1
            elif record["record"]["operation"] == Operation.UPDATE:
                self.update_count += 1

        self.max_seq_id = max(self.max_seq_id, record["log_offset"])
