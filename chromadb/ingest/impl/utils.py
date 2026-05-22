import re
from typing import Tuple
import warnings
from uuid import UUID

topic_regex = r"persistent:\/\/(?P<tenant>.+)\/(?P<namespace>.+)\/(?P<topic>.+)"


def parse_topic_name(topic_name: str) -> Tuple[str, str, str]:
    """Parse the topic name into the tenant, namespace and topic name"""
    match = re.match(topic_regex, topic_name)
    if not match:
        raise ValueError(f"Invalid topic name: {topic_name}")
    return match.group("tenant"), match.group("namespace"), match.group("topic")


def create_topic_name(tenant: str, namespace: str, collection_id: UUID) -> str:
    return f"persistent://{tenant}/{namespace}/{str(collection_id)}"


def trigger_vector_segments_max_seq_id_migration(
    db: object,
    segment_manager: object,
) -> None:
    """
    Trigger the migration of vector segments' max_seq_id from the pickled metadata file to SQLite.

    This hook is retained for compatibility with legacy callers, but migration now
    happens inside the Rust backend during normal initialization.
    """
    warnings.warn(
        "trigger_vector_segments_max_seq_id_migration is deprecated. "
        "This Python-specific migration path is no longer used with the Rust backend.",
        DeprecationWarning,
        stacklevel=2,
    )
