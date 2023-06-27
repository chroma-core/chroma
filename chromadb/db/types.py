from typing import List, Dict, Tuple, Any
from uuid import UUID

# TODO: Add more sophisticated typing to our Postgres queries
# Here's our first attempt.
DBCollection = List[Tuple[UUID, str, Dict[str, Any]]]

DBCollections = List[DBCollection]
