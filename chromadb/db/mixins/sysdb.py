from typing import Optional, Sequence, Any, Tuple, cast, Dict, Union, Set
from uuid import UUID
from overrides import override
from pypika import Table, Column
from itertools import groupby

from chromadb.config import System
from chromadb.db.base import (
    Cursor,
    SqlDB,
    ParameterValue,
    get_sql,
    NotFoundError,
    UniqueConstraintError,
)
from chromadb.db.system import SysDB
from chromadb.types import (
    OptionalArgument,
    Segment,
    Metadata,
    Collection,
    SegmentScope,
    Unspecified,
    UpdateMetadata,
)


class SqlSysDB(SqlDB, SysDB):
    def __init__(self, system: System):
        super().__init__(system)

    @override
    def create_segment(self, segment: Segment) -> None:
        with self.tx() as cur:
            segments = Table("segments")
            insert_segment = (
                self.querybuilder()
                .into(segments)
                .columns(
                    segments.id,
                    segments.type,
                    segments.scope,
                    segments.topic,
                    segments.collection,
                )
                .insert(
                    ParameterValue(self.uuid_to_db(segment["id"])),
                    ParameterValue(segment["type"]),
                    ParameterValue(segment["scope"].value),
                    ParameterValue(segment["topic"]),
                    ParameterValue(self.uuid_to_db(segment["collection"])),
                )
            )
            sql, params = get_sql(insert_segment, self.parameter_format())
            try:
                cur.execute(sql, params)
            except self.unique_constraint_error() as e:
                raise UniqueConstraintError(
                    f"Segment {segment['id']} already exists"
                ) from e
            metadata_t = Table("segment_metadata")
            if segment["metadata"]:
                self._insert_metadata(
                    cur,
                    metadata_t,
                    metadata_t.segment_id,
                    segment["id"],
                    segment["metadata"],
                )

    @override
    def create_collection(self, collection: Collection) -> None:
        """Create a new collection"""
        with self.tx() as cur:
            collections = Table("collections")
            insert_collection = (
                self.querybuilder()
                .into(collections)
                .columns(
                    collections.id,
                    collections.topic,
                    collections.name,
                    collections.dimension,
                )
                .insert(
                    ParameterValue(self.uuid_to_db(collection["id"])),
                    ParameterValue(collection["topic"]),
                    ParameterValue(collection["name"]),
                    ParameterValue(collection["dimension"]),
                )
            )
            sql, params = get_sql(insert_collection, self.parameter_format())
            try:
                cur.execute(sql, params)
            except self.unique_constraint_error() as e:
                raise UniqueConstraintError(
                    f"Collection {collection['id']} already exists"
                ) from e
            metadata_t = Table("collection_metadata")
            if collection["metadata"]:
                self._insert_metadata(
                    cur,
                    metadata_t,
                    metadata_t.collection_id,
                    collection["id"],
                    collection["metadata"],
                )

    @override
    def get_segments(
        self,
        id: Optional[UUID] = None,
        type: Optional[str] = None,
        scope: Optional[SegmentScope] = None,
        topic: Optional[str] = None,
        collection: Optional[UUID] = None,
    ) -> Sequence[Segment]:
        segments_t = Table("segments")
        metadata_t = Table("segment_metadata")
        q = (
            self.querybuilder()
            .from_(segments_t)
            .select(
                segments_t.id,
                segments_t.type,
                segments_t.scope,
                segments_t.topic,
                segments_t.collection,
                metadata_t.key,
                metadata_t.str_value,
                metadata_t.int_value,
                metadata_t.float_value,
            )
            .left_join(metadata_t)
            .on(segments_t.id == metadata_t.segment_id)
            .orderby(segments_t.id)
        )
        if id:
            q = q.where(segments_t.id == ParameterValue(self.uuid_to_db(id)))
        if type:
            q = q.where(segments_t.type == ParameterValue(type))
        if scope:
            q = q.where(segments_t.scope == ParameterValue(scope.value))
        if topic:
            q = q.where(segments_t.topic == ParameterValue(topic))
        if collection:
            q = q.where(
                segments_t.collection == ParameterValue(self.uuid_to_db(collection))
            )

        with self.tx() as cur:
            sql, params = get_sql(q, self.parameter_format())
            rows = cur.execute(sql, params).fetchall()
            by_segment = groupby(rows, lambda r: cast(object, r[0]))
            segments = []
            for segment_id, segment_rows in by_segment:
                id = self.uuid_from_db(str(segment_id))
                rows = list(segment_rows)
                type = str(rows[0][1])
                scope = SegmentScope(str(rows[0][2]))
                topic = str(rows[0][3]) if rows[0][3] else None
                collection = self.uuid_from_db(rows[0][4]) if rows[0][4] else None
                metadata = self._metadata_from_rows(rows)
                segments.append(
                    Segment(
                        id=cast(UUID, id),
                        type=type,
                        scope=scope,
                        topic=topic,
                        collection=collection,
                        metadata=metadata,
                    )
                )

            return segments

    @override
    def get_collections(
        self,
        id: Optional[UUID] = None,
        topic: Optional[str] = None,
        name: Optional[str] = None,
    ) -> Sequence[Collection]:
        """Get collections by name, embedding function and/or metadata"""
        collections_t = Table("collections")
        metadata_t = Table("collection_metadata")
        q = (
            self.querybuilder()
            .from_(collections_t)
            .select(
                collections_t.id,
                collections_t.name,
                collections_t.topic,
                collections_t.dimension,
                metadata_t.key,
                metadata_t.str_value,
                metadata_t.int_value,
                metadata_t.float_value,
            )
            .left_join(metadata_t)
            .on(collections_t.id == metadata_t.collection_id)
            .orderby(collections_t.id)
        )
        if id:
            q = q.where(collections_t.id == ParameterValue(self.uuid_to_db(id)))
        if topic:
            q = q.where(collections_t.topic == ParameterValue(topic))
        if name:
            q = q.where(collections_t.name == ParameterValue(name))

        with self.tx() as cur:
            sql, params = get_sql(q, self.parameter_format())
            rows = cur.execute(sql, params).fetchall()
            by_collection = groupby(rows, lambda r: cast(object, r[0]))
            collections = []
            for collection_id, collection_rows in by_collection:
                id = self.uuid_from_db(str(collection_id))
                rows = list(collection_rows)
                name = str(rows[0][1])
                topic = str(rows[0][2])
                dimension = int(rows[0][3]) if rows[0][3] else None
                metadata = self._metadata_from_rows(rows)
                collections.append(
                    Collection(
                        id=cast(UUID, id),
                        topic=topic,
                        name=name,
                        metadata=metadata,
                        dimension=dimension,
                    )
                )

            return collections

    @override
    def delete_segment(self, id: UUID) -> None:
        """Delete a segment from the SysDB"""
        t = Table("segments")
        q = (
            self.querybuilder()
            .from_(t)
            .where(t.id == ParameterValue(self.uuid_to_db(id)))
            .delete()
        )
        with self.tx() as cur:
            # no need for explicit del from metadata table because of ON DELETE CASCADE
            sql, params = get_sql(q, self.parameter_format())
            sql = sql + " RETURNING id"
            result = cur.execute(sql, params).fetchone()
            if not result:
                raise NotFoundError(f"Segment {id} not found")

    @override
    def delete_collection(self, id: UUID) -> None:
        """Delete a topic and all associated segments from the SysDB"""
        t = Table("collections")
        q = (
            self.querybuilder()
            .from_(t)
            .where(t.id == ParameterValue(self.uuid_to_db(id)))
            .delete()
        )
        with self.tx() as cur:
            # no need for explicit del from metadata table because of ON DELETE CASCADE
            sql, params = get_sql(q, self.parameter_format())
            sql = sql + " RETURNING id"
            result = cur.execute(sql, params).fetchone()
            if not result:
                raise NotFoundError(f"Collection {id} not found")

    @override
    def update_segment(
        self,
        id: UUID,
        topic: OptionalArgument[Optional[str]] = Unspecified(),
        collection: OptionalArgument[Optional[UUID]] = Unspecified(),
        metadata: OptionalArgument[Optional[UpdateMetadata]] = Unspecified(),
    ) -> None:
        segments_t = Table("segments")
        metadata_t = Table("segment_metadata")

        q = (
            self.querybuilder()
            .update(segments_t)
            .where(segments_t.id == ParameterValue(self.uuid_to_db(id)))
        )

        if not topic == Unspecified():
            q = q.set(segments_t.topic, ParameterValue(topic))

        if not collection == Unspecified():
            collection = cast(Optional[UUID], collection)
            q = q.set(
                segments_t.collection, ParameterValue(self.uuid_to_db(collection))
            )

        with self.tx() as cur:
            sql, params = get_sql(q, self.parameter_format())
            if sql:  # pypika emits a blank string if nothing to do
                cur.execute(sql, params)

            if metadata is None:
                q = (
                    self.querybuilder()
                    .from_(metadata_t)
                    .where(metadata_t.segment_id == ParameterValue(self.uuid_to_db(id)))
                    .delete()
                )
                sql, params = get_sql(q, self.parameter_format())
                cur.execute(sql, params)
            elif metadata != Unspecified():
                metadata = cast(UpdateMetadata, metadata)
                metadata = cast(UpdateMetadata, metadata)
                self._insert_metadata(
                    cur,
                    metadata_t,
                    metadata_t.segment_id,
                    id,
                    metadata,
                    set(metadata.keys()),
                )

    @override
    def update_collection(
        self,
        id: UUID,
        topic: OptionalArgument[Optional[str]] = Unspecified(),
        name: OptionalArgument[str] = Unspecified(),
        dimension: OptionalArgument[Optional[int]] = Unspecified(),
        metadata: OptionalArgument[Optional[UpdateMetadata]] = Unspecified(),
    ) -> None:
        collections_t = Table("collections")
        metadata_t = Table("collection_metadata")

        q = (
            self.querybuilder()
            .update(collections_t)
            .where(collections_t.id == ParameterValue(self.uuid_to_db(id)))
        )

        if not topic == Unspecified():
            q = q.set(collections_t.topic, ParameterValue(topic))

        if not name == Unspecified():
            q = q.set(collections_t.name, ParameterValue(name))

        if not dimension == Unspecified():
            q = q.set(collections_t.dimension, ParameterValue(dimension))

        with self.tx() as cur:
            sql, params = get_sql(q, self.parameter_format())
            if sql:  # pypika emits a blank string if nothing to do
                cur.execute(sql, params)

            # TODO: Update to use better semantics where it's possible to update
            # individual keys without wiping all the existing metadata.

            # For now, follow current legancy semantics where metadata is fully reset
            if metadata != Unspecified():
                q = (
                    self.querybuilder()
                    .from_(metadata_t)
                    .where(
                        metadata_t.collection_id == ParameterValue(self.uuid_to_db(id))
                    )
                    .delete()
                )
                sql, params = get_sql(q, self.parameter_format())
                cur.execute(sql, params)
                if metadata is not None:
                    metadata = cast(UpdateMetadata, metadata)
                    self._insert_metadata(
                        cur,
                        metadata_t,
                        metadata_t.collection_id,
                        id,
                        metadata,
                        set(metadata.keys()),
                    )

    def _metadata_from_rows(
        self, rows: Sequence[Tuple[Any, ...]]
    ) -> Optional[Metadata]:
        """Given SQL rows, return a metadata map (assuming that the last four columns
        are the key, str_value, int_value & float_value)"""
        metadata: Dict[str, Union[str, int, float]] = {}
        for row in rows:
            key = str(row[-4])
            if row[-3] is not None:
                metadata[key] = str(row[-3])
            elif row[-2] is not None:
                metadata[key] = int(row[-2])
            elif row[-1] is not None:
                metadata[key] = float(row[-1])
        return metadata or None

    def _insert_metadata(
        self,
        cur: Cursor,
        table: Table,
        id_col: Column,
        id: UUID,
        metadata: UpdateMetadata,
        clear_keys: Optional[Set[str]] = None,
    ) -> None:
        # It would be cleaner to use something like ON CONFLICT UPDATE here But that is
        # very difficult to do in a portable way (e.g sqlite and postgres have
        # completely different sytnax)
        if clear_keys:
            q = (
                self.querybuilder()
                .from_(table)
                .where(id_col == ParameterValue(self.uuid_to_db(id)))
                .where(table.key.isin([ParameterValue(k) for k in clear_keys]))
                .delete()
            )
            sql, params = get_sql(q, self.parameter_format())
            cur.execute(sql, params)

        q = (
            self.querybuilder()
            .into(table)
            .columns(
                id_col, table.key, table.str_value, table.int_value, table.float_value
            )
        )
        sql_id = self.uuid_to_db(id)
        for k, v in metadata.items():
            if isinstance(v, str):
                q = q.insert(
                    ParameterValue(sql_id),
                    ParameterValue(k),
                    ParameterValue(v),
                    None,
                    None,
                )
            elif isinstance(v, int):
                q = q.insert(
                    ParameterValue(sql_id),
                    ParameterValue(k),
                    None,
                    ParameterValue(v),
                    None,
                )
            elif isinstance(v, float):
                q = q.insert(
                    ParameterValue(sql_id),
                    ParameterValue(k),
                    None,
                    None,
                    ParameterValue(v),
                )
            elif v is None:
                continue

        sql, params = get_sql(q, self.parameter_format())
        if sql:
            cur.execute(sql, params)
