import json
from typing import Optional, Sequence, Any, Tuple, cast, Dict, Union, Set
from uuid import UUID
from overrides import override
from pypika import Table, Column
from itertools import groupby

from chromadb.api.configuration import (
    CollectionConfigurationInternal,
    ConfigurationParameter,
    HNSWConfigurationInternal,
    InvalidConfigurationError,
)
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, System
from chromadb.db.base import (
    Cursor,
    SqlDB,
    ParameterValue,
    get_sql,
    UniqueConstraintError,
)
from chromadb.db.system import SysDB
from chromadb.errors import NotFoundError
from chromadb.telemetry.opentelemetry import (
    add_attributes_to_current_span,
    OpenTelemetryClient,
    OpenTelemetryGranularity,
    trace_method,
)
from chromadb.ingest import Producer
from chromadb.types import (
    Database,
    OptionalArgument,
    Segment,
    Metadata,
    Collection,
    SegmentScope,
    Tenant,
    Unspecified,
    UpdateMetadata,
)


class SqlSysDB(SqlDB, SysDB):
    # Used only to delete log streams on collection deletion.
    # TODO: refactor to remove this dependency into a separate interface
    _producer: Producer

    def __init__(self, system: System):
        super().__init__(system)
        self._opentelemetry_client = system.require(OpenTelemetryClient)

    @trace_method("SqlSysDB.create_segment", OpenTelemetryGranularity.ALL)
    @override
    def start(self) -> None:
        super().start()
        self._producer = self._system.instance(Producer)

    @override
    def create_database(
        self, id: UUID, name: str, tenant: str = DEFAULT_TENANT
    ) -> None:
        with self.tx() as cur:
            # Get the tenant id for the tenant name and then insert the database with the id, name and tenant id
            databases = Table("databases")
            tenants = Table("tenants")
            insert_database = (
                self.querybuilder()
                .into(databases)
                .columns(databases.id, databases.name, databases.tenant_id)
                .insert(
                    ParameterValue(self.uuid_to_db(id)),
                    ParameterValue(name),
                    self.querybuilder()
                    .select(tenants.id)
                    .from_(tenants)
                    .where(tenants.id == ParameterValue(tenant)),
                )
            )
            sql, params = get_sql(insert_database, self.parameter_format())
            try:
                cur.execute(sql, params)
            except self.unique_constraint_error() as e:
                raise UniqueConstraintError(
                    f"Database {name} already exists for tenant {tenant}"
                ) from e

    @override
    def get_database(self, name: str, tenant: str = DEFAULT_TENANT) -> Database:
        with self.tx() as cur:
            databases = Table("databases")
            q = (
                self.querybuilder()
                .from_(databases)
                .select(databases.id, databases.name)
                .where(databases.name == ParameterValue(name))
                .where(databases.tenant_id == ParameterValue(tenant))
            )
            sql, params = get_sql(q, self.parameter_format())
            row = cur.execute(sql, params).fetchone()
            if not row:
                raise NotFoundError(
                    f"Database {name} not found for tenant {tenant}. Are you sure it exists?"
                )
            if row[0] is None:
                raise NotFoundError(
                    f"Database {name} not found for tenant {tenant}. Are you sure it exists?"
                )
            id: UUID = cast(UUID, self.uuid_from_db(row[0]))
            return Database(
                id=id,
                name=row[1],
                tenant=tenant,
            )

    @override
    def create_tenant(self, name: str) -> None:
        with self.tx() as cur:
            tenants = Table("tenants")
            insert_tenant = (
                self.querybuilder()
                .into(tenants)
                .columns(tenants.id)
                .insert(ParameterValue(name))
            )
            sql, params = get_sql(insert_tenant, self.parameter_format())
            try:
                cur.execute(sql, params)
            except self.unique_constraint_error() as e:
                raise UniqueConstraintError(f"Tenant {name} already exists") from e

    @override
    def get_tenant(self, name: str) -> Tenant:
        with self.tx() as cur:
            tenants = Table("tenants")
            q = (
                self.querybuilder()
                .from_(tenants)
                .select(tenants.id)
                .where(tenants.id == ParameterValue(name))
            )
            sql, params = get_sql(q, self.parameter_format())
            row = cur.execute(sql, params).fetchone()
            if not row:
                raise NotFoundError(f"Tenant {name} not found")
            return Tenant(name=name)

    @override
    def create_segment(self, segment: Segment) -> None:
        add_attributes_to_current_span(
            {
                "segment_id": str(segment["id"]),
                "segment_type": segment["type"],
                "segment_scope": segment["scope"].value,
                "collection": str(segment["collection"]),
            }
        )
        with self.tx() as cur:
            segments = Table("segments")
            insert_segment = (
                self.querybuilder()
                .into(segments)
                .columns(
                    segments.id,
                    segments.type,
                    segments.scope,
                    segments.collection,
                )
                .insert(
                    ParameterValue(self.uuid_to_db(segment["id"])),
                    ParameterValue(segment["type"]),
                    ParameterValue(segment["scope"].value),
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

    @trace_method("SqlSysDB.create_collection", OpenTelemetryGranularity.ALL)
    @override
    def create_collection(
        self,
        id: UUID,
        name: str,
        configuration: CollectionConfigurationInternal,
        metadata: Optional[Metadata] = None,
        dimension: Optional[int] = None,
        get_or_create: bool = False,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Tuple[Collection, bool]:
        if id is None and not get_or_create:
            raise ValueError("id must be specified if get_or_create is False")

        add_attributes_to_current_span(
            {
                "collection_id": str(id),
                "collection_name": name,
            }
        )

        existing = self.get_collections(name=name, tenant=tenant, database=database)
        if existing:
            if get_or_create:
                # We ignore configuration on the get path - configuration is immutable
                collection = existing[0]
                if metadata is not None and collection["metadata"] != metadata:
                    self.update_collection(
                        collection.id,
                        metadata=metadata,
                    )
                return (
                    self.get_collections(
                        id=collection["id"], tenant=tenant, database=database
                    )[0],
                    False,
                )
            else:
                raise UniqueConstraintError(f"Collection {name} already exists")

        collection = Collection(
            id=id,
            name=name,
            configuration=configuration,
            metadata=metadata,
            dimension=dimension,
            tenant=tenant,
            database=database,
            version=0,
        )

        with self.tx() as cur:
            collections = Table("collections")
            databases = Table("databases")

            insert_collection = (
                self.querybuilder()
                .into(collections)
                .columns(
                    collections.id,
                    collections.name,
                    collections.config_json_str,
                    collections.dimension,
                    collections.database_id,
                )
                .insert(
                    ParameterValue(self.uuid_to_db(collection["id"])),
                    ParameterValue(collection["name"]),
                    ParameterValue(configuration.to_json_str()),
                    ParameterValue(collection["dimension"]),
                    # Get the database id for the database with the given name and tenant
                    self.querybuilder()
                    .select(databases.id)
                    .from_(databases)
                    .where(databases.name == ParameterValue(database))
                    .where(databases.tenant_id == ParameterValue(tenant)),
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
                    collection.id,
                    collection["metadata"],
                )
        return collection, True

    @trace_method("SqlSysDB.get_segments", OpenTelemetryGranularity.ALL)
    @override
    def get_segments(
        self,
        collection: UUID,
        id: Optional[UUID] = None,
        type: Optional[str] = None,
        scope: Optional[SegmentScope] = None,
    ) -> Sequence[Segment]:
        add_attributes_to_current_span(
            {
                "segment_id": str(id),
                "segment_type": type if type else "",
                "segment_scope": scope.value if scope else "",
                "collection": str(collection),
            }
        )
        segments_t = Table("segments")
        metadata_t = Table("segment_metadata")
        q = (
            self.querybuilder()
            .from_(segments_t)
            .select(
                segments_t.id,
                segments_t.type,
                segments_t.scope,
                segments_t.collection,
                metadata_t.key,
                metadata_t.str_value,
                metadata_t.int_value,
                metadata_t.float_value,
                metadata_t.bool_value,
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
                collection = self.uuid_from_db(rows[0][3])  # type: ignore[assignment]
                metadata = self._metadata_from_rows(rows)
                segments.append(
                    Segment(
                        id=cast(UUID, id),
                        type=type,
                        scope=scope,
                        collection=collection,
                        metadata=metadata,
                    )
                )

            return segments

    @trace_method("SqlSysDB.get_collections", OpenTelemetryGranularity.ALL)
    @override
    def get_collections(
        self,
        id: Optional[UUID] = None,
        name: Optional[str] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Sequence[Collection]:
        """Get collections by name, embedding function and/or metadata"""

        if name is not None and (tenant is None or database is None):
            raise ValueError(
                "If name is specified, tenant and database must also be specified in order to uniquely identify the collection"
            )

        add_attributes_to_current_span(
            {
                "collection_id": str(id),
                "collection_name": name if name else "",
            }
        )

        collections_t = Table("collections")
        metadata_t = Table("collection_metadata")
        databases_t = Table("databases")
        q = (
            self.querybuilder()
            .from_(collections_t)
            .select(
                collections_t.id,
                collections_t.name,
                collections_t.config_json_str,
                collections_t.dimension,
                databases_t.name,
                databases_t.tenant_id,
                metadata_t.key,
                metadata_t.str_value,
                metadata_t.int_value,
                metadata_t.float_value,
                metadata_t.bool_value,
            )
            .left_join(metadata_t)
            .on(collections_t.id == metadata_t.collection_id)
            .left_join(databases_t)
            .on(collections_t.database_id == databases_t.id)
            .orderby(collections_t.id)
        )
        if id:
            q = q.where(collections_t.id == ParameterValue(self.uuid_to_db(id)))
        if name:
            q = q.where(collections_t.name == ParameterValue(name))

        # Only if we have a name, tenant and database do we need to filter databases
        # Given an id, we can uniquely identify the collection so we don't need to filter databases
        if id is None and tenant and database:
            databases_t = Table("databases")
            q = q.where(
                collections_t.database_id
                == self.querybuilder()
                .select(databases_t.id)
                .from_(databases_t)
                .where(databases_t.name == ParameterValue(database))
                .where(databases_t.tenant_id == ParameterValue(tenant))
            )
        # cant set limit and offset here because this is metadata and we havent reduced yet

        with self.tx() as cur:
            sql, params = get_sql(q, self.parameter_format())
            rows = cur.execute(sql, params).fetchall()
            by_collection = groupby(rows, lambda r: cast(object, r[0]))
            collections = []
            for collection_id, collection_rows in by_collection:
                id = self.uuid_from_db(str(collection_id))
                rows = list(collection_rows)
                name = str(rows[0][1])
                metadata = self._metadata_from_rows(rows)
                dimension = int(rows[0][3]) if rows[0][3] else None
                if rows[0][2] is not None:
                    configuration = self._load_config_from_json_str_and_migrate(
                        str(collection_id), rows[0][2]
                    )
                else:
                    # 07/2024: This is a legacy case where we don't have a collection
                    # configuration stored in the database. This non-destructively migrates
                    # the collection to have a configuration, and takes into account any
                    # HNSW params that might be in the existing metadata.
                    configuration = self._insert_config_from_legacy_params(
                        collection_id, metadata
                    )

                collections.append(
                    Collection(
                        id=cast(UUID, id),
                        name=name,
                        configuration=configuration,
                        metadata=metadata,
                        dimension=dimension,
                        tenant=str(rows[0][5]),
                        database=str(rows[0][4]),
                        version=0,
                    )
                )

            # apply limit and offset
            if limit is not None:
                if offset is None:
                    offset = 0
                collections = collections[offset : offset + limit]
            else:
                collections = collections[offset:]

            return collections

    @trace_method("SqlSysDB.delete_segment", OpenTelemetryGranularity.ALL)
    @override
    def delete_segment(self, collection: UUID, id: UUID) -> None:
        """Delete a segment from the SysDB"""
        add_attributes_to_current_span(
            {
                "segment_id": str(id),
            }
        )
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

    @trace_method("SqlSysDB.delete_collection", OpenTelemetryGranularity.ALL)
    @override
    def delete_collection(
        self,
        id: UUID,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> None:
        """Delete a collection and all associated segments from the SysDB. Deletes
        the log stream for this collection as well."""
        add_attributes_to_current_span(
            {
                "collection_id": str(id),
            }
        )
        t = Table("collections")
        databases_t = Table("databases")
        q = (
            self.querybuilder()
            .from_(t)
            .where(t.id == ParameterValue(self.uuid_to_db(id)))
            .where(
                t.database_id
                == self.querybuilder()
                .select(databases_t.id)
                .from_(databases_t)
                .where(databases_t.name == ParameterValue(database))
                .where(databases_t.tenant_id == ParameterValue(tenant))
            )
            .delete()
        )
        with self.tx() as cur:
            # no need for explicit del from metadata table because of ON DELETE CASCADE
            sql, params = get_sql(q, self.parameter_format())
            sql = sql + " RETURNING id"
            result = cur.execute(sql, params).fetchone()
            if not result:
                raise NotFoundError(f"Collection {id} not found")
        self._producer.delete_log(result[0])

    @trace_method("SqlSysDB.update_segment", OpenTelemetryGranularity.ALL)
    @override
    def update_segment(
        self,
        collection: UUID,
        id: UUID,
        metadata: OptionalArgument[Optional[UpdateMetadata]] = Unspecified(),
    ) -> None:
        add_attributes_to_current_span(
            {
                "segment_id": str(id),
                "collection": str(collection),
            }
        )
        segments_t = Table("segments")
        metadata_t = Table("segment_metadata")

        q = (
            self.querybuilder()
            .update(segments_t)
            .where(segments_t.id == ParameterValue(self.uuid_to_db(id)))
            .set(segments_t.collection, ParameterValue(self.uuid_to_db(collection)))
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

    @trace_method("SqlSysDB.update_collection", OpenTelemetryGranularity.ALL)
    @override
    def update_collection(
        self,
        id: UUID,
        name: OptionalArgument[str] = Unspecified(),
        dimension: OptionalArgument[Optional[int]] = Unspecified(),
        metadata: OptionalArgument[Optional[UpdateMetadata]] = Unspecified(),
    ) -> None:
        add_attributes_to_current_span(
            {
                "collection_id": str(id),
            }
        )
        collections_t = Table("collections")
        metadata_t = Table("collection_metadata")

        q = (
            self.querybuilder()
            .update(collections_t)
            .where(collections_t.id == ParameterValue(self.uuid_to_db(id)))
        )

        if not name == Unspecified():
            q = q.set(collections_t.name, ParameterValue(name))

        if not dimension == Unspecified():
            q = q.set(collections_t.dimension, ParameterValue(dimension))

        with self.tx() as cur:
            sql, params = get_sql(q, self.parameter_format())
            if sql:  # pypika emits a blank string if nothing to do
                sql = sql + " RETURNING id"
                result = cur.execute(sql, params)
                if not result.fetchone():
                    raise NotFoundError(f"Collection {id} not found")

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

    @trace_method("SqlSysDB._metadata_from_rows", OpenTelemetryGranularity.ALL)
    def _metadata_from_rows(
        self, rows: Sequence[Tuple[Any, ...]]
    ) -> Optional[Metadata]:
        """Given SQL rows, return a metadata map (assuming that the last four columns
        are the key, str_value, int_value & float_value)"""
        add_attributes_to_current_span(
            {
                "num_rows": len(rows),
            }
        )
        metadata: Dict[str, Union[str, int, float, bool]] = {}
        for row in rows:
            key = str(row[-5])
            if row[-4] is not None:
                metadata[key] = str(row[-4])
            elif row[-3] is not None:
                metadata[key] = int(row[-3])
            elif row[-2] is not None:
                metadata[key] = float(row[-2])
            elif row[-1] is not None:
                metadata[key] = bool(row[-1])
        return metadata or None

    @trace_method("SqlSysDB._insert_metadata", OpenTelemetryGranularity.ALL)
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
        add_attributes_to_current_span(
            {
                "num_keys": len(metadata),
            }
        )
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
                id_col,
                table.key,
                table.str_value,
                table.int_value,
                table.float_value,
                table.bool_value,
            )
        )
        sql_id = self.uuid_to_db(id)
        for k, v in metadata.items():
            # Note: The order is important here because isinstance(v, bool)
            # and isinstance(v, int) both are true for v of bool type.
            if isinstance(v, bool):
                q = q.insert(
                    ParameterValue(sql_id),
                    ParameterValue(k),
                    None,
                    None,
                    None,
                    ParameterValue(int(v)),
                )
            elif isinstance(v, str):
                q = q.insert(
                    ParameterValue(sql_id),
                    ParameterValue(k),
                    ParameterValue(v),
                    None,
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
                    None,
                )
            elif isinstance(v, float):
                q = q.insert(
                    ParameterValue(sql_id),
                    ParameterValue(k),
                    None,
                    None,
                    ParameterValue(v),
                    None,
                )
            elif v is None:
                continue

        sql, params = get_sql(q, self.parameter_format())
        if sql:
            cur.execute(sql, params)

    def _load_config_from_json_str_and_migrate(
        self, collection_id: str, json_str: str
    ) -> CollectionConfigurationInternal:
        try:
            config_json = json.loads(json_str)
        except json.JSONDecodeError:
            raise ValueError(
                f"Unable to decode configuration from JSON string: {json_str}"
            )

        try:
            return CollectionConfigurationInternal.from_json_str(json_str)
        except InvalidConfigurationError as error:
            # 07/17/2024: the initial migration from the legacy metadata-based config to the new sysdb-based config had a bug where the batch_size and sync_threshold were swapped. Along with this migration, a validator was added to HNSWConfigurationInternal to ensure that batch_size <= sync_threshold.
            hnsw_configuration = config_json.get("hnsw_configuration")
            if hnsw_configuration:
                batch_size = hnsw_configuration.get("batch_size")
                sync_threshold = hnsw_configuration.get("sync_threshold")

                if batch_size and sync_threshold and batch_size > sync_threshold:
                    # Allow new defaults to be set
                    hnsw_configuration = {
                        k: v
                        for k, v in hnsw_configuration.items()
                        if k not in ["batch_size", "sync_threshold"]
                    }
                    config_json.update({"hnsw_configuration": hnsw_configuration})

                    configuration = CollectionConfigurationInternal.from_json(
                        config_json
                    )

                    collections_t = Table("collections")
                    q = (
                        self.querybuilder()
                        .update(collections_t)
                        .set(
                            collections_t.config_json_str,
                            ParameterValue(configuration.to_json_str()),
                        )
                        .where(collections_t.id == ParameterValue(collection_id))
                    )
                    sql, params = get_sql(q, self.parameter_format())
                    with self.tx() as cur:
                        cur.execute(sql, params)

                    return configuration

            raise error

    def _insert_config_from_legacy_params(
        self, collection_id: Any, metadata: Optional[Metadata]
    ) -> CollectionConfigurationInternal:
        """Insert the configuration from legacy metadata params into the collections table, and return the configuration object."""

        # This is a legacy case where we don't have configuration stored in the database
        # This is non-destructive, we don't delete or overwrite any keys in the metadata
        from chromadb.segment.impl.vector.hnsw_params import PersistentHnswParams

        collections_t = Table("collections")

        # Get any existing HNSW params from the metadata (works regardless whether metadata has persistent params)
        hnsw_metadata_params = PersistentHnswParams.extract(metadata or {})

        hnsw_configuration = HNSWConfigurationInternal.from_legacy_params(
            hnsw_metadata_params  # type: ignore[arg-type]
        )
        configuration = CollectionConfigurationInternal(
            parameters=[
                ConfigurationParameter(
                    name="hnsw_configuration", value=hnsw_configuration
                )
            ]
        )
        # Write the configuration into the database
        configuration_json_str = configuration.to_json_str()
        q = (
            self.querybuilder()
            .update(collections_t)
            .set(
                collections_t.config_json_str,
                ParameterValue(configuration_json_str),
            )
            .where(collections_t.id == ParameterValue(collection_id))
        )
        sql, params = get_sql(q, self.parameter_format())
        with self.tx() as cur:
            cur.execute(sql, params)
        return configuration
