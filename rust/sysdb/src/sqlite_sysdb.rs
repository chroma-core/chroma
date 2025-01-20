use crate::sqlite::{MigrationHash, MigrationMode, SqliteDb};
use chroma_types::{
    Collection, CollectionUuid, Database, Metadata, MetadataValue, Segment, Tenant,
};
use sqlx::sqlite::SqliteRow;
use sqlx::{error::ErrorKind, Executor};
use sqlx::{Column, Row};
use std::collections::HashMap;
use std::path::PathBuf;

//////////////////////// SqliteSysDb ////////////////////////

const DEFAULT_TENANT: &str = "default_tenant";
const DEFAULT_DATABASE: &str = "default_database";

#[derive(Debug)]
pub struct SqliteSysDb {
    db: SqliteDb,
}

impl SqliteSysDb {
    fn new(db: SqliteDb) -> Self {
        Self { db }
    }

    // TODO: THIS IS JUST FOR TESTING
    // REMOVE IN FAVOR OF CONFIG PARSING
    pub async fn new_hack_test(path: &str) -> Self {
        let sqlite_db_config = crate::sqlite::SqliteDBConfig {
            url: path.to_string(),
            migrations_root_dir: PathBuf::from(
                "/Users/hammad/Documents/chroma/chromadb/migrations",
            ),
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Apply,
        };
        let db = SqliteDb::try_from_config(&sqlite_db_config).await.unwrap();
        Self::new(db)
    }

    ////////////////////////// Database Methods ////////////////////////

    // TODO: real error
    // TODO: shouldn't need to be pub since enum should wrap
    // TODO: maybe this should not be optional?
    pub async fn create_database(
        &self,
        id: uuid::Uuid,
        name: &str,
        tenant: Option<&str>,
    ) -> Result<(), String> {
        let query = "INSERT INTO databases (id, name, tenant_id) VALUES ($1, $2, $3)";

        let conn = self.db.get_conn();
        let query = sqlx::query(query)
            .bind(id.to_string())
            .bind(name)
            .bind(tenant.unwrap_or(DEFAULT_TENANT));

        // TODO: error
        let mut tx = conn.begin().await.map_err(|e| e.to_string())?;

        let res = tx.execute(query).await;
        match res {
            Ok(_) => {}
            Err(e) => match e {
                sqlx::Error::Database(ref db_err) => {
                    if db_err.kind() == ErrorKind::UniqueViolation {
                        // TODO: real error
                        return Err(format!(
                            "Database {} already exists for tenant {}",
                            name,
                            tenant.unwrap_or("default_tenant")
                        ));
                    }
                }
                _ => {
                    return Err(e.to_string());
                }
            },
        }
        tx.commit().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn get_database(&self, name: &str, tenant: Option<&str>) -> Result<Database, String> {
        let query_str =
            "SELECT id, name, tenant_id FROM databases WHERE name = $1 AND tenant_id = $2";
        let conn = self.db.get_conn();
        let query = sqlx::query(query_str)
            .bind(name)
            .bind(tenant.unwrap_or(DEFAULT_TENANT));
        let row = conn.fetch_one(query).await.map_err(|e| e.to_string())?;
        // TODO: error
        Ok(Database {
            id: uuid::Uuid::parse_str(row.get("id")).map_err(|e| e.to_string())?,
            name: row.get("name"),
            tenant: row.get("tenant_id"),
        })
    }

    ////////////////////////// Tenant Methods ////////////////////////

    pub async fn create_tenant(&self, name: &str) -> Result<Tenant, String> {
        let query = "INSERT INTO tenants (id) VALUES ($1)";

        let conn = self.db.get_conn();
        let query = sqlx::query(query).bind(name);
        let mut tx = conn.begin().await.map_err(|e| e.to_string())?;
        let res = tx.execute(query).await;
        match res {
            Ok(_) => {}
            Err(e) => match e {
                sqlx::Error::Database(ref db_err) => {
                    if db_err.kind() == ErrorKind::UniqueViolation {
                        // TODO: real error
                        return Err(format!("Tenant {} already exists", name));
                    }
                }
                _ => {
                    return Err(e.to_string());
                }
            },
        }
        tx.commit().await.map_err(|e| e.to_string())?;
        Ok(Tenant {
            id: name.to_string(),
            // last_compaction_time is a distributed system concept only
            last_compaction_time: 0,
        })
    }

    pub async fn get_tenant(&self, name: &str) -> Result<Tenant, String> {
        let query_str = "SELECT id FROM tenants WHERE id = $1";
        let conn = self.db.get_conn();
        let query = sqlx::query(query_str).bind(name);
        let row = conn.fetch_one(query).await.map_err(|e| e.to_string())?;
        Ok(Tenant {
            id: row.get("id"),
            last_compaction_time: 0,
        })
    }

    ////////////////////////// Collection Methods ////////////////////////

    #[allow(clippy::too_many_arguments)]
    pub async fn create_collection(
        &self,
        // TODO: unify all id types on wrappers
        id: Option<CollectionUuid>,
        name: &str,
        segments: Vec<Segment>,
        metadata: Option<&Metadata>,
        dimension: Option<i32>,
        get_or_create: bool,
        tenant: Option<&str>,
        database: Option<&str>,
    ) -> Result<(Collection, bool), String> {
        let mut tx = self
            .db
            .get_conn()
            .begin()
            .await
            .map_err(|e| e.to_string())?;

        match (id, get_or_create) {
            (None, false) => {
                // TODO: real error
                Err("id must be provided if get_or_create is false".to_string())
            }
            (Some(id), false) => {
                let database_id = self
                    .get_database(database.unwrap_or(DEFAULT_DATABASE), tenant)
                    .await?
                    .id;
                // get_or_create is false, only create the collection if it does not exist
                self.insert_collection(&mut tx, id, name, "{}", dimension, database_id)
                    .await?;
                if let Some(metadata) = metadata {
                    self.insert_metadata_for_collection_id(&mut tx, &id.to_string(), &metadata)
                        .await?;
                }
                self.insert_segments_for_collection_id(&mut tx, &id.to_string(), &segments)
                    .await?;
                tx.commit().await.map_err(|e| e.to_string())?;

                // TODO:, this should probably reuse the same txn for consistency
                Ok((self.get_collection_by_id(id).await?, true))
            }
            (Some(_), true) => {
                unimplemented!();
            }
            (None, true) => {
                unimplemented!();
            }
        }

        // Check if collection exists
    }

    async fn insert_collection(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: CollectionUuid,
        name: &str,
        //TODO: support config_json_str
        _config_json_str: &str,
        dimension: Option<i32>,
        // TODO: uuid type
        database_id: uuid::Uuid,
    ) -> Result<(), String> {
        let query = "INSERT INTO collections (id, name, config_json_str, dimension, database_id) VALUES ($1, $2, $3, $4, $5)";
        let query = sqlx::query(query)
            .bind(id.to_string())
            .bind(name)
            .bind::<Option<String>>(None)
            .bind(dimension)
            .bind(database_id.to_string());

        // TODO: unique constraint violation error
        tx.execute(query).await.map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn insert_segments_for_collection_id(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        collection_id: &str,
        segments: &[Segment],
    ) -> Result<(), String> {
        let query = "INSERT INTO segments (id, type, scope, collection) VALUES ($1, $2, $3, $4)";
        for segment in segments {
            let query = sqlx::query(query)
                .bind(segment.id.to_string())
                .bind(segment.r#type.to_string())
                .bind(segment.scope.to_string())
                .bind(collection_id);
            tx.execute(query).await.map_err(|e| e.to_string())?;
            if let Some(segment_metadata) = &segment.metadata {
                self.insert_metadata_for_segment_id(tx, &segment.id.to_string(), segment_metadata)
                    .await?;
            }
        }
        Ok(())
    }

    async fn insert_metadata_for_collection_id(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        collection_id: &str,
        metadata: &Metadata,
    ) -> Result<(), String> {
        let query = "INSERT INTO collection_metadata (collection_id, key, str_value, int_value, float_value, bool_value) VALUES ($1, $2, $3, $4, $5, $6)";
        self.insert_metadata(tx, query, collection_id, metadata)
            .await?;
        Ok(())
    }

    async fn insert_metadata_for_segment_id(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        segment_id: &str,
        metadata: &Metadata,
    ) -> Result<(), String> {
        let query = "INSERT INTO segment_metadata (segment_id, key, str_value, int_value, float_value, bool_value) VALUES ($1, $2, $3, $4, $5, $6)";
        self.insert_metadata(tx, query, segment_id, metadata)
            .await?;
        Ok(())
    }

    async fn insert_metadata(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        query: &str,
        id: &str,
        metadata: &Metadata,
    ) -> Result<(), String> {
        for (key, value) in metadata.iter() {
            let (str_value, int_value, float_value, bool_value) = match value {
                MetadataValue::Str(str_value) => (Some(str_value), None, None, None),
                MetadataValue::Int(int_value) => (None, Some(*int_value), None, None),
                MetadataValue::Float(float_value) => (None, None, Some(*float_value), None),
                MetadataValue::Bool(bool_value) => (None, None, None, Some(*bool_value)),
            };
            let query = sqlx::query(query)
                .bind(id)
                .bind(key)
                .bind(str_value)
                .bind(int_value)
                .bind(float_value)
                .bind(bool_value);
            tx.execute(query).await.map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    // TODO: error
    async fn get_collection_by_id(&self, id: CollectionUuid) -> Result<Collection, String> {
        let collection_query_str = "SELECT collections.id, collections.name, collections.config_json_str, collections.dimension, databases.name as database_name, databases.tenant_id FROM collections LEFT JOIN databases ON collections.database_id = databases.id WHERE collections.id = $1";
        let query = sqlx::query(collection_query_str).bind(id.to_string());

        let mut tx = self
            .db
            .get_conn()
            .begin()
            .await
            .map_err(|e| e.to_string())?;
        let collection_row = tx.fetch_one(query).await.map_err(|e| e.to_string())?;

        let collection_id: String = collection_row.get("id");
        let metadata_query_str = "SELECT collection_metadata.key, collection_metadata.str_value, collection_metadata.int_value, collection_metadata.float_value, collection_metadata.bool_value FROM collection_metadata WHERE collection_metadata.collection_id = $1";
        let query = sqlx::query(metadata_query_str).bind(collection_id);
        let metadata_rows = tx.fetch_all(query).await.map_err(|e| e.to_string())?;

        tx.commit().await.map_err(|e| e.to_string())?;

        let mut metadata = HashMap::new();
        // TODO: Use TryFrom traits instead of verbose conversion
        for row in metadata_rows {
            let key: String = row.get("key");
            let str_value: Option<String> = row.get("str_value");
            let int_value: Option<i64> = row.get("int_value");
            let float_value: Option<f64> = row.get("float_value");
            let bool_value: Option<bool> = row.get("bool_value");

            let as_metadata_value = match (str_value, int_value, float_value, bool_value) {
                (Some(str_value), None, None, None) => MetadataValue::Str(str_value),
                (None, Some(int_value), None, None) => MetadataValue::Int(int_value),
                (None, None, Some(float_value), None) => MetadataValue::Float(float_value),
                (None, None, None, Some(bool_value)) => MetadataValue::Bool(bool_value),
                _ => {
                    return Err("Invalid metadata value".to_string());
                }
            };
            metadata.insert(key, as_metadata_value);
        }

        Self::sqlx_row_to_collection(collection_row, metadata)
    }

    // TODO: error
    fn sqlx_row_to_collection(row: SqliteRow, metadata: Metadata) -> Result<Collection, String> {
        let parsed_id = uuid::Uuid::parse_str(row.get("id")).map_err(|e| e.to_string())?;
        let collection_uuid = CollectionUuid(parsed_id);
        // TODO: empty metadata vs null metadata handling
        Ok(Collection {
            collection_id: collection_uuid,
            name: row.get("name"),
            metadata: Some(metadata),
            dimension: row.get("dimension"),
            database: row.get("database_name"),
            tenant: row.get("tenant_id"),

            // TODO: this fields are _currently_ unused in the rust code
            // however we can start using them when we switch to async compaction
            // as part of the switch to rust
            log_position: 0,
            version: 0,
            total_records_post_compaction: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use chroma_types::{SegmentScope, SegmentType, SegmentUuid};

    use crate::sqlite::tests::get_new_sqlite_db;

    use super::*;

    #[tokio::test]
    async fn test_create_database() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db);
        let db_id = uuid::Uuid::new_v4();
        sysdb.create_database(db_id, "test", None).await.unwrap();

        // Second call should fail
        let result = sysdb
            .create_database(uuid::Uuid::new_v4(), "test", None)
            .await;

        // TODO:
        // test same id or name
        // custom tenant

        // TODO: real error
        assert_eq!(
            result,
            Err("Database test already exists for tenant default_tenant".to_string())
        );

        let db = sysdb
            .get_database("test", None)
            .await
            .expect("Database to be created");
        assert_eq!(db.name, "test");
        assert_eq!(db.tenant, "default_tenant");
        assert_eq!(db.id, db_id);
    }

    #[tokio::test]
    async fn test_create_get_tenant() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db);
        sysdb
            .create_tenant("test")
            .await
            .expect("Tenant to be created");

        // Second call should fail
        let result = sysdb.create_tenant("test").await;

        // TODO: real error
        assert_eq!(result, Err("Tenant test already exists".to_string()));

        let tenant = sysdb.get_tenant("test").await.unwrap();
        assert_eq!(tenant.id, "test");
    }

    #[tokio::test]
    async fn test_create_collection() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db);
        let collection_id = CollectionUuid::new();
        let segments = vec![Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::Sqlite,
            scope: SegmentScope::METADATA,
            metadata: None,
            collection: collection_id,
            file_path: HashMap::new(),
        }];

        // TODO: reason about none vs empty metadata
        let metadata = Metadata::new();
        let (collection, created) = sysdb
            .create_collection(
                Some(collection_id),
                "test",
                segments,
                Some(&metadata),
                Some(256),
                false,
                None,
                None,
            )
            .await
            .expect("Collection to be created");

        assert!(created);
        assert_eq!(collection.collection_id, collection_id);
        assert_eq!(collection.name, "test");
        assert_eq!(collection.metadata, Some(metadata));
        assert_eq!(collection.dimension, Some(256));
        assert_eq!(collection.database, "default_database");
        assert_eq!(collection.tenant, "default_tenant");
    }
}
