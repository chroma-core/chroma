use super::serde;

struct S3SysDBClient {
    storage: chroma_storage::s3::S3Storage,
    sysdb_prefix: String,
}

// OLD

// service SysDB {
//   rpc CreateDatabase(CreateDatabaseRequest) returns (CreateDatabaseResponse) {}
//   rpc GetDatabase(GetDatabaseRequest) returns (GetDatabaseResponse) {}
//   rpc CreateTenant(CreateTenantRequest) returns (CreateTenantResponse) {}
//   rpc GetTenant(GetTenantRequest) returns (GetTenantResponse) {}
//   rpc CreateSegment(CreateSegmentRequest) returns (CreateSegmentResponse) {}
//   rpc DeleteSegment(DeleteSegmentRequest) returns (DeleteSegmentResponse) {}
//   rpc GetSegments(GetSegmentsRequest) returns (GetSegmentsResponse) {}
//   rpc UpdateSegment(UpdateSegmentRequest) returns (UpdateSegmentResponse) {}
//   rpc CreateCollection(CreateCollectionRequest) returns (CreateCollectionResponse) {}
//   rpc DeleteCollection(DeleteCollectionRequest) returns (DeleteCollectionResponse) {}
//   rpc GetCollections(GetCollectionsRequest) returns (GetCollectionsResponse) {}
//   rpc UpdateCollection(UpdateCollectionRequest) returns (UpdateCollectionResponse) {}
//   rpc ResetState(google.protobuf.Empty) returns (ResetStateResponse) {}
//   rpc GetLastCompactionTimeForTenant(GetLastCompactionTimeForTenantRequest) returns (GetLastCompactionTimeForTenantResponse) {}
//   rpc SetLastCompactionTimeForTenant(SetLastCompactionTimeForTenantRequest) returns (google.protobuf.Empty) {}
//   rpc FlushCollectionCompaction(FlushCollectionCwompactionRequest) returns (FlushCollectionCompactionResponse) {}
// }

// NEW

// service SysDB {
//   rpc CreateDatabase(CreateDatabaseRequest) returns (CreateDatabaseResponse) {}
//   rpc GetDatabase(GetDatabaseRequest) returns (GetDatabaseResponse) {}

//   rpc CreateTenant(CreateTenantRequest) returns (CreateTenantResponse) {}
//   rpc GetTenant(GetTenantRequest) returns (GetTenantResponse) {}

//   rpc CreateCollection(CreateCollectionRequest) returns (CreateCollectionResponse) {}
//   rpc DeleteCollection(DeleteCollectionRequest) returns (DeleteCollectionResponse) {}
//   rpc GetCollections(GetCollectionsRequest) returns (GetCollectionsResponse) {}
//   rpc UpdateCollection(UpdateCollectionRequest) returns (UpdateCollectionResponse) {}

//   rpc ResetState(google.protobuf.Empty) returns (ResetStateResponse) {}

//   rpc GetLastCompactionTimeForTenant(GetLastCompactionTimeForTenantRequest) returns (GetLastCompactionTimeForTenantResponse) {}
//   rpc SetLastCompactionTimeForTenant(SetLastCompactionTimeForTenantRequest) returns (google.protobuf.Empty) {}

//   rpc FlushCollectionCompaction(FlushCollectionCwompactionRequest) returns (FlushCollectionCompactionResponse) {}
// }

impl S3SysDBClient {
    // TODO: From config
    // TODO: use storage wrapper not storage
    fn new(storage: chroma_storage::s3::S3Storage, prefix: String) -> Self {
        S3SysDBClient {
            storage,
            sysdb_prefix: prefix,
        }
    }

    // TODO: accept owned
    async fn create_tenant(&self, tenant: &str) {
        let path_to_tenant = format!("{}/{}", self.sysdb_prefix, tenant);
        let path_to_tenant_data = format!("{}/data", path_to_tenant);

        // TODO: This should use block, but we need tuple level serialization for that
        let tenant_data = serde::TenantData::new(tenant.to_string());
        let tenant_data_bytes = bincode::serialize(&tenant_data).unwrap();

        // TODO: PUT IF NOT EXISTS
        let res = self
            .storage
            .put_bytes_if_not_exists(&path_to_tenant_data, tenant_data_bytes)
            .await;
    }

    async fn get_tenant(&self, tenant: &str) -> Result<serde::TenantData, String> {
        let path_to_tenant = format!("{}/{}", self.sysdb_prefix, tenant);
        let path_to_tenant_data = format!("{}/data", path_to_tenant);

        // TODO: don't unwrap
        let bytes = self.storage.get(&path_to_tenant_data).await.unwrap();
        let tenant_data: serde::TenantData = bincode::deserialize(&bytes).unwrap();

        Ok(tenant_data)
    }

    // TODO: we need to push these DDL onto the log in order to process deletes realistically...
    // however we don't curently support deletes
    async fn create_database(&self, id: String, name: String, tenant: String) {
        let path_to_tenant = format!("{}/{}", self.sysdb_prefix, tenant);
        let path_to_database = format!("{}/{}", path_to_tenant, id);
        let path_to_database_data = format!("{}/data", path_to_database);

        // TODO: this should use block not bincode
        let database_data = serde::DatabaseData::new(name, id);
        let database_data_bytes = bincode::serialize(&database_data).unwrap();

        // TODO: put if not exists
        let res = self
            .storage
            .put_bytes_if_not_exists(&path_to_database_data, database_data_bytes)
            .await;
    }

    /*

        message CreateCollectionRequest {
        string id = 1;
        string name = 2;
        string configuration_json_str = 3;
        optional UpdateMetadata metadata = 4;
        optional int32 dimension = 5;
        optional bool get_or_create = 6;
        string tenant = 7;
        string database = 8;
        // When segments are set, then the collection and segments will be created as
        // a single atomic operation.
        repeated Segment segments = 9;  // Optional.
    }
         */
    async fn create_collection(&self, id: String, name: String) {}
}

// Ideally

// DDL -> Gets put on log
// Example
// CreateTenant gets put on the log
// GetTenant() gets called -> tenant gets resolved into logstream
// log stream pulled, we resolve the state and return the information
//

// Example
// CreateDatabase()
// CreateDatabase gets put on log
//

mod tests {
    use super::*;
}
