use super::serde;
use chroma_storage;

struct S3SysDBClient {
    storage: chroma_storage::Storage,
    sysdb_prefix: String,
}

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

impl S3SysDBClient {
    // TODO: From config
    fn new(storage: chroma_storage::Storage, prefix: String) -> Self {
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
            .put_bytes(&path_to_tenant_data, tenant_data_bytes)
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

    // TODO: we need to push these DDL onto the log in order to process deletes realistically
    async fn create_database(&self, id: String, name: String, tenant: String) {
        let path_to_tenant = format!("{}/{}", self.sysdb_prefix, tenant);
        let path_to_database = format!("{}/{}", path_to_tenant, id);
        let path_to_database_data = format!("{}/data", path_to_database);

        // TODO: this shold use block
        let database_data = serde::DatabaseData { id, name };
        let database_data_bytes = bincode::serialize(&database_data).unwrap();

        // TODO: put if not exists
        let res = self
            .storage
            .put_bytes(&path_to_database_data, database_data_bytes)
            .await;
    }
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
