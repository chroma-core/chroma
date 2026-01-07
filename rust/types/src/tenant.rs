use google_cloud_spanner::row::Row;

use crate::chroma_proto::{Tenant as ProtoTenant, TenantLastCompactionTime};
use crate::sysdb_errors::SysDbError;

#[derive(Debug, Clone)]
pub struct Tenant {
    pub id: String,
    pub last_compaction_time: i64,
    pub resource_name: Option<String>,
}

impl TryFrom<TenantLastCompactionTime> for Tenant {
    type Error = ();

    fn try_from(proto_tenant: TenantLastCompactionTime) -> Result<Self, Self::Error> {
        Ok(Tenant {
            id: proto_tenant.tenant_id,
            last_compaction_time: proto_tenant.last_compaction_time,
            resource_name: None,
        })
    }
}

impl TryFrom<Row> for Tenant {
    type Error = SysDbError;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let id: String = row
            .column_by_name("id")
            .map_err(|e| SysDbError::Internal(format!("failed to read 'id' column: {}", e)))?;

        // resource_name can be NULL, so we handle the error as None
        let resource_name: Option<String> = row.column_by_name("resource_name").ok();

        let last_compaction_time: i64 =
            row.column_by_name("last_compaction_time").map_err(|e| {
                SysDbError::Internal(format!(
                    "failed to read 'last_compaction_time' column: {}",
                    e
                ))
            })?;

        Ok(Tenant {
            id,
            resource_name,
            last_compaction_time,
        })
    }
}

impl From<Tenant> for ProtoTenant {
    fn from(tenant: Tenant) -> Self {
        ProtoTenant {
            name: tenant.id,
            resource_name: tenant.resource_name,
        }
    }
}
