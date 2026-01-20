use crate::chroma_proto::TenantLastCompactionTime;

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

impl From<Tenant> for crate::chroma_proto::Tenant {
    fn from(tenant: Tenant) -> Self {
        crate::chroma_proto::Tenant {
            name: tenant.id,
            resource_name: tenant.resource_name,
        }
    }
}
