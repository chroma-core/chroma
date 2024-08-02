use crate::chroma_proto::TenantLastCompactionTime;

pub struct Tenant {
    pub id: String,
    pub last_compaction_time: i64,
}

impl TryFrom<TenantLastCompactionTime> for Tenant {
    type Error = ();

    fn try_from(proto_tenant: TenantLastCompactionTime) -> Result<Self, Self::Error> {
        Ok(Tenant {
            id: proto_tenant.tenant_id,
            last_compaction_time: proto_tenant.last_compaction_time,
        })
    }
}
