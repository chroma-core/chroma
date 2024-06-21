use crate::chroma_proto::TenantLastCompactionTime;

pub(crate) struct Tenant {
    pub(crate) id: String,
    pub(crate) last_compaction_time: i64,
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
