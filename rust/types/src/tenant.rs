use crate::chroma_proto::TenantLastCompactionTime;
use pyo3::pyclass;

#[derive(Debug, PartialEq)]
#[pyclass]
pub struct Tenant {
    #[pyo3(get)]
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
