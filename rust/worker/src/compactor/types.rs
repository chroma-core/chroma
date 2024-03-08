#[derive(Clone, Eq, PartialEq)]
pub(crate) struct Task {
    pub(crate) collection_id: String,
    pub(crate) tenant_id: String,
    pub(crate) offset: i64,
}
