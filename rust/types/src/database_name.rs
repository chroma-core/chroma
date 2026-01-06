#[derive(
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    serde::Deserialize,
    serde::Serialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DatabaseName(String);

impl DatabaseName {
    pub fn new(dbname: impl Into<String>) -> Option<Self> {
        let dbname = dbname.into();
        if !dbname.is_empty() {
            Some(DatabaseName(dbname))
        } else {
            None
        }
    }

    pub fn into_string(self) -> String {
        self.0
    }
}
