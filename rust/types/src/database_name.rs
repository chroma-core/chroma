use serde::Deserialize;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DatabaseName(String);

impl<'de> Deserialize<'de> for DatabaseName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        DatabaseName::new(s)
            .ok_or_else(|| serde::de::Error::custom("database name cannot be empty"))
    }
}

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
