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
            .ok_or_else(|| serde::de::Error::custom("database name must be at least 3 characters"))
    }
}

impl DatabaseName {
    /// Creates a new DatabaseName if the name is at least 3 characters long.
    pub fn new(dbname: impl Into<String>) -> Option<Self> {
        let dbname = dbname.into();
        if dbname.len() >= 3 {
            Some(DatabaseName(dbname))
        } else {
            None
        }
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn topology(&self) -> Option<String> {
        self.0.split_once('+').map(|x| x.0.to_string())
    }
}

impl AsRef<str> for DatabaseName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for DatabaseName {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<DatabaseName> for str {
    fn eq(&self, other: &DatabaseName) -> bool {
        self == other.0
    }
}

impl PartialEq<String> for DatabaseName {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl PartialEq<DatabaseName> for String {
    fn eq(&self, other: &DatabaseName) -> bool {
        *self == other.0
    }
}
