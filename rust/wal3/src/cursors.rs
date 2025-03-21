use std::sync::Arc;

use chroma_storage::{ETag, PutOptions, Storage};

use crate::{CursorStoreOptions, Error, LogPosition};

//////////////////////////////////////////// CursorName ////////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CursorName(String);

impl CursorName {
    pub fn new(name: &str) -> Option<Self> {
        if !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            Some(Self(name.to_string()))
        } else {
            None
        }
    }

    pub fn path(&self) -> String {
        format!("cursor/{}.json", self.0)
    }

    pub fn from_path(path: &str) -> Option<Self> {
        let cursor_name = path.strip_prefix("cursor/")?.strip_suffix(".json")?;
        CursorName::new(cursor_name)
    }
}

////////////////////////////////////////////// Witness /////////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Witness(ETag, pub Cursor);

////////////////////////////////////////////// Cursor //////////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Cursor {
    position: LogPosition,
    epoch_us: u64,
    writer: String,
}

//////////////////////////////////////////// CursorStore ///////////////////////////////////////////

pub struct CursorStore {
    storage: Arc<Storage>,
    prefix: String,
    writer: String,
    semaphore: tokio::sync::Semaphore,
}

impl CursorStore {
    pub fn new(
        options: CursorStoreOptions,
        storage: Arc<Storage>,
        prefix: String,
        writer: String,
    ) -> Self {
        let semaphore = tokio::sync::Semaphore::new(options.concurrency);
        Self {
            storage,
            prefix,
            writer,
            semaphore,
        }
    }

    pub async fn load(&self, name: CursorName) -> Result<Witness, Error> {
        // SAFETY(rescrv):  Semaphore poisoning.
        let _permit = self.semaphore.acquire().await.unwrap();
        let path = format!("{}/{}", self.prefix, name.path());
        let (data, e_tag) = self.storage.get_with_e_tag(&path).await.map_err(Arc::new)?;
        let Some(e_tag) = e_tag else {
            return Err(Error::CorruptCursor(format!(
                "Missing ETag for cursor {}",
                name.0
            )));
        };
        let cursor: Cursor = serde_json::from_slice(&data).map_err(|e| {
            Error::CorruptCursor(format!("Failed to deserialize cursor {}: {}", name.0, e))
        })?;
        Ok(Witness(e_tag, cursor))
    }

    pub async fn init(&self, name: CursorName, cursor: Cursor) -> Result<Witness, Error> {
        // Semaphore taken by put.
        let options = PutOptions::if_not_exists();
        self.put(name, cursor, options).await
    }

    pub async fn save(
        &self,
        name: CursorName,
        cursor: Cursor,
        witness: Witness,
    ) -> Result<Witness, Error> {
        // Semaphore taken by put.
        let options = PutOptions::if_matches(&witness.0);
        self.put(name, cursor, options).await
    }

    async fn put(
        &self,
        name: CursorName,
        mut cursor: Cursor,
        options: PutOptions,
    ) -> Result<Witness, Error> {
        // SAFETY(rescrv):  Semaphore poisoning.
        let _permit = self.semaphore.acquire().await.unwrap();
        cursor.writer = self.writer.clone();
        let path = format!("{}/{}", self.prefix, name.path());
        let data = serde_json::to_vec(&cursor).map_err(|err| {
            Error::CorruptCursor(format!("Failed to serialize cursor {}: {}", name.0, err))
        })?;
        let e_tag = self
            .storage
            .put_bytes(&path, data, options)
            .await
            .map_err(Arc::new)?;
        let Some(e_tag) = e_tag else {
            return Err(Error::CorruptCursor(format!(
                "Missing ETag for cursor {}",
                name.0
            )));
        };
        Ok(Witness(e_tag, cursor))
    }
}

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_new() {
        assert!(CursorName::new("valid_cursor").is_some());
        assert!(CursorName::new("__valid_cursor__").is_some());
        assert!(CursorName::new("__valid_cursor9__").is_some());
        assert!(CursorName::new("some-cursor").is_none());
        assert!(CursorName::new("").is_none());
    }

    #[test]
    fn cursor_path_round_trip() {
        let name = super::CursorName::new("test_cursor").unwrap();
        let path = name.path();
        let parsed_name = super::CursorName::from_path(&path).unwrap();
        assert_eq!(name.0, parsed_name.0);
    }
}
