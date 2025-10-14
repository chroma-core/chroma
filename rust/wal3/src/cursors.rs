use std::borrow::Cow;
use std::sync::Arc;

use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, ETag, GetOptions, PutOptions, Storage,
    StorageError,
};

use crate::{CursorStoreOptions, Error, LogPosition};

//////////////////////////////////////////// CursorName ////////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CursorName<'a>(Cow<'a, str>);

impl CursorName<'_> {
    /// # Safety
    ///
    /// The caller must ensure that the name is a valid cursor name.  This means a non-empty
    /// alphanumeric string with underscores.
    pub const unsafe fn from_string_unchecked(name: &str) -> CursorName {
        CursorName(Cow::Borrowed(name))
    }

    pub fn new(name: &str) -> Option<Self> {
        if Self::validate(name) {
            Some(Self(name.to_string().into()))
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

    pub fn is_valid(&self) -> bool {
        Self::validate(&self.0)
    }

    fn validate(name: &str) -> bool {
        !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    }
}

////////////////////////////////////////////// Witness /////////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Witness {
    e_tag: ETag,
    pub cursor: Cursor,
}

impl Witness {
    /// This method constructs a witness that will likely fail, but that contains a new cursor.
    /// Useful in tests and not much else.
    pub fn default_etag_with_cursor(cursor: Cursor) -> Self {
        Self {
            e_tag: ETag("NO MATCH".to_string()),
            cursor,
        }
    }

    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }
}

////////////////////////////////////////////// Cursor //////////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Cursor {
    pub position: LogPosition,
    pub epoch_us: u64,
    pub writer: String,
}

impl Default for Cursor {
    fn default() -> Self {
        Self {
            position: LogPosition::from_offset(1),
            epoch_us: 1,
            writer: "default-cursor".to_string(),
        }
    }
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
        // This semaphore keeps the cursor store to a percentage of all traffic.
        let semaphore = tokio::sync::Semaphore::new(options.concurrency);
        Self {
            storage,
            prefix,
            writer,
            semaphore,
        }
    }

    pub async fn load(&self, name: &CursorName<'_>) -> Result<Option<Witness>, Error> {
        // SAFETY(rescrv):  Semaphore poisoning.
        let _permit = self.semaphore.acquire().await.unwrap();
        let path = format!("{}/{}", self.prefix, name.path());
        let (data, e_tag) = match self
            .storage
            .get_with_e_tag(&path, GetOptions::new(StorageRequestPriority::P0))
            .await
            .map_err(Arc::new)
        {
            Ok((data, e_tag)) => (data, e_tag),
            Err(err) => match &*err {
                StorageError::NotFound { path: _, source: _ } => return Ok(None),
                _ => return Err(err.into()),
            },
        };
        let Some(e_tag) = e_tag else {
            return Err(Error::CorruptCursor(format!(
                "Missing ETag for cursor {}",
                name.0
            )));
        };
        let cursor: Cursor = serde_json::from_slice(&data).map_err(|e| {
            Error::CorruptCursor(format!("Failed to deserialize cursor {}: {}", name.0, e))
        })?;
        Ok(Some(Witness { e_tag, cursor }))
    }

    pub async fn init(&self, name: &CursorName<'_>, cursor: Cursor) -> Result<Witness, Error> {
        // Semaphore taken by put.
        let options = PutOptions::if_not_exists(StorageRequestPriority::P0);
        self.put(name, cursor, options).await
    }

    pub async fn save(
        &self,
        name: &CursorName<'_>,
        cursor: &Cursor,
        witness: &Witness,
    ) -> Result<Witness, Error> {
        // Semaphore taken by put.
        let options = PutOptions::if_matches(&witness.e_tag, StorageRequestPriority::P0);
        self.put(name, cursor.clone(), options).await
    }

    async fn put(
        &self,
        name: &CursorName<'_>,
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
        Ok(Witness { e_tag, cursor })
    }

    pub async fn list(&self) -> Result<Vec<CursorName<'_>>, Error> {
        let curdir = format!("{}/cursor/", self.prefix);
        let paths = self
            .storage
            .list_prefix(&curdir, GetOptions::default())
            .await
            .map_err(Arc::new)?;
        let mut cursors = vec![];
        for path in paths {
            if let Some(mut cursor) = path.strip_prefix(&self.prefix) {
                cursor = cursor.trim_start_matches('/');
                if let Some(cursor_name) = CursorName::from_path(cursor) {
                    cursors.push(cursor_name)
                } else {
                    return Err(Error::CorruptCursor(format!(
                        "do not understand cursor at {path}"
                    )));
                }
            }
        }
        Ok(cursors)
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

    #[tokio::test]
    async fn test_k8s_integration_save_and_load() {
        let storage = Arc::new(chroma_storage::s3_client_for_test_with_new_bucket().await);
        let store = CursorStore::new(
            CursorStoreOptions::default(),
            Arc::clone(&storage),
            "prefix-for-save-and-load".to_string(),
            "test-writer".to_string(),
        );
        store
            .init(
                &CursorName::new("test_cursor").unwrap(),
                Cursor {
                    position: LogPosition::from_offset(42),
                    epoch_us: 12345u64,
                    writer: "test-writer".to_string(),
                },
            )
            .await
            .unwrap();
        let witness = store
            .load(&CursorName::new("test_cursor").unwrap())
            .await
            .unwrap()
            .unwrap();
        store
            .save(
                &CursorName::new("test_cursor").unwrap(),
                &Cursor {
                    position: LogPosition::from_offset(99),
                    epoch_us: 54321u64,
                    writer: "writer-test".to_string(),
                },
                &witness,
            )
            .await
            .unwrap();
        let witness = store
            .load(&CursorName::new("test_cursor").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(LogPosition::from_offset(99), witness.cursor.position);
        assert_eq!(54321u64, witness.cursor.epoch_us);
        assert_eq!("test-writer", witness.cursor.writer);
    }
}
