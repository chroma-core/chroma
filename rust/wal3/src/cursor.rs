use std::collections::HashSet;

use futures::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload, Result};

use crate::{Error, LogPosition};

////////////////////////////////////////////// Cursor //////////////////////////////////////////////

#[derive(
    Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct Cursor(String);

impl Cursor {
    pub fn new(s: impl AsRef<str>) -> Option<Self> {
        let s = s.as_ref();
        if !s.is_empty()
            && s.chars().next().unwrap().is_alphabetic()
            && s.chars().all(|c| c.is_alphanumeric())
        {
            Some(Self(s.to_string()))
        } else {
            None
        }
    }
}

////////////////////////////////////////// CursorMetadata //////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Deserialize, serde::Serialize)]
pub struct CursorMetadata {
    pub cursor: Cursor,
    pub position: LogPosition,
    // TODO(rescrv):  metadata about writer process
}

//////////////////////////////////////////// CursorStore ///////////////////////////////////////////

#[derive(Clone, Debug, Default)]
pub struct CursorStore {}

impl CursorStore {
    pub async fn load(
        &self,
        object_store: &impl ObjectStore,
        cursor: &Cursor,
    ) -> Result<LogPosition, Error> {
        let path = self.prefix_for_cursor(cursor);
        let mut listings = object_store.list(Some(&path));
        let mut max_position = None;
        while let Some(meta) = listings.next().await.transpose()? {
            let this_position = self
                .extract_position_from_path(&meta.location, cursor)
                .ok_or_else(|| {
                    Error::CorruptCursor(format!(
                        "could not extract position from cursor: {:?}",
                        meta.location,
                    ))
                })?;
            match &mut max_position {
                Some(max_position) => {
                    if this_position > *max_position {
                        *max_position = this_position;
                    }
                }
                None => {
                    max_position = Some(this_position);
                }
            }
        }
        if let Some(max_position) = max_position {
            Ok(max_position)
        } else {
            Err(Error::NoSuchCursor(cursor.clone()))
        }
    }

    pub async fn save(
        &self,
        object_store: &impl ObjectStore,
        cursor: &Cursor,
        position: LogPosition,
    ) -> Result<(), Error> {
        let metadata = CursorMetadata {
            cursor: cursor.clone(),
            position,
        };
        let location = self.path_for_position(cursor, position);
        let payload = serde_json::to_string(&metadata).map_err(|err| {
            Error::CorruptCursor(format!("could not encode cursor metadata: {err:?}"))
        })?;
        let payload = PutPayload::from(payload);
        let opts: PutOptions = PutMode::Create.into();
        let _ = object_store.put_opts(&location, payload, opts).await?;
        self.prune(object_store, cursor, position).await?;
        Ok(())
    }

    pub async fn snip(
        &self,
        object_store: &impl ObjectStore,
        cursor: &Cursor,
    ) -> Result<(), Error> {
        self.prune(object_store, cursor, LogPosition(u64::MAX))
            .await
    }

    pub async fn list(&self, object_store: &impl ObjectStore) -> Result<Vec<Cursor>, Error> {
        let mut cursors: HashSet<Cursor> = HashSet::default();
        let mut listings = object_store.list(Some(&Path::from("cursor/")));
        while let Some(meta) = listings.next().await.transpose()? {
            if let Some(cursor) = self.extract_cursor_from_path(&meta.location) {
                cursors.insert(cursor);
            } else {
                return Err(Error::CorruptCursor(format!(
                    "could not extract cursor from path: {:?}",
                    meta.location,
                )));
            }
        }
        let mut cursors: Vec<_> = cursors.into_iter().collect();
        cursors.sort();
        Ok(cursors)
    }

    pub fn path_for_position(&self, cursor: &Cursor, position: LogPosition) -> Path {
        Path::from(format!("cursor/{}/{}", cursor.0, position.0))
    }

    pub fn prefix_for_cursor(&self, cursor: &Cursor) -> Path {
        Path::from(format!("cursor/{}/", cursor.0))
    }

    pub fn extract_cursor_from_path(&self, path: &Path) -> Option<Cursor> {
        let path: &str = path.as_ref();
        if let Some(path) = path.strip_prefix("cursor/") {
            let (cursor_name, log_position) = path.split_once('/')?;
            let _: u64 = log_position.parse().ok()?;
            Cursor::new(cursor_name)
        } else {
            None
        }
    }

    pub fn extract_position_from_path(&self, path: &Path, cursor: &Cursor) -> Option<LogPosition> {
        let path: &str = path.as_ref();
        if let Some(path) = path.strip_prefix("cursor/") {
            let (cursor_name, log_position) = path.split_once('/')?;
            if cursor_name != cursor.0 {
                return None;
            }
            Some(LogPosition(log_position.parse().ok()?))
        } else {
            None
        }
    }

    async fn prune(
        &self,
        object_store: &impl ObjectStore,
        cursor: &Cursor,
        position: LogPosition,
    ) -> Result<(), Error> {
        let path = self.prefix_for_cursor(cursor);
        let mut listings = object_store.list(Some(&path));
        while let Some(meta) = listings.next().await.transpose()? {
            let this_position = self
                .extract_position_from_path(&meta.location, cursor)
                .ok_or_else(|| {
                    Error::CorruptCursor(format!(
                        "could not extract position from cursor: {:?}",
                        meta.location,
                    ))
                })?;
            if this_position < position {
                object_store.delete(&meta.location).await?;
            }
        }
        Ok(())
    }
}

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn save_load_list_snip() {
        let object_store = object_store::memory::InMemory::new();
        let cursor_store = CursorStore::default();
        assert_eq!(
            Vec::<Cursor>::new(),
            cursor_store.list(&object_store).await.unwrap()
        );
        let cursor = Cursor::new("cursor1").unwrap();
        cursor_store
            .save(&object_store, &cursor, LogPosition(1))
            .await
            .unwrap();
        assert_eq!(
            LogPosition(1),
            cursor_store.load(&object_store, &cursor).await.unwrap()
        );
        assert_eq!(
            vec![cursor.clone()],
            cursor_store.list(&object_store).await.unwrap()
        );
        cursor_store.snip(&object_store, &cursor).await.unwrap();
        assert_eq!(
            Vec::<Cursor>::new(),
            cursor_store.list(&object_store).await.unwrap()
        );
    }

    #[test]
    fn paths() {
        let cursor_store = CursorStore::default();
        let cursor = Cursor::new("cursor1").unwrap();
        let position = LogPosition(1);
        assert_eq!(
            Path::from("cursor/cursor1/1"),
            cursor_store.path_for_position(&cursor, position)
        );
        assert_eq!(
            Path::from("cursor/cursor1/"),
            cursor_store.prefix_for_cursor(&cursor)
        );
        assert_eq!(
            Cursor::new("cursor1").unwrap(),
            cursor_store
                .extract_cursor_from_path(&Path::from("cursor/cursor1/1"))
                .unwrap(),
        );
        assert_eq!(
            LogPosition(1),
            cursor_store
                .extract_position_from_path(&Path::from("cursor/cursor1/1"), &cursor)
                .unwrap(),
        );
        assert_eq!(
            LogPosition(u64::MAX),
            cursor_store
                .extract_position_from_path(
                    &Path::from("cursor/cursor1/18446744073709551615"),
                    &cursor,
                )
                .unwrap(),
        );
    }
}
