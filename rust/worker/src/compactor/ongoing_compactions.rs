use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use chroma_types::CollectionUuid;
use uuid::Uuid;

/// Persists the collections whose compactions may be interrupted by a process crash.
#[derive(Debug)]
pub(crate) struct OngoingCompactions {
    path: PathBuf,
    collection_ids: HashSet<CollectionUuid>,
}

impl OngoingCompactions {
    /// Opens the state file assigned to one compactor member.
    ///
    /// The member ID becomes the file name beneath `directory`, which keeps
    /// multiple compactor pods using the same node-local volume isolated.
    ///
    /// # Errors
    ///
    /// Returns an error if the member ID cannot safely form a file name or the
    /// state file cannot be read or created. Malformed lines are discarded so
    /// local state corruption cannot put the compactor into a crash loop.
    pub(crate) fn for_member(directory: &Path, member_id: &str) -> io::Result<Self> {
        if member_id.is_empty()
            || !member_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid compactor member ID for state file: {member_id:?}"),
            ));
        }

        Self::load(directory.join(format!("{member_id}.txt")))
    }

    /// Loads a state file, creating an empty one when it does not yet exist.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read or created. Malformed lines
    /// are discarded while valid collection UUIDs remain recoverable.
    pub(crate) fn load(path: PathBuf) -> io::Result<Self> {
        let collection_ids = match fs::read_to_string(&path) {
            Ok(contents) => {
                let mut collection_ids = HashSet::new();
                for (line_number, line) in contents.lines().enumerate() {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    match Uuid::parse_str(line) {
                        Ok(collection_id) => {
                            collection_ids.insert(CollectionUuid(collection_id));
                        }
                        Err(error) => {
                            tracing::error!(
                                path = %path.display(),
                                line_number = line_number + 1,
                                line,
                                error = ?error,
                                "Discarding malformed ongoing-compaction state",
                            );
                        }
                    }
                }
                collection_ids
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => HashSet::new(),
            Err(error) => return Err(error),
        };

        let state = Self {
            path,
            collection_ids,
        };
        state.persist(&state.collection_ids)?;
        Ok(state)
    }

    pub(crate) fn collection_ids(&self) -> impl Iterator<Item = CollectionUuid> + '_ {
        self.collection_ids.iter().copied()
    }

    /// Records a collection before its compaction can begin.
    ///
    /// # Errors
    ///
    /// Returns an error if the updated state cannot be durably replaced. The
    /// in-memory state remains unchanged when persistence fails.
    pub(crate) fn insert(&mut self, collection_id: CollectionUuid) -> io::Result<bool> {
        if self.collection_ids.contains(&collection_id) {
            return Ok(false);
        }

        let mut next = self.collection_ids.clone();
        next.insert(collection_id);
        self.replace(next)
    }

    /// Removes a collection after its compaction reaches a terminal state.
    ///
    /// # Errors
    ///
    /// Returns an error if the updated state cannot be durably replaced. The
    /// in-memory state remains unchanged when persistence fails.
    pub(crate) fn remove(&mut self, collection_id: CollectionUuid) -> io::Result<bool> {
        if !self.collection_ids.contains(&collection_id) {
            return Ok(false);
        }

        let mut next = self.collection_ids.clone();
        next.remove(&collection_id);
        self.replace(next)
    }

    fn replace(&mut self, next: HashSet<CollectionUuid>) -> io::Result<bool> {
        self.persist(&next)?;
        self.collection_ids = next;
        Ok(true)
    }

    fn persist(&self, collection_ids: &HashSet<CollectionUuid>) -> io::Result<()> {
        let parent = self.path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("state file has no parent: {}", self.path.display()),
            )
        })?;
        fs::create_dir_all(parent)?;

        let file_name = self.path.file_name().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("state file has no file name: {}", self.path.display()),
            )
        })?;
        let temporary_path = self
            .path
            .with_file_name(format!(".{}.tmp", file_name.to_string_lossy()));
        let mut temporary = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary_path)?;

        let mut sorted_ids: Vec<_> = collection_ids.iter().map(|id| id.0).collect();
        sorted_ids.sort_unstable();
        for collection_id in sorted_ids {
            writeln!(temporary, "{collection_id}")?;
        }
        temporary.sync_all()?;
        fs::rename(&temporary_path, &self.path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn collection_id(value: &str) -> CollectionUuid {
        CollectionUuid::from_str(value).unwrap()
    }

    #[test]
    fn transitions_are_durable_and_sorted() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("member.txt");
        let first = collection_id("00000000-0000-0000-0000-000000000001");
        let second = collection_id("00000000-0000-0000-0000-000000000002");

        let mut state = OngoingCompactions::load(path.clone()).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "");
        assert!(state.insert(second).unwrap());
        assert!(state.insert(first).unwrap());
        assert!(!state.insert(first).unwrap());
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "00000000-0000-0000-0000-000000000001\n\
             00000000-0000-0000-0000-000000000002\n"
        );

        let mut reopened = OngoingCompactions::load(path.clone()).unwrap();
        let mut recovered: Vec<_> = reopened.collection_ids().collect();
        recovered.sort_unstable();
        assert_eq!(recovered, vec![first, second]);

        assert!(reopened.remove(first).unwrap());
        assert!(!reopened.remove(first).unwrap());
        assert!(reopened.remove(second).unwrap());
        assert_eq!(fs::read_to_string(path).unwrap(), "");
    }

    #[test]
    fn malformed_uuid_is_discarded_without_losing_valid_entries() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("member.txt");
        let valid = collection_id("00000000-0000-0000-0000-000000000001");
        fs::write(&path, "not-a-uuid\n00000000-0000-0000-0000-000000000001\n").unwrap();

        let state = OngoingCompactions::load(path.clone()).unwrap();

        assert_eq!(state.collection_ids().collect::<Vec<_>>(), vec![valid]);
        assert_eq!(
            fs::read_to_string(path).unwrap(),
            "00000000-0000-0000-0000-000000000001\n"
        );
    }

    #[test]
    fn failed_persistence_does_not_publish_the_new_in_memory_state() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("member.txt");
        let collection_id = collection_id("00000000-0000-0000-0000-000000000001");
        let mut state = OngoingCompactions::load(path.clone()).unwrap();
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();

        assert!(state.insert(collection_id).is_err());
        assert_eq!(state.collection_ids().collect::<Vec<_>>(), Vec::new());
    }

    #[test]
    fn member_ids_are_isolated_and_validated() {
        let directory = tempfile::tempdir().unwrap();

        let first = OngoingCompactions::for_member(directory.path(), "compactor-0").unwrap();
        let second = OngoingCompactions::for_member(directory.path(), "compactor-1").unwrap();

        assert_ne!(first.path, second.path);
        let error = OngoingCompactions::for_member(directory.path(), "../compactor").unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
