use anyhow::Result;
use async_tempfile::TempFile;
use std::{future::Future, path::PathBuf};
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub(crate) fn get_dir_for_persistent_dataset_files() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("dataset_files")
}

async fn get_dataset_cache_path(
    dataset_name: &str,
    cache_dir: Option<PathBuf>,
) -> Result<PathBuf, std::io::Error> {
    let base_dir = cache_dir.unwrap_or(
        dirs::cache_dir()
            .expect("Failed to get cache directory")
            .join("chroma-test-datasets"),
    );
    let dataset_dir = base_dir.join(dataset_name);

    tokio::fs::create_dir_all(&dataset_dir).await?;

    Ok(dataset_dir)
}

/// Calls the populate callback to create a cached dataset file if it doesn't exist, and returns the path to the cached file.
///
/// The callback returns the writer it was given so that this function can flush
/// it. Two properties depend on that, and both break silently without it:
///
/// 1. The bytes the callback wrote are in the file before it is renamed into
///    place. A file handle does not finish its pending writes just because it
///    goes out of scope, and its own documentation requires a flush before it
///    is dropped for that reason. Without one the rename can publish a short
///    file, and the next reader gets a truncated one rather than an error.
/// 2. A caller cannot forget. Taking the writer back is what makes the flush
///    this function's responsibility instead of every callback's, which is why
///    the callback returns it rather than being trusted to flush it.
pub(crate) async fn get_or_populate_cached_dataset_file<F, Fut>(
    dataset_name: impl AsRef<str>,
    file_name: impl AsRef<str>,
    cache_dir: Option<PathBuf>,
    populate: F,
) -> Result<PathBuf>
where
    F: FnOnce(Box<dyn AsyncWrite + Unpin + Send>) -> Fut,
    Fut: Future<Output = Result<Box<dyn AsyncWrite + Unpin + Send>>>,
{
    let dataset_dir = get_dataset_cache_path(dataset_name.as_ref(), cache_dir).await?;
    let file_path = dataset_dir.join(file_name.as_ref());

    if !file_path.exists() {
        // We assume that dataset creation was successful if the file exists, so we use a temporary file to avoid scenarios where the file is partially written and then the callback fails.
        let temp = TempFile::new().await?;
        let mut writer = populate(Box::new(
            temp.try_clone().await.expect("Failed to clone file handle"),
        ))
        .await?;
        writer.flush().await?;
        drop(writer);
        tokio::fs::rename(temp.file_path(), &file_path).await?;
    }

    Ok(file_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_tempfile::TempDir;

    /// A cached file holds every byte the callback wrote.
    ///
    /// This covers the round trip, not the race it guards against. Losing the
    /// tail of a write depends on a pending operation still being in flight
    /// when the handle is dropped, which a test cannot force: on an idle
    /// machine the write lands first and the assertion passes either way. The
    /// flush is required by the file handle's own contract rather than by this
    /// test.
    #[tokio::test]
    async fn a_cached_file_holds_every_byte_the_callback_wrote() {
        let dir = TempDir::new().await.unwrap();
        let payload = vec![7u8; 1 << 20];
        let expected = payload.clone();

        let path = get_or_populate_cached_dataset_file(
            "flush_test",
            "payload.bin",
            Some(dir.to_path_buf()),
            |mut writer| async move {
                writer.write_all(&payload).await?;
                Ok(writer)
            },
        )
        .await
        .unwrap();

        assert_eq!(tokio::fs::read(&path).await.unwrap(), expected);
    }

    /// A second call returns the first call's file rather than repopulating it.
    #[tokio::test]
    async fn an_existing_cached_file_is_not_repopulated() {
        let dir = TempDir::new().await.unwrap();

        let write = |bytes: Vec<u8>| {
            let dir = dir.to_path_buf();
            async move {
                get_or_populate_cached_dataset_file(
                    "flush_test",
                    "once.bin",
                    Some(dir),
                    |mut writer| async move {
                        writer.write_all(&bytes).await?;
                        Ok(writer)
                    },
                )
                .await
                .unwrap()
            }
        };

        let first = write(b"first".to_vec()).await;
        let second = write(b"second".to_vec()).await;

        assert_eq!(first, second);
        assert_eq!(tokio::fs::read(&second).await.unwrap(), b"first");
    }
}
