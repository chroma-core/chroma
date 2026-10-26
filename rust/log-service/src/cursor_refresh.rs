//! Verify durable compaction progress before rejecting writes for a full log.
use super::*;

// Concurrent rejected requests share a short-lived result rather than each reading storage.
const BACKPRESSURE_CURSOR_MAX_AGE: Duration = Duration::from_secs(1);

#[derive(Serialize, Deserialize)]
pub(super) struct VerifiedCursor {
    pub(super) checked_at: SystemTime,
    pub(super) witness: Option<CursorWitness>,
}

impl LogServer {
    /// Returns true when the collection still needs compaction backpressure.
    #[allow(clippy::result_large_err)]
    pub(super) async fn revalidate_backpressure(
        &self,
        collection_id: CollectionUuid,
    ) -> Result<bool, Status> {
        // Replicated logs obtain their positions from the replication metadata store.
        // This refresh applies only to the object-storage dirty-log summaries.
        if !self.need_to_compact_s3.lock().contains_key(&collection_id) {
            return Ok(true);
        }
        let handle = self.open_logs.get_or_create_state(LogKey { collection_id });
        let _active = handle.active.lock().await;
        let refresh_key = cache_key_for_verified_cursor(collection_id);
        let cached = if let Some(cache) = &self.cache {
            cache
                .get(&refresh_key)
                .await
                .ok()
                .flatten()
                .and_then(|bytes| serde_json::from_slice::<VerifiedCursor>(&bytes.bytes).ok())
        } else {
            None
        };
        let (verified, refreshed_from_storage) = match cached.filter(|v| {
            v.checked_at
                .elapsed()
                .is_ok_and(|age| age < BACKPRESSURE_CURSOR_MAX_AGE)
        }) {
            Some(verified) => (verified, false),
            None => {
                let storage = self.preferred_storage().map_err(status_from_chroma_error)?;
                let cursors = CursorStore::new(
                    CursorStoreOptions::default(),
                    Arc::new(storage.clone()),
                    collection_id.storage_prefix_for_log(),
                    self.config.my_member_id.clone(),
                );
                let checked_at = SystemTime::now();
                let witness = cursors.load(&INTRINSIC_CURSOR).await.map_err(|err| {
                    tracing::warn!(%collection_id, %err, "could not revalidate compaction backpressure");
                    Status::unavailable("could not verify compaction progress")
                })?;
                let verified = VerifiedCursor {
                    checked_at,
                    witness,
                };
                if let Some(cache) = &self.cache {
                    let cursor_key = cache_key_for_cursor(collection_id, &INTRINSIC_CURSOR);
                    if let Some(witness) = &verified.witness {
                        if let Ok(bytes) = serde_json::to_vec(witness) {
                            cache.insert(cursor_key, CachedBytes::new(bytes)).await;
                        }
                    } else {
                        cache.remove(&cursor_key).await;
                    }
                    if let Ok(bytes) = serde_json::to_vec(&verified) {
                        cache.insert(refresh_key, CachedBytes::new(bytes)).await;
                    }
                }
                (verified, true)
            }
        };
        // Re-read summaries after storage I/O so concurrent rollups are included. Do not
        // clear the shared pressure set: another topology or a later rollup can still be full.
        let rollup = self.need_to_compact_s3.lock().get(&collection_id).copied();
        let s3_pressure = rollup.is_some_and(|mut rollup| {
            if let Some(witness) = &verified.witness {
                rollup.witness_cursor(Some(witness));
            }
            rollup.requires_backpressure(self.config.num_records_before_backpressure)
        });
        let repl_pressure = self
            .need_to_compact_repl
            .lock()
            .iter()
            .any(|((_, id), rollup)| {
                *id == collection_id
                    && rollup.requires_backpressure(self.config.num_records_before_backpressure)
            });
        let backpressure = s3_pressure || repl_pressure;
        if refreshed_from_storage {
            tracing::info!(%collection_id,
                cached_position = ?rollup.map(|r| r.start_log_position),
                pending_limit = ?rollup.map(|r| r.limit_log_position),
                committed_position = ?verified.witness.as_ref().map(|w| w.cursor.position),
                backpressure,
                "revalidated compaction backpressure");
        }
        Ok(backpressure)
    }
}
