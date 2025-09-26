//! Common test utilities for s3heap integration tests.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use s3heap::{Error, HeapScheduler, Triggerable};

/// Mock implementation of HeapScheduler for testing.
#[derive(Clone)]
pub struct MockHeapScheduler {
    #[allow(clippy::type_complexity)]
    done_items: Arc<Mutex<HashMap<(Uuid, String, Uuid), bool>>>,
    #[allow(clippy::type_complexity)]
    next_times: Arc<Mutex<HashMap<(Uuid, String), Option<(DateTime<Utc>, Uuid)>>>>,
}

impl MockHeapScheduler {
    pub fn new() -> Self {
        Self {
            done_items: Arc::new(Mutex::new(HashMap::new())),
            next_times: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Configure whether an item is done.
    pub fn set_done(&self, item: &Triggerable, nonce: Uuid, done: bool) {
        let key = (item.uuid, item.name.clone(), nonce);
        self.done_items.lock().unwrap().insert(key, done);
    }

    /// Configure the next scheduled time for an item.
    pub fn set_next_time(&self, item: &Triggerable, when: Option<(DateTime<Utc>, Uuid)>) {
        let key = (item.uuid, item.name.clone());
        self.next_times.lock().unwrap().insert(key, when);
    }
}

impl Default for MockHeapScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl HeapScheduler for MockHeapScheduler {
    async fn is_done(&self, item: &Triggerable, nonce: Uuid) -> Result<bool, Error> {
        let key = (item.uuid, item.name.clone(), nonce);
        Ok(self
            .done_items
            .lock()
            .unwrap()
            .get(&key)
            .copied()
            .unwrap_or(false))
    }

    async fn next_time_and_nonce(
        &self,
        item: &Triggerable,
    ) -> Result<Option<(DateTime<Utc>, Uuid)>, Error> {
        let key = (item.uuid, item.name.clone());
        Ok(self.next_times.lock().unwrap().get(&key).cloned().flatten())
    }
}

/// Helper to create test triggerables with predictable UUIDs.
pub fn create_test_triggerable(index: u32, name: &str) -> Triggerable {
    let mut bytes = [0u8; 16];
    bytes[12..16].copy_from_slice(&index.to_be_bytes());
    Triggerable {
        uuid: Uuid::from_bytes(bytes),
        name: name.to_string(),
    }
}

/// Helper to create a test time at a specific minute offset from a base time.
pub fn test_time_at_minute_offset(base: DateTime<Utc>, minutes: i64) -> DateTime<Utc> {
    base + chrono::Duration::minutes(minutes)
}

/// Helper to generate a test nonce.
pub fn test_nonce(index: u32) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[0..4].copy_from_slice(&index.to_be_bytes());
    Uuid::from_bytes(bytes)
}
