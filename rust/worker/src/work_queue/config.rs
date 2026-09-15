use serde::{Deserialize, Serialize};
use std::time::Duration;

use mdac::TokenBucket;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct WorkQueueConfig {
    pub storage_path: String,
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub get_work_rate_limit: Option<GetWorkRateLimitConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct PersistenceConfig {
    pub time_threshold_seconds: u64,
    pub pending_threshold: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetWorkRateLimitConfig {
    /// Maximum number of work items that may be returned in one burst.
    pub capacity: u32,
    /// Nanoseconds required to replenish one work-item token.
    pub interval_ns: u64,
}

impl GetWorkRateLimitConfig {
    pub fn try_token_bucket(&self) -> Result<TokenBucket, String> {
        if self.capacity == 0
            || self.interval_ns == 0
            || self
                .interval_ns
                .checked_mul(u64::from(self.capacity))
                .is_none()
        {
            return Err(
                "capacity and interval_ns must be positive and their product must fit in u64"
                    .to_string(),
            );
        }

        Ok(TokenBucket::new(
            self.capacity,
            Duration::from_nanos(self.interval_ns),
        ))
    }
}

impl Default for WorkQueueConfig {
    fn default() -> Self {
        Self {
            storage_path: "work-queue/queue.parquet".to_string(),
            persistence: PersistenceConfig {
                time_threshold_seconds: 2,
                pending_threshold: 100,
            },
            get_work_rate_limit: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_get_work_rate_limit() {
        let config: WorkQueueConfig = serde_json::from_value(serde_json::json!({
            "storage_path": "queue.parquet",
            "persistence": {
                "time_threshold_seconds": 2,
                "pending_threshold": 100
            },
            "get_work_rate_limit": {
                "capacity": 2,
                "interval_ns": 1_000_000_000u64
            }
        }))
        .unwrap();

        let limiter = config
            .get_work_rate_limit
            .unwrap()
            .try_token_bucket()
            .unwrap();
        assert!(limiter.drain(2));
        assert!(!limiter.drain(1));
    }

    #[test]
    fn get_work_rate_limit_is_optional() {
        let config: WorkQueueConfig = serde_json::from_value(serde_json::json!({
            "storage_path": "queue.parquet",
            "persistence": {
                "time_threshold_seconds": 2,
                "pending_threshold": 100
            }
        }))
        .unwrap();

        assert!(config.get_work_rate_limit.is_none());
    }

    #[test]
    fn reject_invalid_get_work_rate_limits() {
        for config in [
            GetWorkRateLimitConfig {
                capacity: 0,
                interval_ns: 1,
            },
            GetWorkRateLimitConfig {
                capacity: 1,
                interval_ns: 0,
            },
            GetWorkRateLimitConfig {
                capacity: 2,
                interval_ns: u64::MAX,
            },
        ] {
            assert!(config.try_token_bucket().is_err());
        }
    }
}
