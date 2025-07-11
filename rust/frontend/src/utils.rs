use chroma_types::MaximumLimitExceededError;

use crate::quota::{DefaultQuota, UsageType};

pub(crate) fn ensure_limit<T>(limit: Option<u32>) -> Result<u32, T>
where
    T: From<MaximumLimitExceededError>,
{
    // SAFETY(c-gamble): This is a safe cast because the default value is
    // `1000usize`, which is less than 2 ^ 32 - 1.
    let max_limit = UsageType::LimitValue.default_quota() as u32;
    match limit {
        Some(provided_limit) if provided_limit <= max_limit => Ok(provided_limit),
        Some(provided_limit) => Err(MaximumLimitExceededError {
            provided: provided_limit,
            max: max_limit,
        }
        .into()),
        None => Ok(max_limit),
    }
}
