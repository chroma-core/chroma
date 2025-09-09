use serde::Serialize;
use std::time::Duration;

pub fn deserialize_duration_from_seconds<'de, D>(d: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let secs: u64 = serde::Deserialize::deserialize(d)?;
    Ok(Duration::from_secs(secs))
}

pub fn serialize_duration_to_seconds<S>(duration: &Duration, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let secs = duration.as_secs();
    secs.serialize(s)
}
