use serde::{Deserialize, Serialize};

use crate::MeterEventData;

/// Core structure representing a single metering event.
///
/// Contains tenant and database identifiers, the related collection ID,
/// and the payload data implementing `MeterEventData`.
#[derive(Debug, Serialize, Deserialize)]
pub struct MeterEvent {
    /// Identifier for the tenant.
    pub tenant: String,
    /// Identifier for the database.
    pub database: String,
    /// UUID of the collection to which this event pertains.
    pub collection_id: String,
    /// The type of action being performed.
    pub action: Action,
    /// User-defined payload data for this event.
    #[serde(flatten)]
    pub data: Box<dyn MeterEventData>,
}

// NOTE(c-gamble): It would probably be a good idea to eventually decouple these
// types from the metering crate with an `Action` trait or something.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum Action {
    Read(ReadAction),
    Write(WriteAction),
    Fork,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "read_action")]
pub enum ReadAction {
    Count,
    Get,
    GetForDelete,
    Query,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "write_action")]
pub enum WriteAction {
    Add,
    Delete,
    Update,
    Upsert,
}
