use std::ops::Deref;

// mirrors chromadb/utils/messageid.py
use num_bigint::BigInt;
use pulsar::{consumer::data::MessageData, proto::MessageIdData};

use crate::types::SeqId;

pub(crate) struct PulsarMessageIdWrapper(pub(crate) MessageData);

impl Deref for PulsarMessageIdWrapper {
    type Target = MessageIdData;

    fn deref(&self) -> &Self::Target {
        &self.0.id
    }
}

pub(crate) fn pulsar_to_int(message_id: PulsarMessageIdWrapper) -> SeqId {
    let ledger_id = message_id.ledger_id;
    let entry_id = message_id.entry_id;
    let batch_index = message_id.batch_index.unwrap_or(0);
    let partition = message_id.partition.unwrap_or(0);

    let mut ledger_id = BigInt::from(ledger_id);
    let mut entry_id = BigInt::from(entry_id);
    let mut batch_index = BigInt::from(batch_index);
    let mut partition = BigInt::from(partition);

    // Convert to offset binary encoding to preserve ordering semantics when encoded
    // see https://en.wikipedia.org/wiki/Offset_binary
    ledger_id = ledger_id + BigInt::from(2).pow(63);
    entry_id = entry_id + BigInt::from(2).pow(63);
    batch_index = batch_index + BigInt::from(2).pow(31);
    partition = partition + BigInt::from(2).pow(31);

    let res = ledger_id << 128 | entry_id << 96 | batch_index << 64 | partition;
    res
}

// We can't use From because we don't own the type
// So the pattern is to wrap it in a newtype and implement TryFrom for that
// And implement Dereference for the newtype to the underlying type
impl From<PulsarMessageIdWrapper> for SeqId {
    fn from(message_id: PulsarMessageIdWrapper) -> Self {
        return pulsar_to_int(message_id);
    }
}
