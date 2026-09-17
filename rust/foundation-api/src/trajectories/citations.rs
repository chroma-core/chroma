#![allow(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::chunkset::{
    direct_chunkset_bases, push_chunkset, push_json_record, read_chunkset, read_json,
};
use super::error::TrajectoryError;
use super::ids::sha256_base36;
use super::limits::{ITEM_ID_WIDTH, VALUE_MAX_BYTES};
use super::model::Citations;
use super::record_format::ChromaRecord;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
struct CitationCounts {
    input_ids: usize,
    surfaced_page_ids: usize,
    read_page_ids: usize,
    final_citations: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CitationsHeader {
    v: u8,
    item_id_bits: usize,
    next_order: usize,
    counts: CitationCounts,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct OrderedCitationItem {
    value: String,
    order: usize,
}

/// Write the complete pruned citation subtree for a trajectory.
pub(crate) fn write_citations(
    records: &mut Vec<ChromaRecord>,
    tid: &str,
    citations: &Citations,
) -> Result<(), TrajectoryError> {
    let mut counts = CitationCounts::default();
    let mut next_order = 0usize;

    counts.input_ids = write_ordered_citation_collection(
        records,
        tid,
        "input_ids",
        &citations.input_ids,
        &mut next_order,
    )?;
    counts.surfaced_page_ids = write_ordered_citation_collection(
        records,
        tid,
        "surfaced_page_ids",
        &citations.surfaced_page_ids,
        &mut next_order,
    )?;
    counts.read_page_ids = write_ordered_citation_collection(
        records,
        tid,
        "read_page_ids",
        &citations.read_page_ids,
        &mut next_order,
    )?;
    counts.final_citations = write_slug_value_collection(
        records,
        tid,
        "final_citations",
        "source_ids",
        &citations.final_citations,
    )?;

    let header = CitationsHeader {
        v: 1,
        item_id_bits: 256,
        next_order,
        counts,
    };
    push_json_record(
        records,
        &format!("gt/{tid}/citations/header"),
        &header,
        VALUE_MAX_BYTES,
    )?;
    Ok(())
}

/// Write a deduplicated ordered string citation collection.
fn write_ordered_citation_collection(
    records: &mut Vec<ChromaRecord>,
    tid: &str,
    collection: &str,
    values: &[String],
    next_order: &mut usize,
) -> Result<usize, TrajectoryError> {
    let mut seen = BTreeSet::new();
    let mut count = 0usize;
    for value in values {
        if !seen.insert(value.clone()) {
            continue;
        }
        let item_id = sha256_base36(value.as_bytes())?;
        let item = serde_json::json!({
            "value": value,
            "order": *next_order,
        });
        *next_order = next_order
            .checked_add(1)
            .ok_or_else(|| TrajectoryError::InvalidValue("citation order overflow".to_string()))?;
        count = count
            .checked_add(1)
            .ok_or_else(|| TrajectoryError::InvalidValue("citation count overflow".to_string()))?;
        push_chunkset(
            records,
            &format!("gt/{tid}/citations/{collection}/{item_id}"),
            &item,
        )?;
    }
    Ok(count)
}

/// Write a map keyed by page slug into stable hash-addressed citation records.
fn write_slug_value_collection(
    records: &mut Vec<ChromaRecord>,
    tid: &str,
    collection: &str,
    value_field: &str,
    values: &BTreeMap<String, Value>,
) -> Result<usize, TrajectoryError> {
    for (slug, value) in values {
        let item_id = sha256_base36(slug.as_bytes())?;
        let mut item = Map::new();
        item.insert("slug".to_string(), Value::String(slug.clone()));
        item.insert(value_field.to_string(), value.clone());
        push_chunkset(
            records,
            &format!("gt/{tid}/citations/{collection}/{item_id}"),
            &Value::Object(item),
        )?;
    }
    Ok(values.len())
}

/// Rehydrate the complete pruned citation object from its persisted subtree.
pub(crate) fn read_citations(
    documents: &BTreeMap<String, String>,
    tid: &str,
) -> Result<Citations, TrajectoryError> {
    let header_key = format!("gt/{tid}/citations/header");
    let header: CitationsHeader = read_json(documents, &header_key)?;
    if header.v != 1 {
        return Err(TrajectoryError::InvalidValue(format!(
            "unsupported citations header version {}",
            header.v
        )));
    }
    if header.item_id_bits != 256 {
        return Err(TrajectoryError::InvalidValue(format!(
            "unsupported citations item id bits {}",
            header.item_id_bits
        )));
    }

    let input_ids = read_ordered_citation_collection(documents, tid, "input_ids")?;
    let surfaced_page_ids = read_ordered_citation_collection(documents, tid, "surfaced_page_ids")?;
    let read_page_ids = read_ordered_citation_collection(documents, tid, "read_page_ids")?;
    let final_citations =
        read_slug_value_collection(documents, tid, "final_citations", "source_ids")?;

    validate_citation_count("input_ids", header.counts.input_ids, input_ids.len())?;
    validate_citation_count(
        "surfaced_page_ids",
        header.counts.surfaced_page_ids,
        surfaced_page_ids.len(),
    )?;
    validate_citation_count(
        "read_page_ids",
        header.counts.read_page_ids,
        read_page_ids.len(),
    )?;
    validate_citation_count(
        "final_citations",
        header.counts.final_citations,
        final_citations.len(),
    )?;

    Ok(Citations {
        input_ids,
        surfaced_page_ids,
        read_page_ids,
        final_citations,
    })
}

/// Check that a citation collection count matches its header count.
fn validate_citation_count(
    name: &str,
    expected: usize,
    actual: usize,
) -> Result<(), TrajectoryError> {
    if expected != actual {
        return Err(TrajectoryError::InvalidValue(format!(
            "citations count mismatch for {name}: expected {expected}, got {actual}"
        )));
    }
    Ok(())
}

/// Read an ordered string citation collection and restore producer order.
fn read_ordered_citation_collection(
    documents: &BTreeMap<String, String>,
    tid: &str,
    collection: &str,
) -> Result<Vec<String>, TrajectoryError> {
    let prefix = format!("gt/{tid}/citations/{collection}/");
    let mut items = Vec::new();
    for base in direct_chunkset_bases(documents, &prefix) {
        let item_id = base
            .strip_prefix(&prefix)
            .ok_or_else(|| TrajectoryError::InvalidKey(base.clone()))?;
        let value: OrderedCitationItem = serde_json::from_value(read_chunkset(documents, &base)?)?;
        validate_item_id(item_id, value.value.as_bytes())?;
        items.push((value.order, value.value));
    }
    items.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    Ok(items.into_iter().map(|(_, value)| value).collect())
}

/// Read a slug-keyed citation collection into a JSON object.
fn read_slug_value_collection(
    documents: &BTreeMap<String, String>,
    tid: &str,
    collection: &str,
    value_field: &str,
) -> Result<BTreeMap<String, Value>, TrajectoryError> {
    let prefix = format!("gt/{tid}/citations/{collection}/");
    let mut values = BTreeMap::new();
    for base in direct_chunkset_bases(documents, &prefix) {
        let item_id = base
            .strip_prefix(&prefix)
            .ok_or_else(|| TrajectoryError::InvalidKey(base.clone()))?;
        let item = read_chunkset(documents, &base)?;
        let Value::Object(mut object) = item else {
            return Err(TrajectoryError::InvalidValue(format!(
                "{base} must contain an object"
            )));
        };
        let slug = object
            .remove("slug")
            .and_then(|value| value.as_str().map(str::to_string))
            .ok_or_else(|| TrajectoryError::InvalidValue(format!("{base} missing string slug")))?;
        validate_item_id(item_id, slug.as_bytes())?;
        let value = object.remove(value_field).ok_or_else(|| {
            TrajectoryError::InvalidValue(format!("{base} missing {value_field}"))
        })?;
        values.insert(slug, value);
    }
    Ok(values)
}

/// Validate that a citation item id is the expected hash of its logical key.
fn validate_item_id(item_id: &str, bytes: &[u8]) -> Result<(), TrajectoryError> {
    if item_id.len() != ITEM_ID_WIDTH {
        return Err(TrajectoryError::InvalidKey(format!(
            "citation item id must be {ITEM_ID_WIDTH} bytes, got {}: {item_id}",
            item_id.len()
        )));
    }
    let expected = sha256_base36(bytes)?;
    if item_id != expected {
        return Err(TrajectoryError::InvalidKey(format!(
            "citation item id {item_id} does not match expected {expected}"
        )));
    }
    Ok(())
}
