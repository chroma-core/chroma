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
    page_write_ops: usize,
    final_citations: usize,
    categories_assigned: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CitationsHeader {
    v: u8,
    item_id_bits: usize,
    next_order: usize,
    counts: CitationCounts,
    has_extra: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct OrderedCitationItem {
    value: String,
    order: usize,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct PageWriteOpItem {
    slug: String,
    op: String,
    order: usize,
}

/// Write the complete citation subtree for a trajectory.
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
    counts.page_write_ops = write_page_write_ops(records, tid, citations, &mut next_order)?;
    counts.final_citations = write_slug_value_collection(
        records,
        tid,
        "final_citations",
        "source_ids",
        &citations.final_citations,
    )?;
    counts.categories_assigned = write_slug_value_collection(
        records,
        tid,
        "categories_assigned",
        "categories",
        &citations.categories_assigned,
    )?;

    let has_extra = !citations.extra.is_empty();
    if has_extra {
        let mut extra = Map::new();
        for (key, value) in &citations.extra {
            extra.insert(key.clone(), value.clone());
        }
        push_chunkset(
            records,
            &format!("gt/{tid}/citations/extra"),
            &Value::Object(extra),
        )?;
    }

    let header = CitationsHeader {
        v: 1,
        item_id_bits: 256,
        next_order,
        counts,
        has_extra,
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

/// Write page creation/update operations with one final operation per slug.
fn write_page_write_ops(
    records: &mut Vec<ChromaRecord>,
    tid: &str,
    citations: &Citations,
    next_order: &mut usize,
) -> Result<usize, TrajectoryError> {
    let mut ops: BTreeMap<String, (String, usize)> = BTreeMap::new();
    for slug in &citations.new_page_slugs {
        upsert_page_write_op(&mut ops, slug, "added", next_order)?;
    }
    for slug in &citations.updated_page_slugs {
        upsert_page_write_op(&mut ops, slug, "updated", next_order)?;
    }

    for (slug, (op, order)) in &ops {
        let item_id = sha256_base36(slug.as_bytes())?;
        let item = serde_json::json!({
            "slug": slug,
            "op": op,
            "order": order,
        });
        push_chunkset(
            records,
            &format!("gt/{tid}/citations/page_write_ops/{item_id}"),
            &item,
        )?;
    }

    Ok(ops.len())
}

/// Insert or replace the operation recorded for one page slug.
fn upsert_page_write_op(
    ops: &mut BTreeMap<String, (String, usize)>,
    slug: &str,
    op: &str,
    next_order: &mut usize,
) -> Result<(), TrajectoryError> {
    if let Some((existing_op, _)) = ops.get_mut(slug) {
        *existing_op = op.to_string();
        return Ok(());
    }
    let order = *next_order;
    *next_order = next_order
        .checked_add(1)
        .ok_or_else(|| TrajectoryError::InvalidValue("citation order overflow".to_string()))?;
    ops.insert(slug.to_string(), (op.to_string(), order));
    Ok(())
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

/// Rehydrate the complete citation object from its persisted subtree.
pub(crate) fn read_citations(
    documents: &BTreeMap<String, String>,
    tid: &str,
) -> Result<Value, TrajectoryError> {
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
    let page_write_ops = read_page_write_ops(documents, tid)?;
    let final_citations =
        read_slug_value_collection(documents, tid, "final_citations", "source_ids")?;
    let categories_assigned =
        read_slug_value_collection(documents, tid, "categories_assigned", "categories")?;

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
        "page_write_ops",
        header.counts.page_write_ops,
        page_write_ops.len(),
    )?;
    validate_citation_count(
        "final_citations",
        header.counts.final_citations,
        final_citations.len(),
    )?;
    validate_citation_count(
        "categories_assigned",
        header.counts.categories_assigned,
        categories_assigned.len(),
    )?;

    let mut new_page_slugs = Vec::new();
    let mut updated_page_slugs = Vec::new();
    let mut sorted_ops = page_write_ops;
    sorted_ops.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    for (_, slug, op) in sorted_ops {
        match op.as_str() {
            "added" => new_page_slugs.push(Value::String(slug)),
            "updated" => updated_page_slugs.push(Value::String(slug)),
            other => {
                return Err(TrajectoryError::InvalidValue(format!(
                    "unsupported page_write_ops op {other:?}"
                )));
            }
        }
    }

    let mut object = Map::new();
    object.insert(
        "input_ids".to_string(),
        Value::Array(input_ids.into_iter().map(Value::String).collect()),
    );
    object.insert(
        "surfaced_page_ids".to_string(),
        Value::Array(surfaced_page_ids.into_iter().map(Value::String).collect()),
    );
    object.insert(
        "read_page_ids".to_string(),
        Value::Array(read_page_ids.into_iter().map(Value::String).collect()),
    );
    object.insert(
        "final_citations".to_string(),
        Value::Object(final_citations),
    );
    object.insert("new_page_slugs".to_string(), Value::Array(new_page_slugs));
    object.insert(
        "updated_page_slugs".to_string(),
        Value::Array(updated_page_slugs),
    );
    object.insert(
        "categories_assigned".to_string(),
        Value::Object(categories_assigned),
    );

    if header.has_extra {
        let extra = read_chunkset(documents, &format!("gt/{tid}/citations/extra"))?;
        let Value::Object(extra) = extra else {
            return Err(TrajectoryError::InvalidValue(format!(
                "gt/{tid}/citations/extra must be an object"
            )));
        };
        for (key, value) in extra {
            if object.contains_key(&key) {
                return Err(TrajectoryError::InvalidValue(format!(
                    "citation extra field {key:?} collides with a known field"
                )));
            }
            object.insert(key, value);
        }
    }

    Ok(Value::Object(object))
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

/// Read the stored page write operations for one trajectory.
fn read_page_write_ops(
    documents: &BTreeMap<String, String>,
    tid: &str,
) -> Result<Vec<(usize, String, String)>, TrajectoryError> {
    let collection = "page_write_ops";
    let prefix = format!("gt/{tid}/citations/{collection}/");
    let mut items = Vec::new();
    for base in direct_chunkset_bases(documents, &prefix) {
        let item_id = base
            .strip_prefix(&prefix)
            .ok_or_else(|| TrajectoryError::InvalidKey(base.clone()))?;
        let value: PageWriteOpItem = serde_json::from_value(read_chunkset(documents, &base)?)?;
        validate_item_id(item_id, value.slug.as_bytes())?;
        items.push((value.order, value.slug, value.op));
    }
    Ok(items)
}

/// Read a slug-keyed citation collection into a JSON object.
fn read_slug_value_collection(
    documents: &BTreeMap<String, String>,
    tid: &str,
    collection: &str,
    value_field: &str,
) -> Result<Map<String, Value>, TrajectoryError> {
    let prefix = format!("gt/{tid}/citations/{collection}/");
    let mut values = Map::new();
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
