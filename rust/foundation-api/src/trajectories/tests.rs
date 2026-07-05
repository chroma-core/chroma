use super::*;
use std::collections::BTreeMap;
use std::error::Error;
use std::io;

use super::chunkset::{push_chunkset, push_json_record, utf8_chunks};
use super::citations::{read_citations, write_citations};
use super::ids::{
    decode_fixed_base36_to_be_bytes, decode_index, encode_be_bytes_base36, encode_index, hex_lower,
    sha256_bytes,
};
use super::limits::{
    CALL_INDEX_WIDTH, CHUNK_INDEX_WIDTH, ENTRY_INDEX_WIDTH, ITEM_ID_WIDTH, TID_WIDTH,
};
use super::metadata::metadata_for_key;
use super::record_format::{
    collect_entry_records, collect_file_records, collect_finalization_records,
    load_one_from_documents, read_trajectory_header, trajectory_header_key, ChromaRecord,
    TrajectoryHeader,
};
use chroma::{
    client::{ChromaHttpClientOptions, ChromaRetryOptions},
    types::MetadataValue,
    ChromaCollection, ChromaHttpClient,
};
use chroma_types::Collection;
use httpmock::MockServer;
use proptest::prelude::*;
use serde_json::json;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn Error>>;

/// Build a boxed test failure with a plain message.
fn test_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::other(message.into()))
}

/// Build a collection handle backed by a mocked Chroma endpoint.
fn mocked_collection(server: &MockServer) -> ChromaCollection {
    let client = ChromaHttpClient::new(ChromaHttpClientOptions {
        endpoint: server.base_url().parse().unwrap(),
        tenant_id: Some("tenant".to_string()),
        database_name: Some("database".to_string()),
        retry_options: ChromaRetryOptions {
            max_retries: 0,
            ..Default::default()
        },
        ..Default::default()
    });
    ChromaCollection::from_collection_model(
        client,
        Collection {
            tenant: "tenant".to_string(),
            database: "database".to_string(),
            ..Default::default()
        },
    )
}

/// Build a mocked collection-scoped endpoint path.
fn collection_path(collection: &ChromaCollection, suffix: &str) -> String {
    format!(
        "/api/v2/tenants/tenant/databases/database/collections/{}/{}",
        collection.id(),
        suffix
    )
}

/// Construct the sample tool used by round-trip fixtures.
fn sample_tool() -> Tool {
    Tool {
        tool_schema: ToolSchema {
            name: "wiki_upsert_file".to_string(),
            description: "write a page".to_string(),
            parameters: json!({"type": "object"}),
            required: vec!["slug".to_string()],
            extra: BTreeMap::new(),
        },
        extra: BTreeMap::new(),
    }
}

/// Construct a representative generated trajectory fixture.
fn sample_file(id: Uuid) -> GenerateTrajectoryFile {
    let mut final_citations = BTreeMap::new();
    final_citations.insert("page-a".to_string(), json!(["source:1", "source:2"]));
    let mut categories_assigned = BTreeMap::new();
    categories_assigned.insert("page-a".to_string(), json!(["systems"]));
    let mut citation_extra = BTreeMap::new();
    citation_extra.insert("custom".to_string(), json!({"kept": true}));

    GenerateTrajectoryFile {
        batch_index: Some(1),
        batch_offset: Some(10),
        worker_id: Some("w01".to_string()),
        span: Some(Span::Text("span".to_string())),
        attempt_id: Some(2),
        deadlock_retries: Some(0),
        attempt_paths: Some(vec![json!("attempt")]),
        started_at: Some(StringOrNumber::String("2026-06-29T00:00:00Z".to_string())),
        duration_seconds: Some(1.25),
        status: Some("completed".to_string()),
        error: None,
        usage: Some(Usage {
            n_calls: Some(2),
            input_tokens: Some(100),
            output_tokens: Some(50),
            cache_read_tokens: None,
            cache_write_tokens: None,
            cost_usd: Some(0.01),
            cost_without_cache_usd: None,
            unknown_model_calls: None,
            models_seen: Some(vec!["model".to_string()]),
            extra: BTreeMap::new(),
        }),
        citations: Some(Citations {
            input_ids: vec!["source:1".to_string(), "source:2".to_string()],
            surfaced_page_ids: vec!["wiki:page-a".to_string()],
            read_page_ids: vec!["wiki:page-b".to_string()],
            final_citations,
            new_page_slugs: vec!["page-a".to_string()],
            updated_page_slugs: vec!["page-b".to_string()],
            categories_assigned,
            extra: citation_extra,
        }),
        final_todos: Some(vec![json!({"task": "done"})]),
        trajectory: Trajectory {
            id,
            actions_and_observations: vec![
                TrajectoryEntry::Action(Action {
                    tools: vec![sample_tool()],
                    params: vec![json!({"slug": "page-a"})],
                    sources: vec![Source::new("agent")],
                    reasoning: Some("because".to_string()),
                    reasoning_signature: Some("sig".to_string()),
                }),
                TrajectoryEntry::Observation(Observation {
                    observations: vec!["x".repeat(VALUE_MAX_BYTES + 64)],
                    sources: vec![Source::new("wiki")],
                    tool_metadata: vec![Some(ToolCallMetadata {
                        lock_handoff: None,
                        lock_waits: None,
                        skipped_due_to_handoff: None,
                        surfaced_page_ids: Some(vec!["wiki:page-a".to_string()]),
                        read_page_id: None,
                        page_id: None,
                        record_ids: None,
                        todos: None,
                        op: Some("added".to_string()),
                        slug: Some("page-a".to_string()),
                        source_ids: Some(vec!["source:1".to_string()]),
                        categories: Some(vec!["systems".to_string()]),
                        latest_raw_source_date: None,
                        extra: BTreeMap::new(),
                    })],
                }),
            ],
        },
        extra: BTreeMap::new(),
    }
}

/// Construct citations that exercise ordering, deduplication, and extras.
fn sample_citations_with_duplicates_and_conflicts() -> Citations {
    Citations {
        input_ids: vec![
            "source:b".to_string(),
            "source:a".to_string(),
            "source:b".to_string(),
        ],
        surfaced_page_ids: vec![
            "wiki:page-2".to_string(),
            "wiki:page-1".to_string(),
            "wiki:page-2".to_string(),
        ],
        read_page_ids: vec!["wiki:read-2".to_string(), "wiki:read-1".to_string()],
        final_citations: BTreeMap::from([
            ("page-a".to_string(), json!(["source:a"])),
            ("page-b".to_string(), json!(["source:b", "source:c"])),
        ]),
        new_page_slugs: vec![
            "page-new".to_string(),
            "page-both".to_string(),
            "page-new".to_string(),
        ],
        updated_page_slugs: vec!["page-updated".to_string(), "page-both".to_string()],
        categories_assigned: BTreeMap::from([
            ("page-a".to_string(), json!(["systems"])),
            ("page-b".to_string(), json!(["storage", "rust"])),
        ]),
        extra: BTreeMap::from([("custom".to_string(), json!({"kept": true}))]),
    }
}

#[test]
/// Verifies that UUIDs round-trip through trajectory id encoding.
fn uuid_tid_roundtrips() -> TestResult {
    let cases = [
        Uuid::nil(),
        Uuid::parse_str("00000000-0000-0000-0000-000000000001")?,
        Uuid::parse_str("12345678-1234-5678-1234-567812345678")?,
        Uuid::from_bytes([0xff; 16]),
    ];

    for uuid in cases {
        let tid = uuid_to_tid(uuid)?;
        assert_eq!(tid.len(), TID_WIDTH);
        assert_eq!(tid_to_uuid(&tid)?, uuid);
        assert_eq!(uuid_to_tid(tid_to_uuid(&tid)?)?, tid);
    }
    Ok(())
}

#[test]
/// Verifies known UUID boundary values encode to stable trajectory ids.
fn uuid_tid_known_boundaries_are_fixed_width_base36() -> TestResult {
    let cases = [
        (
            Uuid::nil(),
            "0000000000000000000000000",
            "00000000-0000-0000-0000-000000000000",
        ),
        (
            Uuid::from_bytes([0xff; 16]),
            "f5lxx1zz5pnorynqglhzmsp33",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ),
    ];

    for (uuid, tid, uuid_text) in cases {
        assert_eq!(uuid_to_tid(uuid)?, tid);
        let decoded_uuid = tid_to_uuid(tid)?;
        assert_eq!(decoded_uuid, uuid);
        assert_eq!(decoded_uuid.to_string(), uuid_text);
        assert_eq!(uuid_to_tid(decoded_uuid)?, tid);
    }
    Ok(())
}

#[test]
/// Verifies malformed trajectory ids are rejected before they become UUIDs.
fn tid_to_uuid_rejects_invalid_width_digits_and_overflow() {
    assert_eq!(
        tid_to_uuid("0").map_err(|err| err.to_string()),
        Err("invalid key: tid must be 25 bytes, got 1: 0".to_string())
    );
    assert_eq!(
        tid_to_uuid("000000000000000000000000Z").map_err(|err| err.to_string()),
        Err("invalid key: invalid lower-case base36 digit 'Z'".to_string())
    );
    assert_eq!(
        tid_to_uuid("zzzzzzzzzzzzzzzzzzzzzzzzz").map_err(|err| err.to_string()),
        Err("invalid key: base36 value overflows 16 bytes: zzzzzzzzzzzzzzzzzzzzzzzzz".to_string())
    );
}

#[test]
/// Verifies SHA-256 item-id encoding round-trips to the digest bytes.
fn sha256_known_vector_is_encoded_as_width_50_base36() -> TestResult {
    let cases = [
        (
            &b""[..],
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "5oaq0bjhj6un82wg98mgigso5q7qlhc63je4gw7ivixqqhkd3p",
        ),
        (
            &b"abc"[..],
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            "4nb7oofka9ml8nasnokxxc1unhmtpr1wxsuwhpd9km3vt5as31",
        ),
    ];

    for (input, digest_hex, encoded) in cases {
        let digest = sha256_bytes(input);
        assert_eq!(hex_lower(&digest), digest_hex);
        assert_eq!(sha256_base36(input)?, encoded);
        assert_eq!(
            decode_fixed_base36_to_be_bytes(encoded, digest.len())?,
            digest.to_vec()
        );
        assert_eq!(encode_be_bytes_base36(&digest, ITEM_ID_WIDTH)?, encoded);
    }
    Ok(())
}

#[test]
/// Verifies fixed-width index encoding across base36 digit boundaries.
fn fixed_width_index_encoding_covers_boundaries() -> TestResult {
    let cases = [
        (0usize, ENTRY_INDEX_WIDTH, "000000"),
        (1, ENTRY_INDEX_WIDTH, "000001"),
        (35, ENTRY_INDEX_WIDTH, "00000z"),
        (36, ENTRY_INDEX_WIDTH, "000010"),
        (2_176_782_335, ENTRY_INDEX_WIDTH, "zzzzzz"),
        (1_679_615, CALL_INDEX_WIDTH, "zzzz"),
        (60_466_175, CHUNK_INDEX_WIDTH, "zzzzz"),
    ];

    for (index, width, encoded) in cases {
        assert_eq!(encode_index(index, width)?, encoded);
        assert_eq!(decode_index(encoded, width)?, index);
        assert_eq!(encode_index(decode_index(encoded, width)?, width)?, encoded);
    }
    Ok(())
}

#[test]
/// Verifies index encoding rejects values and text outside the requested width.
fn fixed_width_index_encoding_rejects_out_of_range_values() {
    assert_eq!(
        encode_index(2_176_782_336, ENTRY_INDEX_WIDTH).map_err(|err| err.to_string()),
        Err("size limit exceeded: base36 value needs 7 digits, width is 6".to_string())
    );
    assert_eq!(
        decode_index("1000000", ENTRY_INDEX_WIDTH).map_err(|err| err.to_string()),
        Err("invalid key: index must be 6 bytes, got 7: 1000000".to_string())
    );
    assert_eq!(
        decode_index("00000Z", ENTRY_INDEX_WIDTH).map_err(|err| err.to_string()),
        Err("invalid key: invalid lower-case base36 digit 'Z'".to_string())
    );
}

#[test]
/// Verifies fixed-width base36 byte encoding round-trips big-endian bytes.
fn fixed_base36_byte_decoding_returns_big_endian_bytes() -> TestResult {
    let cases = [
        ("0000", 2usize, vec![0x00, 0x00], 4usize),
        ("0073", 2, vec![0x00, 0xff], 4),
        (
            "09ys742pps3qo",
            8,
            vec![0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0],
            13,
        ),
        ("f5lxx1zz5pnorynqglhzmsp33", 16, vec![0xff; 16], TID_WIDTH),
    ];

    for (text, output_len, bytes, width) in cases {
        assert_eq!(decode_fixed_base36_to_be_bytes(text, output_len)?, bytes);
        assert_eq!(encode_be_bytes_base36(&bytes, width)?, text);
        assert_eq!(
            decode_fixed_base36_to_be_bytes(&encode_be_bytes_base36(&bytes, width)?, output_len)?,
            bytes
        );
    }
    Ok(())
}

proptest! {
    /// Verifies base36 byte encoding round-trips arbitrary big-endian bytes.
    #[test]
    fn base36_byte_encoding_round_trips_arbitrary_big_endian_bytes(
        bytes in proptest::collection::vec(any::<u8>(), 0..=64),
    ) {
        // Because 36^2 > 256, two base36 digits per input byte is always
        // enough to represent the value. The zero-length input still needs one
        // output digit because the encoder represents zero as "0".
        let width = bytes.len().saturating_mul(2).max(1);

        let encoded = encode_be_bytes_base36(&bytes, width)
            .map_err(|err| TestCaseError::fail(err.to_string()))?;
        let decoded = decode_fixed_base36_to_be_bytes(&encoded, bytes.len())
            .map_err(|err| TestCaseError::fail(err.to_string()))?;

        prop_assert_eq!(decoded, bytes);
    }
}

#[test]
/// Verifies every one-byte base36 digit round-trips through encode and decode.
fn single_base36_digits_roundtrip() -> TestResult {
    for value in 0u8..=35 {
        let expected = if value < 10 {
            char::from(b'0' + value)
        } else {
            char::from(b'a' + value - 10)
        }
        .to_string();

        let encoded = encode_be_bytes_base36(&[value], 1)?;
        assert_eq!(encoded, expected);
        assert_eq!(decode_fixed_base36_to_be_bytes(&encoded, 1)?, vec![value]);
    }
    Ok(())
}

#[test]
/// Verifies base36 byte decoding rejects uppercase and overflow values.
fn fixed_base36_byte_decoding_rejects_invalid_text() {
    assert_eq!(
        decode_fixed_base36_to_be_bytes("Z", 1).map_err(|err| err.to_string()),
        Err("invalid key: invalid lower-case base36 digit 'Z'".to_string())
    );
    assert_eq!(
        decode_fixed_base36_to_be_bytes("zz", 1).map_err(|err| err.to_string()),
        Err("invalid key: base36 value overflows 1 bytes: zz".to_string())
    );
}

#[test]
/// Verifies citation records round-trip as a complete normalized object.
fn citations_roundtrip_deduplicates_orders_and_preserves_extra() -> TestResult {
    let tid = uuid_to_tid(Uuid::parse_str("00000000-0000-0000-0000-000000000007")?)?;
    let citations = sample_citations_with_duplicates_and_conflicts();
    let mut records = Vec::new();
    write_citations(&mut records, &tid, &citations)?;
    let documents = documents_from_records(&records);

    assert_eq!(
        serde_json::from_str::<serde_json::Value>(
            documents
                .get(&format!("gt/{tid}/citations/header"))
                .ok_or_else(|| test_error("missing citations header"))?
        )?,
        json!({
            "v": 1,
            "item_id_bits": 256,
            "next_order": 9,
            "counts": {
                "input_ids": 2,
                "surfaced_page_ids": 2,
                "read_page_ids": 2,
                "page_write_ops": 3,
                "final_citations": 2,
                "categories_assigned": 2
            },
            "has_extra": true
        })
    );
    assert_eq!(
        read_citations(&documents, &tid)?,
        json!({
            "input_ids": ["source:b", "source:a"],
            "surfaced_page_ids": ["wiki:page-2", "wiki:page-1"],
            "read_page_ids": ["wiki:read-2", "wiki:read-1"],
            "final_citations": {
                "page-a": ["source:a"],
                "page-b": ["source:b", "source:c"]
            },
            "new_page_slugs": ["page-new"],
            "updated_page_slugs": ["page-both", "page-updated"],
            "categories_assigned": {
                "page-a": ["systems"],
                "page-b": ["storage", "rust"]
            },
            "custom": {"kept": true}
        })
    );
    Ok(())
}

#[test]
/// Verifies empty citation records round-trip to the complete empty shape.
fn empty_citations_roundtrip_to_complete_empty_shape() -> TestResult {
    let tid = uuid_to_tid(Uuid::parse_str("00000000-0000-0000-0000-000000000008")?)?;
    let citations = Citations {
        input_ids: Vec::new(),
        surfaced_page_ids: Vec::new(),
        read_page_ids: Vec::new(),
        final_citations: BTreeMap::new(),
        new_page_slugs: Vec::new(),
        updated_page_slugs: Vec::new(),
        categories_assigned: BTreeMap::new(),
        extra: BTreeMap::new(),
    };
    let mut records = Vec::new();
    write_citations(&mut records, &tid, &citations)?;
    let documents = documents_from_records(&records);

    assert_eq!(
        read_citations(&documents, &tid)?,
        json!({
            "input_ids": [],
            "surfaced_page_ids": [],
            "read_page_ids": [],
            "final_citations": {},
            "new_page_slugs": [],
            "updated_page_slugs": [],
            "categories_assigned": {}
        })
    );
    Ok(())
}

#[test]
/// Verifies citation headers reject mismatched collection counts.
fn citations_reject_header_count_mismatch() -> TestResult {
    let tid = uuid_to_tid(Uuid::parse_str("00000000-0000-0000-0000-000000000009")?)?;
    let citations = Citations {
        input_ids: vec!["source:a".to_string()],
        surfaced_page_ids: Vec::new(),
        read_page_ids: Vec::new(),
        final_citations: BTreeMap::new(),
        new_page_slugs: Vec::new(),
        updated_page_slugs: Vec::new(),
        categories_assigned: BTreeMap::new(),
        extra: BTreeMap::new(),
    };
    let mut records = Vec::new();
    write_citations(&mut records, &tid, &citations)?;
    let mut documents = documents_from_records(&records);
    let header_key = format!("gt/{tid}/citations/header");
    let mut header: serde_json::Value = serde_json::from_str(
        documents
            .get(&header_key)
            .ok_or_else(|| test_error("missing citations header"))?,
    )?;
    header["counts"]["input_ids"] = json!(2);
    documents.insert(header_key, serde_json::to_string(&header)?);

    assert_eq!(
        read_citations(&documents, &tid).map_err(|err| err.to_string()),
        Err("invalid value: citations count mismatch for input_ids: expected 2, got 1".to_string())
    );
    Ok(())
}

#[test]
/// Verifies citation item ids must match their logical key hash.
fn citations_reject_item_id_mismatch() -> TestResult {
    let tid = uuid_to_tid(Uuid::parse_str("00000000-0000-0000-0000-00000000000a")?)?;
    let citations = Citations {
        input_ids: vec!["source:a".to_string()],
        surfaced_page_ids: Vec::new(),
        read_page_ids: Vec::new(),
        final_citations: BTreeMap::new(),
        new_page_slugs: Vec::new(),
        updated_page_slugs: Vec::new(),
        categories_assigned: BTreeMap::new(),
        extra: BTreeMap::new(),
    };
    let mut records = Vec::new();
    write_citations(&mut records, &tid, &citations)?;
    let mut documents = documents_from_records(&records);
    let correct_item_id = sha256_base36(b"source:a")?;
    let wrong_item_id = "0".repeat(ITEM_ID_WIDTH);
    rename_document_prefix(
        &mut documents,
        &format!("gt/{tid}/citations/input_ids/{correct_item_id}"),
        &format!("gt/{tid}/citations/input_ids/{wrong_item_id}"),
    )?;

    assert_eq!(
        read_citations(&documents, &tid).map_err(|err| err.to_string()),
        Err(format!(
            "invalid key: citation item id {wrong_item_id} does not match expected {correct_item_id}"
        ))
    );
    Ok(())
}

#[test]
/// Verifies citation extras cannot replace known citation fields.
fn citations_reject_extra_field_collision() -> TestResult {
    let tid = uuid_to_tid(Uuid::parse_str("00000000-0000-0000-0000-00000000000b")?)?;
    let citations = Citations {
        input_ids: Vec::new(),
        surfaced_page_ids: Vec::new(),
        read_page_ids: Vec::new(),
        final_citations: BTreeMap::new(),
        new_page_slugs: Vec::new(),
        updated_page_slugs: Vec::new(),
        categories_assigned: BTreeMap::new(),
        extra: BTreeMap::from([("input_ids".to_string(), json!(["shadow"]))]),
    };
    let mut records = Vec::new();
    write_citations(&mut records, &tid, &citations)?;
    let documents = documents_from_records(&records);

    assert_eq!(
        read_citations(&documents, &tid).map_err(|err| err.to_string()),
        Err(
            "invalid value: citation extra field \"input_ids\" collides with a known field"
                .to_string()
        )
    );
    Ok(())
}

#[test]
/// Verifies unsupported page-write operations are rejected on read.
fn citations_reject_unsupported_page_write_operation() -> TestResult {
    let tid = uuid_to_tid(Uuid::parse_str("00000000-0000-0000-0000-00000000000c")?)?;
    let citations = Citations {
        input_ids: Vec::new(),
        surfaced_page_ids: Vec::new(),
        read_page_ids: Vec::new(),
        final_citations: BTreeMap::new(),
        new_page_slugs: vec!["page-a".to_string()],
        updated_page_slugs: Vec::new(),
        categories_assigned: BTreeMap::new(),
        extra: BTreeMap::new(),
    };
    let mut records = Vec::new();
    write_citations(&mut records, &tid, &citations)?;
    let mut documents = documents_from_records(&records);
    let mut replacement_records = Vec::new();
    push_chunkset(
        &mut replacement_records,
        &format!(
            "gt/{tid}/citations/page_write_ops/{}",
            sha256_base36(b"page-a")?
        ),
        &json!({
            "slug": "page-a",
            "op": "deleted",
            "order": 0
        }),
    )?;
    insert_documents(&mut documents, replacement_records);

    assert_eq!(
        read_citations(&documents, &tid).map_err(|err| err.to_string()),
        Err("invalid value: unsupported page_write_ops op \"deleted\"".to_string())
    );
    Ok(())
}

#[test]
/// Verifies that finalized record serialization can be loaded losslessly.
fn save_and_load_finalized_file_roundtrips() -> TestResult {
    let file = sample_file(Uuid::parse_str("00000000-0000-0000-0000-000000000001")?);
    let records = records_for_file(&file, WriteState::Finalized)?;
    assert!(records
        .iter()
        .all(|record| record.document.len() <= VALUE_MAX_BYTES));
    assert!(records
        .iter()
        .all(|record| record.id.len() <= KEY_MAX_BYTES));
    assert!(records
        .iter()
        .any(|record| record.id.contains("/chunks/00001")));

    let documents = documents_from_records(&records);
    let loaded = load_one_from_documents(&documents, file.trajectory.id, true)?;
    assert_eq!(serde_json::to_value(&loaded)?, serde_json::to_value(&file)?);
    Ok(())
}

#[test]
/// Verifies that finalization writes the complete entry payload.
fn finalization_records_include_complete_entry_payload() -> TestResult {
    let file = sample_file(Uuid::parse_str("00000000-0000-0000-0000-000000000002")?);
    let tid = uuid_to_tid(file.trajectory.id)?;
    let mut header = TrajectoryHeader {
        v: 1,
        record_type: "GenerateTrajectoryFile".to_string(),
        tid: tid.clone(),
        entries: file.trajectory.actions_and_observations.len(),
        write_state: WriteState::Open,
        has_citations: false,
    };
    let mut records = Vec::new();
    collect_finalization_records(&mut records, &mut header, &file, &tid)?;

    assert!(records.iter().any(|record| {
        record
            .id
            .contains("/entries/000000/action/tools/0000/metadata")
    }));
    let documents = documents_from_records(&records);
    let loaded = load_one_from_documents(&documents, file.trajectory.id, true)?;
    assert_eq!(serde_json::to_value(loaded)?, serde_json::to_value(file)?);
    Ok(())
}

#[test]
/// Verifies that trajectory header metadata identifies trajectory records.
fn trajectory_header_has_trajectory_metadata() -> TestResult {
    let first = sample_file(Uuid::parse_str("00000000-0000-0000-0000-000000000001")?);
    let records = records_for_file(&first, WriteState::Finalized)?;
    let header_key = trajectory_header_key(first.trajectory.id)?;
    let header = records
        .iter()
        .find(|record| record.id == header_key)
        .ok_or_else(|| test_error(format!("missing header record {header_key}")))?;
    assert_eq!(
        header.metadata.get("record_kind"),
        Some(&MetadataValue::Str("trajectory_header".to_string()))
    );
    assert_eq!(
        header.metadata.get("tid"),
        Some(&MetadataValue::Str(first.trajectory.id.to_string()))
    );
    Ok(())
}

#[test]
/// Verifies open trajectories can accept entries before finalization.
fn open_trajectory_records_can_be_extended_and_finalized() -> TestResult {
    let mut file = sample_file(Uuid::parse_str("00000000-0000-0000-0000-000000000003")?);
    let entries = file.trajectory.actions_and_observations.clone();
    file.trajectory.actions_and_observations.clear();
    file.duration_seconds = None;
    file.status = None;
    file.usage = None;
    file.final_todos = None;
    file.citations = None;

    let records = records_for_file(&file, WriteState::Open)?;
    let mut documents = documents_from_records(&records);
    assert!(matches!(
        load_one_from_documents(&documents, file.trajectory.id, true),
        Err(TrajectoryError::FinalizedRequired { .. })
    ));

    let tid = uuid_to_tid(file.trajectory.id)?;
    let mut header = read_trajectory_header(&documents, file.trajectory.id)?;
    let mut append_records = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        collect_entry_records(&mut append_records, &tid, index, entry)?;
    }
    header.entries = entries.len();
    push_json_record(
        &mut append_records,
        &format!("gt/{tid}/header"),
        &header,
        VALUE_MAX_BYTES,
    )?;
    insert_documents(&mut documents, append_records);

    let open_loaded = load_one_from_documents(&documents, file.trajectory.id, false)?;
    assert_eq!(open_loaded.trajectory.actions_and_observations.len(), 2);

    let finalized = sample_file(file.trajectory.id);
    let mut finalize_records = Vec::new();
    collect_finalization_records(
        &mut finalize_records,
        &mut header,
        &finalized,
        &uuid_to_tid(finalized.trajectory.id)?,
    )?;
    insert_documents(&mut documents, finalize_records);

    let finalized_loaded = load_one_from_documents(&documents, finalized.trajectory.id, true)?;
    assert_eq!(
        serde_json::to_value(finalized_loaded)?,
        serde_json::to_value(finalized)?
    );
    Ok(())
}

#[tokio::test]
/// Verifies transactional open-trajectory adds first prove ids absent.
async fn create_open_trajectory_reads_absence_before_add() -> TestResult {
    let server = MockServer::start_async().await;
    let collection = mocked_collection(&server);
    let mut file = sample_file(Uuid::parse_str("00000000-0000-0000-0000-000000000009")?);
    file.trajectory.actions_and_observations.clear();
    file.duration_seconds = None;
    file.status = None;
    file.error = None;
    file.usage = None;
    file.citations = None;
    file.final_todos = None;

    let tid = uuid_to_tid(file.trajectory.id)?;
    let root_key = format!("gt/{tid}/root_metadata");
    let header_key = trajectory_header_key(file.trajectory.id)?;
    let get_path = collection_path(&collection, "conditional/get");
    let commit_path = collection_path(&collection, "conditional/commit");

    let get_mock = server
        .mock_async(|when, then| {
            when.method("POST").path(get_path).json_body(json!({
                "ids": [root_key, header_key],
                "where": null,
                "where_document": null,
                "limit": null,
                "offset": 0,
                "include": [],
                "read_token": null,
            }));
            then.status(200).json_body(json!({
                "ids": [],
                "embeddings": null,
                "documents": null,
                "uris": null,
                "metadatas": null,
                "include": [],
                "read_token": 42,
            }));
        })
        .await;
    let commit_mock = server
        .mock_async(|when, then| {
            when.method("POST").path(commit_path);
            then.status(200).json_body(json!({
                "first_inserted_record_offset": 7,
                "record_count": 2,
            }));
        })
        .await;

    let mut txn = collection.conditional();
    chroma_create_open_trajectory(&mut txn, &file).await?;
    let result = txn.commit().await?;

    assert_eq!(result.record_count, 2);
    assert_eq!(get_mock.calls(), 1);
    assert_eq!(commit_mock.calls(), 1);
    Ok(())
}

#[test]
/// Verifies action and observation vectors must remain parallel.
fn unequal_parallel_vectors_are_rejected() -> TestResult {
    let mut file = sample_file(Uuid::parse_str("00000000-0000-0000-0000-000000000004")?);
    let TrajectoryEntry::Action(action) = &mut file.trajectory.actions_and_observations[0] else {
        return Err(test_error("expected action entry"));
    };
    action.sources.clear();
    let mut records = Vec::new();
    let error = match collect_file_records(&mut records, &file, WriteState::Finalized) {
        Ok(()) => return Err(test_error("expected parallel vector validation error")),
        Err(error) => error,
    };
    assert!(matches!(error, TrajectoryError::InvalidValue(_)));
    Ok(())
}

#[test]
/// Verifies chunkset digest mismatches are rejected during load.
fn chunk_hash_mismatch_is_rejected() -> TestResult {
    let file = sample_file(Uuid::parse_str("00000000-0000-0000-0000-000000000005")?);
    let records = records_for_file(&file, WriteState::Finalized)?;
    let mut documents = documents_from_records(&records);
    let chunk_key = documents
        .keys()
        .find(|key| key.ends_with("/chunks/00000"))
        .cloned()
        .ok_or_else(|| test_error("missing chunk record"))?;
    let chunk = documents
        .get_mut(&chunk_key)
        .ok_or_else(|| test_error(format!("missing chunk record {chunk_key}")))?;
    let replacement = if chunk.starts_with('0') { "1" } else { "0" };
    chunk.replace_range(0..1, replacement);
    let error = match load_one_from_documents(&documents, file.trajectory.id, true) {
        Ok(_) => return Err(test_error("expected chunk hash mismatch")),
        Err(error) => error,
    };
    assert!(matches!(error, TrajectoryError::HashMismatch { .. }));
    Ok(())
}

#[test]
/// Verifies metadata extraction for nested entry chunk keys.
fn metadata_for_entry_chunk_is_queryable() -> TestResult {
    let tid = uuid_to_tid(Uuid::parse_str("00000000-0000-0000-0000-000000000006")?)?;
    let metadata = metadata_for_key(&format!(
        "gt/{tid}/entries/000001/observation/tool_metadata/0002/chunks/00003"
    ))?;
    assert_eq!(
        metadata.get("subtree"),
        Some(&MetadataValue::Str("entries".to_string()))
    );
    assert_eq!(metadata.get("entry"), Some(&MetadataValue::Int(1)));
    assert_eq!(
        metadata.get("entry_kind"),
        Some(&MetadataValue::Str("observation".to_string()))
    );
    assert_eq!(
        metadata.get("field"),
        Some(&MetadataValue::Str("tool_metadata".to_string()))
    );
    assert_eq!(metadata.get("call"), Some(&MetadataValue::Int(2)));
    assert_eq!(metadata.get("chunk"), Some(&MetadataValue::Int(3)));
    Ok(())
}

#[test]
/// Verifies chunking never splits inside a UTF-8 scalar value.
fn chunking_keeps_utf8_boundaries() -> TestResult {
    let chunks = utf8_chunks("aé日".as_bytes(), 3)?;
    assert_eq!(
        chunks
            .into_iter()
            .map(std::str::from_utf8)
            .collect::<Result<Vec<_>, _>>()?,
        vec!["aé", "日"]
    );
    Ok(())
}

/// Serialize a fixture into Chroma records with the requested write state.
fn records_for_file(
    file: &GenerateTrajectoryFile,
    write_state: WriteState,
) -> Result<Vec<ChromaRecord>, TrajectoryError> {
    let mut records = Vec::new();
    collect_file_records(&mut records, file, write_state)?;
    Ok(records)
}

/// Build a document map from generated Chroma records.
fn documents_from_records(records: &[ChromaRecord]) -> BTreeMap<String, String> {
    records
        .iter()
        .map(|record| (record.id.clone(), record.document.clone()))
        .collect()
}

/// Insert generated records into an existing document map.
fn insert_documents(documents: &mut BTreeMap<String, String>, records: Vec<ChromaRecord>) {
    for record in records {
        documents.insert(record.id, record.document);
    }
}

/// Rename every document key under one prefix to another prefix.
fn rename_document_prefix(
    documents: &mut BTreeMap<String, String>,
    old_prefix: &str,
    new_prefix: &str,
) -> TestResult {
    let keys = documents
        .keys()
        .filter(|key| key.starts_with(old_prefix))
        .cloned()
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return Err(test_error(format!("missing document prefix {old_prefix}")));
    }

    for key in keys {
        let value = documents
            .remove(&key)
            .ok_or_else(|| test_error(format!("missing document key {key}")))?;
        let suffix = key
            .strip_prefix(old_prefix)
            .ok_or_else(|| test_error(format!("document key {key} is outside {old_prefix}")))?;
        documents.insert(format!("{new_prefix}{suffix}"), value);
    }
    Ok(())
}
