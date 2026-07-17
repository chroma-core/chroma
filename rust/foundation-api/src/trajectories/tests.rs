use super::*;
use std::collections::BTreeMap;
use std::error::Error;
use std::io;

use super::chunkset::{push_json_record, utf8_chunks};
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
use serde_json::{json, Value};
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

/// Construct a pruned reasoning entry.
fn reasoning_entry(reasoning: Option<String>, writes: &[&str]) -> ReasoningEntry {
    ReasoningEntry {
        reasoning,
        writes: writes
            .iter()
            .map(|slug| ReasoningWrite {
                slug: (*slug).to_string(),
            })
            .collect(),
    }
}

/// Construct citation attribution that exercises ordering and deduplication.
fn sample_citations_with_duplicates() -> Citations {
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
    }
}

/// Construct a representative pruned reasoning trajectory fixture.
fn sample_file(id: Uuid) -> ReasoningTrajectoryFile {
    ReasoningTrajectoryFile {
        citations: Some(Citations {
            input_ids: vec!["source:b".to_string(), "source:a".to_string()],
            surfaced_page_ids: vec!["wiki:page-2".to_string(), "wiki:page-1".to_string()],
            read_page_ids: vec!["wiki:read-2".to_string(), "wiki:read-1".to_string()],
            final_citations: BTreeMap::from([
                ("page-a".to_string(), json!(["source:a"])),
                ("page-b".to_string(), json!(["source:b", "source:c"])),
            ]),
        }),
        trajectory: ReasoningTrajectory {
            id,
            entries: vec![
                reasoning_entry(Some("x".repeat(VALUE_MAX_BYTES + 64)), &["page-a"]),
                reasoning_entry(Some("context".to_string()), &[]),
                reasoning_entry(None, &["page-b"]),
            ],
        },
    }
}

/// Construct a full historical generated trajectory JSON fixture.
fn generated_json(id: Uuid) -> Value {
    json!({
        "batch_index": 1,
        "batch_offset": 10,
        "worker_id": "w01",
        "span": "span",
        "attempt_id": 2,
        "deadlock_retries": 0,
        "attempt_paths": ["attempt"],
        "started_at": "2026-06-29T00:00:00Z",
        "duration_seconds": 1.25,
        "status": "completed",
        "usage": {
            "n_calls": 5,
            "input_tokens": 100,
            "output_tokens": 50,
            "cost_usd": 0.01
        },
        "citations": {
            "input_ids": ["source:1", "source:2"],
            "surfaced_page_ids": ["wiki:page-a"],
            "read_page_ids": ["wiki:page-b"],
            "final_citations": {
                "page-a": ["source:1", "source:2"]
            },
            "new_page_slugs": ["page-a"],
            "updated_page_slugs": ["page-b"],
            "categories_assigned": {
                "page-a": ["systems"]
            },
            "custom": {"discarded": true}
        },
        "final_todos": [{"task": "done"}],
        "trajectory": {
            "id": id,
            "actions_and_observations": [
                {
                    "tools": [{
                        "tool_schema": {
                            "name": "wiki_upsert_file",
                            "description": "write a page",
                            "parameters": {"type": "object"},
                            "required": ["slug"]
                        }
                    }],
                    "params": [{"slug": "page-a"}],
                    "sources": ["agent"],
                    "reasoning": "  first  ",
                    "reasoning_signature": "sig"
                },
                {
                    "observations": ["x"],
                    "sources": ["wiki"],
                    "tool_metadata": [{
                        "slug": "page-a",
                        "skipped_due_to_handoff": false,
                        "categories": ["systems"]
                    }]
                },
                {
                    "tools": [{
                        "tool_schema": {"name": "search"}
                    }],
                    "params": [{"query": "target"}],
                    "sources": ["agent"],
                    "reasoning": "middle"
                },
                {
                    "observations": ["ok"],
                    "sources": ["search"],
                    "tool_metadata": [null]
                },
                {
                    "tools": [
                        {"tool_schema": {"name": "wiki_apply_patch"}},
                        {"tool_schema": {"name": "wiki_upsert_file"}},
                        {"tool_schema": {"name": "wiki_upsert_file"}},
                        {"tool_schema": {"name": "search"}}
                    ],
                    "params": [{}, {"slug": "sibling"}, {"slug": "sibling"}, {"slug": "ignored"}],
                    "sources": ["agent", "agent", "agent", "agent"],
                    "reasoning": "  \n  "
                },
                {
                    "observations": ["ok", "ok", "ok", "ok"],
                    "sources": ["wiki", "wiki", "wiki", "search"],
                    "tool_metadata": [
                        {"slug": "target", "skipped_due_to_handoff": false},
                        null,
                        null,
                        {"slug": "ignored"}
                    ]
                },
                {
                    "tools": [{
                        "tool_schema": {"name": "wiki_upsert_file"}
                    }],
                    "params": [{"slug": "skipped"}],
                    "sources": ["agent"],
                    "reasoning": "skipped reason"
                },
                {
                    "observations": ["ok"],
                    "sources": ["wiki"],
                    "tool_metadata": [{
                        "slug": "skipped",
                        "skipped_due_to_handoff": true
                    }]
                },
                {
                    "tools": [{
                        "tool_schema": {"name": "search"}
                    }],
                    "params": [{}],
                    "sources": ["agent"],
                    "reasoning": "   "
                }
            ]
        },
        "unknown_top_level": {"discarded": true}
    })
}

#[test]
/// Verifies full generated JSON deserializes into only user-facing projection data.
fn generated_json_deserializes_to_reasoning_projection() -> TestResult {
    let id = Uuid::parse_str("00000000-0000-0000-0000-000000000020")?;
    let file: ReasoningTrajectoryFile = serde_json::from_value(generated_json(id))?;

    assert_eq!(
        file,
        ReasoningTrajectoryFile {
            citations: Some(Citations {
                input_ids: vec!["source:1".to_string(), "source:2".to_string()],
                surfaced_page_ids: vec!["wiki:page-a".to_string()],
                read_page_ids: vec!["wiki:page-b".to_string()],
                final_citations: BTreeMap::from([(
                    "page-a".to_string(),
                    json!(["source:1", "source:2"])
                )]),
            }),
            trajectory: ReasoningTrajectory {
                id,
                entries: vec![
                    reasoning_entry(Some("first".to_string()), &["page-a"]),
                    reasoning_entry(Some("middle".to_string()), &[]),
                    reasoning_entry(None, &["target", "sibling"]),
                    reasoning_entry(Some("skipped reason".to_string()), &[]),
                ],
            },
        }
    );

    let stored_json = serde_json::to_string(&file)?;
    for discarded in [
        "tool_schema",
        "params",
        "sources",
        "reasoning_signature",
        "observations",
        "tool_metadata",
        "batch_index",
        "categories_assigned",
        "custom",
    ] {
        assert!(
            !stored_json.contains(discarded),
            "stored json retained discarded field {discarded}"
        );
    }
    Ok(())
}

#[test]
/// Verifies the pruned type can deserialize its own serialized JSON.
fn reasoning_file_deserializes_from_pruned_json() -> TestResult {
    let file = sample_file(Uuid::parse_str("00000000-0000-0000-0000-000000000021")?);
    let json = serde_json::to_value(&file)?;
    let decoded: ReasoningTrajectoryFile = serde_json::from_value(json)?;
    assert_eq!(decoded, file);

    let id = Uuid::parse_str("00000000-0000-0000-0000-000000000022")?;
    let decoded: ReasoningTrajectoryFile = serde_json::from_value(json!({
        "trajectory": {
            "id": id,
            "entries": [
                {
                    "reasoning": "  kept  ",
                    "writes": [{"slug": "a"}, {"slug": "a"}]
                },
                {
                    "reasoning": "   ",
                    "writes": []
                }
            ]
        }
    }))?;
    assert_eq!(
        decoded,
        ReasoningTrajectoryFile {
            citations: None,
            trajectory: ReasoningTrajectory {
                id,
                entries: vec![reasoning_entry(Some("kept".to_string()), &["a"])],
            },
        }
    );
    Ok(())
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
/// Verifies citation records round-trip as a pruned normalized object.
fn citations_roundtrip_deduplicates_and_orders() -> TestResult {
    let tid = uuid_to_tid(Uuid::parse_str("00000000-0000-0000-0000-000000000007")?)?;
    let citations = sample_citations_with_duplicates();
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
            "next_order": 6,
            "counts": {
                "input_ids": 2,
                "surfaced_page_ids": 2,
                "read_page_ids": 2,
                "final_citations": 2
            }
        })
    );
    assert_eq!(
        read_citations(&documents, &tid)?,
        Citations {
            input_ids: vec!["source:b".to_string(), "source:a".to_string()],
            surfaced_page_ids: vec!["wiki:page-2".to_string(), "wiki:page-1".to_string()],
            read_page_ids: vec!["wiki:read-2".to_string(), "wiki:read-1".to_string()],
            final_citations: BTreeMap::from([
                ("page-a".to_string(), json!(["source:a"])),
                ("page-b".to_string(), json!(["source:b", "source:c"])),
            ]),
        }
    );
    assert!(records
        .iter()
        .all(|record| !record.id.contains("page_write_ops")));
    assert!(records
        .iter()
        .all(|record| !record.id.contains("categories_assigned")));
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
    };
    let mut records = Vec::new();
    write_citations(&mut records, &tid, &citations)?;
    let documents = documents_from_records(&records);

    assert_eq!(read_citations(&documents, &tid)?, citations);
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
/// Verifies finalized record serialization round-trips only pruned data.
fn save_and_load_finalized_file_roundtrips_pruned_data() -> TestResult {
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
        .any(|record| record.id.contains("/entries/000000/entry/chunks/00001")));

    for forbidden in [
        "root_metadata",
        "/action/",
        "/observation/",
        "tools",
        "params",
        "sources",
        "reasoning_signature",
        "tool_metadata",
        "page_write_ops",
        "categories_assigned",
        "/extra/",
    ] {
        assert!(
            records.iter().all(|record| !record.id.contains(forbidden)),
            "record ids retained forbidden storage segment {forbidden}"
        );
    }

    let documents = documents_from_records(&records);
    let loaded = load_one_from_documents(&documents, file.trajectory.id, true)?;
    assert_eq!(loaded, file);
    Ok(())
}

#[test]
/// Verifies that finalization writes pruned entry payloads.
fn finalization_records_include_pruned_entry_payload() -> TestResult {
    let file = sample_file(Uuid::parse_str("00000000-0000-0000-0000-000000000002")?);
    let tid = uuid_to_tid(file.trajectory.id)?;
    let mut header = TrajectoryHeader {
        v: 1,
        record_type: "ReasoningTrajectoryFile".to_string(),
        tid: tid.clone(),
        entries: file.trajectory.entries.len(),
        write_state: WriteState::Open,
        has_citations: false,
    };
    let mut records = Vec::new();
    collect_finalization_records(&mut records, &mut header, &file, &tid)?;

    assert!(records
        .iter()
        .any(|record| { record.id.contains("/entries/000000/entry/metadata") }));
    assert!(records
        .iter()
        .all(|record| !record.id.contains("/action/tools/")));
    let documents = documents_from_records(&records);
    let loaded = load_one_from_documents(&documents, file.trajectory.id, true)?;
    assert_eq!(loaded, file);
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
        header.metadata.get("schema"),
        Some(&MetadataValue::Str("reasoning_trajectory".to_string()))
    );
    assert_eq!(
        header.metadata.get("tid"),
        Some(&MetadataValue::Str(first.trajectory.id.to_string()))
    );
    Ok(())
}

#[test]
/// Verifies open trajectories can accept pruned entries before finalization.
fn open_trajectory_records_can_be_extended_and_finalized() -> TestResult {
    let finalized = sample_file(Uuid::parse_str("00000000-0000-0000-0000-000000000003")?);
    let entries = finalized.trajectory.entries.clone();
    let mut open_file = finalized.clone();
    open_file.trajectory.entries.clear();
    open_file.citations = None;

    let records = records_for_file(&open_file, WriteState::Open)?;
    let mut documents = documents_from_records(&records);
    assert!(matches!(
        load_one_from_documents(&documents, open_file.trajectory.id, true),
        Err(TrajectoryError::FinalizedRequired { .. })
    ));

    let tid = uuid_to_tid(open_file.trajectory.id)?;
    let mut header = read_trajectory_header(&documents, open_file.trajectory.id)?;
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

    let open_loaded = load_one_from_documents(&documents, open_file.trajectory.id, false)?;
    assert_eq!(open_loaded.trajectory.entries.len(), entries.len());
    assert!(open_loaded.citations.is_none());

    let mut finalize_records = Vec::new();
    collect_finalization_records(
        &mut finalize_records,
        &mut header,
        &finalized,
        &uuid_to_tid(finalized.trajectory.id)?,
    )?;
    insert_documents(&mut documents, finalize_records);

    let finalized_loaded = load_one_from_documents(&documents, finalized.trajectory.id, true)?;
    assert_eq!(finalized_loaded, finalized);
    Ok(())
}

#[tokio::test]
/// Verifies transactional open-trajectory adds first prove ids absent.
async fn create_open_trajectory_reads_absence_before_add() -> TestResult {
    let server = MockServer::start_async().await;
    let collection = mocked_collection(&server);
    let mut file = sample_file(Uuid::parse_str("00000000-0000-0000-0000-000000000009")?);
    file.trajectory.entries.clear();
    file.citations = None;

    let header_key = trajectory_header_key(file.trajectory.id)?;
    let get_path = collection_path(&collection, "conditional/get");
    let commit_path = collection_path(&collection, "conditional/commit");

    let get_mock = server
        .mock_async(|when, then| {
            when.method("POST").path(get_path).json_body(json!({
                "ids": [header_key],
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
                "record_count": 1,
            }));
        })
        .await;

    let mut txn = collection.conditional();
    chroma_create_open_trajectory(&mut txn, &file).await?;
    let result = txn.commit().await?;

    assert_eq!(result.record_count, 1);
    assert_eq!(get_mock.calls(), 1);
    assert_eq!(commit_mock.calls(), 1);
    Ok(())
}

#[tokio::test]
/// Verifies route-facing open writes return the complete compact response.
async fn create_open_generate_trajectory_returns_complete_write_response() -> TestResult {
    let server = MockServer::start_async().await;
    let collection = mocked_collection(&server);
    let mut file = sample_file(Uuid::parse_str("00000000-0000-0000-0000-00000000000d")?);
    file.trajectory.entries.clear();
    file.citations = None;

    let header_key = trajectory_header_key(file.trajectory.id)?;
    let get_path = collection_path(&collection, "conditional/get");
    let commit_path = collection_path(&collection, "conditional/commit");

    let get_mock = server
        .mock_async(|when, then| {
            when.method("POST").path(get_path).json_body(json!({
                "ids": [header_key],
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
                "first_inserted_record_offset": 17,
                "record_count": 1,
            }));
        })
        .await;

    assert_eq!(
        create_open_generate_trajectory(&collection, &file).await?,
        TrajectoryWriteResponse {
            trajectory_id: file.trajectory.id,
            write_state: WriteState::Open,
            entry_count: 0,
            record_count: 1,
            first_inserted_record_offset: Some(17),
        }
    );
    assert_eq!(get_mock.calls(), 1);
    assert_eq!(commit_mock.calls(), 1);
    Ok(())
}

#[tokio::test]
/// Verifies complete saves return finalized compact write metadata.
async fn save_generate_trajectory_returns_complete_write_response() -> TestResult {
    let server = MockServer::start_async().await;
    let collection = mocked_collection(&server);
    let file = sample_file(Uuid::parse_str("00000000-0000-0000-0000-00000000000e")?);
    let commit_path = collection_path(&collection, "conditional/commit");

    let commit_mock = server
        .mock_async(|when, then| {
            when.method("POST").path(commit_path);
            then.status(200).json_body(json!({
                "first_inserted_record_offset": 23,
                "record_count": 99,
            }));
        })
        .await;

    assert_eq!(
        save_generate_trajectory(&collection, &file).await?,
        TrajectoryWriteResponse {
            trajectory_id: file.trajectory.id,
            write_state: WriteState::Finalized,
            entry_count: file.trajectory.entries.len(),
            record_count: 99,
            first_inserted_record_offset: Some(23),
        }
    );
    assert_eq!(commit_mock.calls(), 1);
    Ok(())
}

#[tokio::test]
/// Verifies create-only open writes report an existing trajectory id.
async fn create_open_trajectory_existing_record_is_already_exists() -> TestResult {
    let server = MockServer::start_async().await;
    let collection = mocked_collection(&server);
    let mut file = sample_file(Uuid::parse_str("00000000-0000-0000-0000-00000000000f")?);
    file.trajectory.entries.clear();
    file.citations = None;

    let header_key = trajectory_header_key(file.trajectory.id)?;
    let get_path = collection_path(&collection, "conditional/get");

    let get_mock = server
        .mock_async(|when, then| {
            when.method("POST").path(get_path);
            then.status(200).json_body(json!({
                "ids": [header_key],
                "embeddings": null,
                "documents": null,
                "uris": null,
                "metadatas": null,
                "include": [],
                "read_token": 42,
            }));
        })
        .await;

    let mut txn = collection.conditional();
    let error = chroma_create_open_trajectory(&mut txn, &file)
        .await
        .unwrap_err();
    assert!(matches!(
        &error,
        TrajectoryError::AlreadyExists { tid } if *tid == file.trajectory.id
    ));
    assert_eq!(get_mock.calls(), 1);
    Ok(())
}

#[tokio::test]
/// Verifies filtered trajectory reads with no header map to NotFound.
async fn load_generate_trajectory_missing_header_is_not_found() -> TestResult {
    let server = MockServer::start_async().await;
    let collection = mocked_collection(&server);
    let trajectory_id = Uuid::parse_str("00000000-0000-0000-0000-000000000010")?;
    let get_path = collection_path(&collection, "conditional/get");

    let get_mock = server
        .mock_async(|when, then| {
            when.method("POST").path(get_path);
            then.status(200).json_body(json!({
                "ids": [],
                "embeddings": null,
                "documents": null,
                "uris": null,
                "metadatas": null,
                "include": ["documents", "metadatas"],
                "read_token": 42,
            }));
        })
        .await;

    let error = load_generate_trajectory(&collection, trajectory_id, false)
        .await
        .unwrap_err();
    assert!(matches!(
        &error,
        TrajectoryError::NotFound { tid } if *tid == trajectory_id
    ));
    assert_eq!(get_mock.calls(), 1);
    Ok(())
}

#[test]
/// Verifies entries with no displayable data are rejected.
fn empty_reasoning_entries_are_rejected() -> TestResult {
    let mut file = sample_file(Uuid::parse_str("00000000-0000-0000-0000-000000000004")?);
    file.trajectory.entries[0] = ReasoningEntry {
        reasoning: None,
        writes: Vec::new(),
    };
    let mut records = Vec::new();
    let error = match collect_file_records(&mut records, &file, WriteState::Finalized) {
        Ok(()) => {
            return Err(test_error(
                "expected empty reasoning entry validation error",
            ))
        }
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
    let metadata = metadata_for_key(&format!("gt/{tid}/entries/000001/entry/chunks/00003"))?;
    assert_eq!(
        metadata.get("subtree"),
        Some(&MetadataValue::Str("entries".to_string()))
    );
    assert_eq!(metadata.get("entry"), Some(&MetadataValue::Int(1)));
    assert_eq!(
        metadata.get("field"),
        Some(&MetadataValue::Str("entry".to_string()))
    );
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
    file: &ReasoningTrajectoryFile,
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
