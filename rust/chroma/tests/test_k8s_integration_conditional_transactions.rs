use std::collections::{BTreeMap, BTreeSet};

use chroma::client::ChromaHttpClientError;
use chroma::types::{
    ConditionalCommitResult, Include, IncludeList, Metadata, MetadataValue, UpdateMetadata,
    UpdateMetadataValue,
};
use chroma::{ChromaCollection, ChromaHttpClient, ChromaHttpClientOptions};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, TestCaseError, TestRunner};
use uuid::Uuid;

#[derive(Clone, Debug)]
struct TransactionCase {
    label: String,
    ops: Vec<TxnOp>,
}

#[derive(Clone, Debug)]
enum TxnOp {
    Add(Vec<RecordPayload>),
    Update(Vec<RecordPayload>),
    Upsert(Vec<RecordPayload>),
    Delete(Vec<String>),
}

#[derive(Clone, Debug)]
struct RecordPayload {
    id: String,
    embedding: Vec<f32>,
    document: String,
    uri: String,
    metadata: Metadata,
}

#[derive(Clone, Debug, PartialEq)]
struct ObservedRecord {
    embedding: Option<Vec<f32>>,
    document: Option<String>,
    uri: Option<String>,
    metadata: Option<Metadata>,
}

struct TestSession {
    client: ChromaHttpClient,
    collections: Vec<String>,
}

impl TestSession {
    fn new() -> Self {
        Self {
            client: ChromaHttpClient::new(ChromaHttpClientOptions {
                endpoint: "http://localhost:8000".parse().unwrap(),
                tenant_id: Some("default_tenant".to_string()),
                database_name: Some("default_database".to_string()),
                ..Default::default()
            }),
            collections: Vec::new(),
        }
    }

    async fn new_collection(&mut self, base: &str) -> Result<ChromaCollection, String> {
        let name = format!("{base}_{}", Uuid::new_v4());
        self.collections.push(name.clone());
        self.client
            .get_or_create_collection(&name, None, None)
            .await
            .map_err(|err| format!("create collection {name}: {err:?}"))
    }

    async fn cleanup(self) {
        for collection in self.collections {
            if let Err(err) = self.client.delete_collection(&collection).await {
                eprintln!("failed to cleanup {collection}: {err:?}");
            }
        }
    }
}

#[tokio::test]
#[test_log::test]
async fn test_k8s_integration_conditional_transactions_fixed_corpus() {
    if let Err(err) = verify_transaction_sequence(fixed_corpus()).await {
        panic!("{err}");
    }
}

#[test]
fn test_k8s_integration_conditional_transactions_proptest() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let cases = std::env::var("CHROMA_TXN_PROPTEST_CASES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8);
    let mut runner = TestRunner::new(ProptestConfig {
        cases,
        failure_persistence: None,
        ..ProptestConfig::default()
    });
    let completed_cases = std::cell::Cell::new(0usize);

    let result = runner.run(&transaction_sequence_strategy(), |sequence| {
        runtime
            .block_on(verify_transaction_sequence(sequence))
            .map_err(TestCaseError::fail)?;
        completed_cases.set(completed_cases.get() + 1);
        Ok(())
    });
    println!(
        "conditional transaction proptest completed {} of {cases} configured cases",
        completed_cases.get()
    );
    result.unwrap();
}

async fn verify_transaction_sequence(sequence: Vec<TransactionCase>) -> Result<(), String> {
    let mut session = TestSession::new();
    let result = verify_transaction_sequence_with_session(&mut session, sequence).await;
    session.cleanup().await;
    result
}

async fn verify_transaction_sequence_with_session(
    session: &mut TestSession,
    sequence: Vec<TransactionCase>,
) -> Result<(), String> {
    let baseline = session
        .new_collection("test_k8s_integration_txn_baseline")
        .await?;
    let transactional = session
        .new_collection("test_k8s_integration_txn_transactional")
        .await?;
    let mut observed_ids = BTreeSet::new();
    let mut admitted_transactions = 0usize;
    let mut admitted_non_empty_transactions = 0usize;
    let mut skipped_invalid_shape = 0usize;
    let mut skipped_inadmissible = 0usize;

    for case in sequence {
        let label = case.label.clone();
        let write_ids = case.write_ids();
        observed_ids.extend(write_ids.iter().cloned());

        if !case.has_valid_shape() {
            skipped_invalid_shape += 1;
            continue;
        }

        let snapshot = observe_records(&baseline, &write_ids).await?;
        if !case.is_admissible(&snapshot) {
            skipped_inadmissible += 1;
            continue;
        }
        admitted_transactions += 1;
        if !case.ops.is_empty() {
            admitted_non_empty_transactions += 1;
        }

        for op in &case.ops {
            apply_baseline_op(&baseline, op)
                .await
                .map_err(|err| format!("{label}: prong one failed: {err:?}"))?;
        }

        let mut prong_two = transactional.conditional();
        let mut prong_three = transactional.conditional();
        for op in &case.ops {
            apply_transaction_op(&mut prong_two, op)
                .await
                .map_err(|err| format!("{label}: prong two op failed: {err:?}"))?;
            apply_transaction_op(&mut prong_three, op)
                .await
                .map_err(|err| format!("{label}: prong three op failed: {err:?}"))?;
        }

        let prong_two_result = prong_two
            .commit()
            .await
            .map_err(|err| format!("{label}: prong two commit failed: {err:?}"))?;
        if case.ops.is_empty() {
            ensure_eq(
                ConditionalCommitResult {
                    first_inserted_record_offset: None,
                    record_count: 0,
                },
                prong_two_result,
                format!("{label}: empty prong two commit result"),
            )?;
        } else {
            ensure(
                prong_two_result.first_inserted_record_offset.is_some(),
                format!("{label}: non-empty prong two commit did not append"),
            )?;
            ensure_eq(
                case.record_count(),
                prong_two_result.record_count,
                format!("{label}: prong two record_count"),
            )?;
        }

        let prong_three_result = prong_three.commit().await;
        if case.ops.is_empty() {
            ensure_eq(
                ConditionalCommitResult {
                    first_inserted_record_offset: None,
                    record_count: 0,
                },
                prong_three_result
                    .map_err(|err| format!("{label}: empty prong three commit failed: {err:?}"))?,
                format!("{label}: empty prong three commit result"),
            )?;
        } else {
            let err = prong_three_result
                .err()
                .ok_or_else(|| format!("{label}: prong three unexpectedly committed"))?;
            ensure(
                is_conditional_conflict(&err),
                format!("{label}: prong three failed without a conditional conflict: {err:?}"),
            )?;
        }

        assert_collections_match(&baseline, &transactional, &observed_ids, &label).await?;
    }

    ensure(
        admitted_transactions > 0,
        format!(
            "verifier did not admit any transactions; skipped_invalid_shape={skipped_invalid_shape}, skipped_inadmissible={skipped_inadmissible}"
        ),
    )?;
    ensure(
        admitted_non_empty_transactions > 0,
        format!(
            "verifier did not admit any non-empty transactions; admitted={admitted_transactions}, skipped_invalid_shape={skipped_invalid_shape}, skipped_inadmissible={skipped_inadmissible}"
        ),
    )?;

    Ok(())
}

async fn assert_collections_match(
    baseline: &ChromaCollection,
    transactional: &ChromaCollection,
    ids: &BTreeSet<String>,
    label: &str,
) -> Result<(), String> {
    let baseline_count = baseline
        .count()
        .await
        .map_err(|err| format!("{label}: count baseline: {err:?}"))?;
    let transactional_count = transactional
        .count()
        .await
        .map_err(|err| format!("{label}: count transactional: {err:?}"))?;
    ensure_eq(
        baseline_count,
        transactional_count,
        format!("{label}: collection counts differ"),
    )?;

    let baseline_records = observe_records(baseline, ids).await?;
    let transactional_records = observe_records(transactional, ids).await?;
    ensure_eq(
        baseline_records,
        transactional_records,
        format!("{label}: collection records differ"),
    )
}

async fn observe_records(
    collection: &ChromaCollection,
    ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, ObservedRecord>, String> {
    if ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    let response = collection
        .get(
            Some(ids.iter().cloned().collect()),
            None,
            None,
            None,
            Some(IncludeList(vec![
                Include::Document,
                Include::Embedding,
                Include::Metadata,
                Include::Uri,
            ])),
        )
        .await
        .map_err(|err| format!("get records from {}: {err:?}", collection.name()))?;

    let mut records = BTreeMap::new();
    for (index, id) in response.ids.iter().enumerate() {
        records.insert(
            id.clone(),
            ObservedRecord {
                embedding: response
                    .embeddings
                    .as_ref()
                    .map(|embeddings| embeddings[index].clone()),
                document: response
                    .documents
                    .as_ref()
                    .and_then(|documents| documents[index].clone()),
                uri: response.uris.as_ref().and_then(|uris| uris[index].clone()),
                metadata: response
                    .metadatas
                    .as_ref()
                    .and_then(|metadatas| metadatas[index].clone()),
            },
        );
    }
    Ok(records)
}

async fn apply_baseline_op(
    collection: &ChromaCollection,
    op: &TxnOp,
) -> Result<(), ChromaHttpClientError> {
    match op {
        TxnOp::Add(records) => {
            collection
                .add(
                    record_ids(records),
                    record_embeddings(records),
                    Some(record_documents(records)),
                    Some(record_uris(records)),
                    Some(record_metadatas(records)),
                )
                .await?;
        }
        TxnOp::Update(records) => {
            collection
                .update(
                    record_ids(records),
                    Some(update_embeddings(records)),
                    Some(record_documents(records)),
                    Some(record_uris(records)),
                    Some(update_metadatas(records)),
                )
                .await?;
        }
        TxnOp::Upsert(records) => {
            collection
                .upsert(
                    record_ids(records),
                    record_embeddings(records),
                    Some(record_documents(records)),
                    Some(record_uris(records)),
                    Some(update_metadatas(records)),
                )
                .await?;
        }
        TxnOp::Delete(ids) => {
            collection.delete(Some(ids.clone()), None, None).await?;
        }
    }
    Ok(())
}

async fn apply_transaction_op(
    txn: &mut chroma::ConditionalCollectionTransaction,
    op: &TxnOp,
) -> Result<(), ChromaHttpClientError> {
    txn.get(Some(op.ids()), None, None, None, None).await?;
    match op {
        TxnOp::Add(records) => {
            txn.add(
                record_ids(records),
                record_embeddings(records),
                Some(record_documents(records)),
                Some(record_uris(records)),
                Some(record_metadatas(records)),
            )
            .await?;
        }
        TxnOp::Update(records) => {
            txn.update(
                record_ids(records),
                Some(update_embeddings(records)),
                Some(record_documents(records)),
                Some(record_uris(records)),
                Some(update_metadatas(records)),
            )
            .await?;
        }
        TxnOp::Upsert(records) => {
            txn.upsert(
                record_ids(records),
                record_embeddings(records),
                Some(record_documents(records)),
                Some(record_uris(records)),
                Some(update_metadatas(records)),
            )
            .await?;
        }
        TxnOp::Delete(ids) => {
            txn.delete(ids.clone()).await?;
        }
    }
    Ok(())
}

fn is_conditional_conflict(err: &ChromaHttpClientError) -> bool {
    match err {
        ChromaHttpClientError::ApiError(message, status) => {
            status.as_u16() == 412
                || (status.as_u16() == 409
                    && (message.contains("conditional write conflict")
                        || message.contains("ConditionalWriteConflictError")))
        }
        ChromaHttpClientError::StaleReadError(_) => true,
        _ => false,
    }
}

impl TransactionCase {
    fn write_ids(&self) -> BTreeSet<String> {
        self.ops
            .iter()
            .flat_map(TxnOp::ids)
            .collect::<BTreeSet<_>>()
    }

    fn record_count(&self) -> usize {
        self.ops.iter().map(TxnOp::record_count).sum()
    }

    fn has_valid_shape(&self) -> bool {
        let mut seen = BTreeSet::new();
        for op in &self.ops {
            let mut op_ids = BTreeSet::new();
            for id in op.ids() {
                if !op_ids.insert(id.clone()) || !seen.insert(id) {
                    return false;
                }
            }
        }
        true
    }

    fn is_admissible(&self, snapshot: &BTreeMap<String, ObservedRecord>) -> bool {
        self.ops.iter().all(|op| match op {
            TxnOp::Add(records) => records
                .iter()
                .all(|record| !snapshot.contains_key(&record.id)),
            TxnOp::Update(records) => records
                .iter()
                .all(|record| snapshot.contains_key(&record.id)),
            TxnOp::Upsert(_) => true,
            TxnOp::Delete(ids) => ids.iter().all(|id| snapshot.contains_key(id)),
        })
    }
}

impl TxnOp {
    fn ids(&self) -> Vec<String> {
        match self {
            TxnOp::Add(records) | TxnOp::Update(records) | TxnOp::Upsert(records) => {
                record_ids(records)
            }
            TxnOp::Delete(ids) => ids.clone(),
        }
    }

    fn record_count(&self) -> usize {
        match self {
            TxnOp::Add(records) | TxnOp::Update(records) | TxnOp::Upsert(records) => records.len(),
            TxnOp::Delete(ids) => ids.len(),
        }
    }
}

fn record_ids(records: &[RecordPayload]) -> Vec<String> {
    records.iter().map(|record| record.id.clone()).collect()
}

fn record_embeddings(records: &[RecordPayload]) -> Vec<Vec<f32>> {
    records
        .iter()
        .map(|record| record.embedding.clone())
        .collect()
}

fn update_embeddings(records: &[RecordPayload]) -> Vec<Option<Vec<f32>>> {
    records
        .iter()
        .map(|record| Some(record.embedding.clone()))
        .collect()
}

fn record_documents(records: &[RecordPayload]) -> Vec<Option<String>> {
    records
        .iter()
        .map(|record| Some(record.document.clone()))
        .collect()
}

fn record_uris(records: &[RecordPayload]) -> Vec<Option<String>> {
    records
        .iter()
        .map(|record| Some(record.uri.clone()))
        .collect()
}

fn record_metadatas(records: &[RecordPayload]) -> Vec<Option<Metadata>> {
    records
        .iter()
        .map(|record| Some(record.metadata.clone()))
        .collect()
}

fn update_metadatas(records: &[RecordPayload]) -> Vec<Option<UpdateMetadata>> {
    records
        .iter()
        .map(|record| Some(metadata_to_update_metadata(&record.metadata)))
        .collect()
}

fn metadata_to_update_metadata(metadata: &Metadata) -> UpdateMetadata {
    metadata
        .iter()
        .map(|(key, value)| (key.clone(), UpdateMetadataValue::from(value.clone())))
        .collect()
}

fn record(id: &str, seed: u32, flavor: &str) -> RecordPayload {
    let mut metadata = Metadata::new();
    metadata.insert("flavor".to_string(), MetadataValue::Str(flavor.to_string()));
    metadata.insert("seed".to_string(), MetadataValue::Int(seed as i64));
    metadata.insert("id".to_string(), MetadataValue::Str(id.to_string()));

    RecordPayload {
        id: id.to_string(),
        embedding: vec![seed as f32, (seed % 17) as f32, (seed % 31) as f32],
        document: format!("{flavor}-document-{id}-{seed}"),
        uri: format!("urn:chroma-transaction-test:{flavor}:{id}:{seed}"),
        metadata,
    }
}

fn records(ids: &[&str], seed: u32, flavor: &str) -> Vec<RecordPayload> {
    ids.iter()
        .enumerate()
        .map(|(index, id)| record(id, seed + index as u32, flavor))
        .collect()
}

fn fixed_corpus() -> Vec<TransactionCase> {
    vec![
        TransactionCase {
            label: "empty".to_string(),
            ops: vec![],
        },
        TransactionCase {
            label: "add-single".to_string(),
            ops: vec![TxnOp::Add(records(&["a1"], 10, "add-single"))],
        },
        TransactionCase {
            label: "add-multiple".to_string(),
            ops: vec![TxnOp::Add(records(&["a2", "a3", "a4"], 20, "add-multiple"))],
        },
        TransactionCase {
            label: "update-single".to_string(),
            ops: vec![TxnOp::Update(records(&["a1"], 30, "update-single"))],
        },
        TransactionCase {
            label: "update-multiple".to_string(),
            ops: vec![TxnOp::Update(records(&["a2", "a3"], 40, "update-multiple"))],
        },
        TransactionCase {
            label: "upsert-absent-single".to_string(),
            ops: vec![TxnOp::Upsert(records(&["u1"], 50, "upsert-absent-single"))],
        },
        TransactionCase {
            label: "upsert-absent-multiple".to_string(),
            ops: vec![TxnOp::Upsert(records(
                &["u2", "u3"],
                60,
                "upsert-absent-multiple",
            ))],
        },
        TransactionCase {
            label: "upsert-present-single".to_string(),
            ops: vec![TxnOp::Upsert(records(&["u1"], 70, "upsert-present-single"))],
        },
        TransactionCase {
            label: "upsert-present-multiple".to_string(),
            ops: vec![TxnOp::Upsert(records(
                &["a2", "u2"],
                80,
                "upsert-present-multiple",
            ))],
        },
        TransactionCase {
            label: "delete-single".to_string(),
            ops: vec![TxnOp::Delete(vec!["a4".to_string()])],
        },
        TransactionCase {
            label: "delete-multiple".to_string(),
            ops: vec![TxnOp::Delete(vec!["a1".to_string(), "u3".to_string()])],
        },
        TransactionCase {
            label: "multi-write".to_string(),
            ops: vec![
                TxnOp::Add(records(&["m1", "m2"], 90, "multi-add")),
                TxnOp::Update(records(&["a2"], 100, "multi-update")),
                TxnOp::Upsert(records(&["u2", "m3"], 110, "multi-upsert")),
                TxnOp::Delete(vec!["a3".to_string()]),
            ],
        },
    ]
}

#[derive(Clone, Debug)]
struct GeneratedTxn {
    ops: Vec<GeneratedOp>,
}

#[derive(Clone, Debug)]
struct GeneratedOp {
    kind: GeneratedOpKind,
    id_count: usize,
    seed: u32,
}

#[derive(Clone, Debug)]
enum GeneratedOpKind {
    AddFresh,
    UpdateExisting,
    UpsertFresh,
    UpsertExisting,
    DeleteExisting,
}

fn transaction_sequence_strategy() -> impl Strategy<Value = Vec<TransactionCase>> {
    let seed = Just(GeneratedTxn {
        ops: vec![GeneratedOp {
            kind: GeneratedOpKind::AddFresh,
            id_count: 2,
            seed: 0,
        }],
    });
    (
        seed,
        proptest::collection::vec(generated_txn_strategy(), 0..=5),
    )
        .prop_map(|(seed, mut generated)| {
            generated.insert(0, seed);
            materialize_generated_txns(generated)
        })
}

fn generated_txn_strategy() -> impl Strategy<Value = GeneratedTxn> {
    proptest::collection::vec(generated_op_strategy(), 0..=4).prop_map(|ops| GeneratedTxn { ops })
}

fn generated_op_strategy() -> impl Strategy<Value = GeneratedOp> {
    (
        prop_oneof![
            Just(GeneratedOpKind::AddFresh),
            Just(GeneratedOpKind::UpdateExisting),
            Just(GeneratedOpKind::UpsertFresh),
            Just(GeneratedOpKind::UpsertExisting),
            Just(GeneratedOpKind::DeleteExisting),
        ],
        prop_oneof![Just(1usize), 2usize..=3],
        0u32..10_000,
    )
        .prop_map(|(kind, id_count, seed)| GeneratedOp {
            kind,
            id_count,
            seed,
        })
}

fn materialize_generated_txns(generated: Vec<GeneratedTxn>) -> Vec<TransactionCase> {
    let mut present = BTreeSet::new();
    let mut next_id = 0usize;
    let mut cases = Vec::with_capacity(generated.len());

    for (txn_index, generated_txn) in generated.into_iter().enumerate() {
        let mut reserved = BTreeSet::new();
        let mut ops = Vec::new();
        let present_at_start = present.clone();

        for (op_index, generated_op) in generated_txn.ops.into_iter().enumerate() {
            let flavor = format!("generated-{txn_index}-{op_index}");
            match generated_op.kind {
                GeneratedOpKind::AddFresh => {
                    let ids = fresh_ids(&mut next_id, generated_op.id_count, "generated-add-fresh");
                    reserved.extend(ids.iter().cloned());
                    ops.push(TxnOp::Add(records_from_strings(
                        &ids,
                        generated_op.seed,
                        &flavor,
                    )));
                }
                GeneratedOpKind::UpdateExisting => {
                    let ids = existing_ids(
                        &present_at_start,
                        &reserved,
                        generated_op.id_count,
                        generated_op.seed,
                    );
                    if !ids.is_empty() {
                        reserved.extend(ids.iter().cloned());
                        ops.push(TxnOp::Update(records_from_strings(
                            &ids,
                            generated_op.seed,
                            &flavor,
                        )));
                    }
                }
                GeneratedOpKind::UpsertFresh => {
                    let ids = fresh_ids(
                        &mut next_id,
                        generated_op.id_count,
                        "generated-upsert-fresh",
                    );
                    reserved.extend(ids.iter().cloned());
                    ops.push(TxnOp::Upsert(records_from_strings(
                        &ids,
                        generated_op.seed,
                        &flavor,
                    )));
                }
                GeneratedOpKind::UpsertExisting => {
                    let ids = existing_ids(
                        &present_at_start,
                        &reserved,
                        generated_op.id_count,
                        generated_op.seed,
                    );
                    if !ids.is_empty() {
                        reserved.extend(ids.iter().cloned());
                        ops.push(TxnOp::Upsert(records_from_strings(
                            &ids,
                            generated_op.seed,
                            &flavor,
                        )));
                    }
                }
                GeneratedOpKind::DeleteExisting => {
                    let ids = existing_ids(
                        &present_at_start,
                        &reserved,
                        generated_op.id_count,
                        generated_op.seed,
                    );
                    if !ids.is_empty() {
                        reserved.extend(ids.iter().cloned());
                        ops.push(TxnOp::Delete(ids));
                    }
                }
            }
        }

        for op in &ops {
            match op {
                TxnOp::Add(records) | TxnOp::Upsert(records) => {
                    present.extend(records.iter().map(|record| record.id.clone()));
                }
                TxnOp::Update(_) => {}
                TxnOp::Delete(ids) => {
                    for id in ids {
                        present.remove(id);
                    }
                }
            }
        }

        cases.push(TransactionCase {
            label: format!("generated-{txn_index}"),
            ops,
        });
    }

    cases
}

fn fresh_ids(next_id: &mut usize, count: usize, prefix: &str) -> Vec<String> {
    (0..count)
        .map(|_| {
            let id = format!("{prefix}-{next_id}");
            *next_id += 1;
            id
        })
        .collect()
}

fn existing_ids(
    present: &BTreeSet<String>,
    reserved: &BTreeSet<String>,
    count: usize,
    seed: u32,
) -> Vec<String> {
    let mut candidates = present
        .iter()
        .filter(|id| !reserved.contains(*id))
        .cloned()
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Vec::new();
    }
    let rotation = seed as usize % candidates.len();
    candidates.rotate_left(rotation);
    candidates.truncate(count.min(candidates.len()));
    candidates
}

fn records_from_strings(ids: &[String], seed: u32, flavor: &str) -> Vec<RecordPayload> {
    ids.iter()
        .enumerate()
        .map(|(index, id)| record(id, seed + index as u32, flavor))
        .collect()
}

fn ensure(condition: bool, message: String) -> Result<(), String> {
    if condition {
        Ok(())
    } else {
        Err(message)
    }
}

fn ensure_eq<T>(expected: T, actual: T, message: String) -> Result<(), String>
where
    T: std::fmt::Debug + PartialEq,
{
    if expected == actual {
        Ok(())
    } else {
        Err(format!(
            "{message}: expected {expected:#?}, actual {actual:#?}"
        ))
    }
}
