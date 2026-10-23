//! Bounded local owner actors scan their owned immutable source postings.
//! This is a batch-boundary eligibility operation, not distributed reassignment.
use super::eligibility::{fp_eligible, QuantizedEligibility};
use anyhow::{ensure, Context, Result};
use chroma_distance::DistanceFunction;
use chroma_index::quantization::Code;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    sync::mpsc::{self, SyncSender},
    thread::{self, JoinHandle},
    time::Duration,
};

#[derive(Clone, Debug, Serialize)]
pub(super) struct SourceLeaf {
    pub leaf: u32,
    pub centroid: Vec<f32>,
    pub ids: Vec<u32>,
    pub versions: Vec<u8>,
    pub codes: Vec<u8>,
    pub current: Vec<Option<u8>>,
    pub vectors: Vec<Option<Vec<f32>>>,
}
impl SourceLeaf {
    pub fn payload_bytes(&self) -> usize {
        self.centroid.len() * 4
            + self.ids.len() * 4
            + self.versions.len()
            + self.codes.len()
            + self.current.len() * std::mem::size_of::<Option<u8>>()
            + self
                .vectors
                .iter()
                .flatten()
                .map(|v| v.len() * 4)
                .sum::<usize>()
    }
    pub fn hash(&self) -> Result<String> {
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(self)?)))
    }
    pub fn validate(&self, dim: usize) -> Result<()> {
        let n = self.ids.len();
        ensure!(
            n <= 100_000
                && self.centroid.len() == dim
                && self.centroid.iter().all(|x| x.is_finite())
                && self.versions.len() == n
                && self.current.len() == n
                && self.vectors.len() == n
                && n.checked_mul(Code::<1>::size(dim)) == Some(self.codes.len()),
            "malformed owner source"
        );
        ensure!(
            self.vectors
                .iter()
                .flatten()
                .all(|v| v.len() == dim && v.iter().all(|x| x.is_finite())),
            "malformed owner vector"
        );
        Ok(())
    }
    /// Match reference scrub's swap-removal order and default-zero missing
    /// version semantics exactly; this deliberately does not strengthen them.
    fn scrub(&mut self, dim: usize) {
        let size = Code::<1>::size(dim);
        let mut i = 0;
        while i < self.ids.len() {
            if self.versions[i] != self.current[i].unwrap_or(0) {
                let last = self.ids.len() - 1;
                if i != last {
                    self.codes
                        .copy_within(last * size..(last + 1) * size, i * size);
                }
                self.codes.truncate(last * size);
                self.ids.swap_remove(i);
                self.versions.swap_remove(i);
                self.current.swap_remove(i);
                self.vectors.swap_remove(i);
            } else {
                i += 1;
            }
        }
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct ScanRequest {
    pub owner: usize,
    pub epoch: u64,
    pub sequence: u64,
    pub leaf: u32,
    pub source_sha256: String,
    pub old: Vec<f32>,
    pub left: Vec<f32>,
    pub right: Vec<f32>,
    pub fp: bool,
    pub previously_evaluated: Vec<u32>,
}
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ScanReply {
    pub owner: usize,
    pub epoch: u64,
    pub sequence: u64,
    pub leaf: u32,
    pub source_sha256: String,
    pub inserted_evaluated: Vec<u32>,
    pub eligible_ids: Vec<u32>,
    pub evaluated: usize,
    pub physical_before_scrub: usize,
    pub physical_after_scrub: usize,
    pub thread: String,
    pub scan_cloned_payload_bytes: usize,
}
struct Envelope {
    request: ScanRequest,
    response: SyncSender<std::result::Result<ScanReply, String>>,
}
struct Worker {
    send: Option<SyncSender<Envelope>>,
    join: Option<JoinHandle<()>>,
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.send.take();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}
pub(super) struct OwnerFleet {
    workers: BTreeMap<usize, Worker>,
    epoch: u64,
    sequence: u64,
    pub copied_source_payload_bytes: usize,
    pub bootstrap_serialized_source_bytes: usize,
    pub source_hashes: BTreeMap<u32, String>,
}
fn scan(
    source: &SourceLeaf,
    request: &ScanRequest,
    metric: &DistanceFunction,
    dim: usize,
) -> Result<ScanReply> {
    ensure!(
        request.previously_evaluated.len() <= 100_000
            && request.old.len() == dim
            && request.left.len() == dim
            && request.right.len() == dim
            && request
                .old
                .iter()
                .chain(&request.left)
                .chain(&request.right)
                .all(|x| x.is_finite()),
        "malformed scan request"
    );
    ensure!(
        source.leaf == request.leaf && source.hash()? == request.source_sha256,
        "owner source binding mismatch"
    );
    let before = source.ids.len();
    let mut source = source.clone();
    source.scrub(dim);
    let quantized = (!request.fp).then(|| {
        QuantizedEligibility::new(
            &source.centroid,
            &request.old,
            &request.left,
            &request.right,
            Code::<1>::packed_len(dim),
        )
    });
    let mut seen: HashSet<_> = request.previously_evaluated.iter().copied().collect();
    ensure!(
        seen.len() == request.previously_evaluated.len(),
        "duplicate evaluated IDs"
    );
    let mut inserted = vec![];
    let mut eligible = vec![];
    let mut evaluated = 0;
    for i in 0..source.ids.len() {
        let id = source.ids[i];
        if source.versions[i] != source.current[i].unwrap_or(0) || !seen.insert(id) {
            continue;
        }
        inserted.push(id);
        let yes = if request.fp {
            let Some(vector) = &source.vectors[i] else {
                continue;
            };
            evaluated += 1;
            fp_eligible(
                metric,
                vector,
                &source.centroid,
                &request.old,
                &request.left,
                &request.right,
            )
        } else {
            evaluated += 1;
            let size = Code::<1>::size(dim);
            quantized
                .as_ref()
                .unwrap()
                .eligible(metric, &source.codes[i * size..(i + 1) * size])
        };
        if yes {
            eligible.push(id);
        }
    }
    Ok(ScanReply {
        owner: request.owner,
        epoch: request.epoch,
        sequence: request.sequence,
        leaf: request.leaf,
        source_sha256: request.source_sha256.clone(),
        inserted_evaluated: inserted,
        eligible_ids: eligible,
        evaluated,
        physical_before_scrub: before,
        physical_after_scrub: source.ids.len(),
        thread: format!("{:?}", thread::current().id()),
        scan_cloned_payload_bytes: source.payload_bytes(),
    })
}
impl OwnerFleet {
    pub fn new(
        sources: BTreeMap<usize, Vec<SourceLeaf>>,
        epoch: u64,
        dim: usize,
        metric: DistanceFunction,
    ) -> Result<Self> {
        ensure!(
            !sources.is_empty() && sources.len() <= 128 && dim > 0 && dim <= 4096,
            "owner fleet cap"
        );
        let mut hashes = BTreeMap::new();
        let mut bytes = 0usize;
        let mut serialized = 0usize;
        let mut rows = 0usize;
        for leaves in sources.values() {
            for source in leaves {
                source.validate(dim)?;
                ensure!(
                    hashes.insert(source.leaf, source.hash()?).is_none(),
                    "duplicate source leaf"
                );
                rows = rows
                    .checked_add(source.ids.len())
                    .context("row cap overflow")?;
                ensure!(
                    rows <= 100_000 && hashes.len() <= 100_000,
                    "source fleet cap"
                );
                bytes = bytes
                    .checked_add(source.payload_bytes())
                    .context("payload count overflow")?;
                serialized = serialized
                    .checked_add(serde_json::to_vec(source)?.len())
                    .context("serialized count overflow")?;
            }
        }
        ensure!(bytes <= 64 * 1024 * 1024, "owner copied payload cap");
        let mut workers = BTreeMap::new();
        for (owner, leaves) in sources {
            let metric = metric.clone();
            let (send, recv) = mpsc::sync_channel::<Envelope>(1);
            let source: BTreeMap<_, _> = leaves.into_iter().map(|l| (l.leaf, l)).collect();
            let join = thread::Builder::new()
                .name(format!("posting-owner-{owner}"))
                .spawn(move || {
                    let mut last = 0;
                    while let Ok(envelope) = recv.recv() {
                        let r = &envelope.request;
                        let result = (|| {
                            ensure!(
                                r.owner == owner && r.epoch == epoch && r.sequence > last,
                                "owner epoch/sequence mismatch"
                            );
                            last = r.sequence;
                            let leaf = source
                                .get(&r.leaf)
                                .context("leaf absent from requested owner")?;
                            scan(leaf, r, &metric, dim)
                        })();
                        let _ = envelope.response.send(result.map_err(|e| e.to_string()));
                    }
                })?;
            workers.insert(
                owner,
                Worker {
                    send: Some(send),
                    join: Some(join),
                },
            );
        }
        Ok(Self {
            workers,
            epoch,
            sequence: 0,
            copied_source_payload_bytes: bytes,
            bootstrap_serialized_source_bytes: serialized,
            source_hashes: hashes,
        })
    }
    pub fn request(
        &mut self,
        owner: usize,
        leaf: u32,
        old: &[f32],
        left: &[f32],
        right: &[f32],
        fp: bool,
        evaluated: &HashSet<u32>,
    ) -> Result<(ScanRequest, ScanReply)> {
        let worker = self
            .workers
            .get(&owner)
            .context("requested owner missing")?;
        self.sequence = self
            .sequence
            .checked_add(1)
            .context("request sequence exhausted")?;
        let mut previous: Vec<_> = evaluated.iter().copied().collect();
        previous.sort_unstable();
        let request = ScanRequest {
            owner,
            epoch: self.epoch,
            sequence: self.sequence,
            leaf,
            source_sha256: self
                .source_hashes
                .get(&leaf)
                .context("requested leaf missing")?
                .clone(),
            old: old.to_vec(),
            left: left.to_vec(),
            right: right.to_vec(),
            fp,
            previously_evaluated: previous,
        };
        let (send, recv) = mpsc::sync_channel(1);
        worker
            .send
            .as_ref()
            .context("owner closed")?
            .send(Envelope {
                request: request.clone(),
                response: send,
            })
            .map_err(|_| anyhow::anyhow!("owner stopped"))?;
        let reply = recv
            .recv_timeout(Duration::from_secs(5))
            .context("owner response timeout/closed")?
            .map_err(anyhow::Error::msg)?;
        ensure!(
            reply.owner == owner
                && reply.leaf == leaf
                && reply.epoch == self.epoch
                && reply.sequence == request.sequence
                && reply.source_sha256 == request.source_sha256,
            "owner reply scope mismatch"
        );
        Ok((request, reply))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_index::quantization::QuantizedQuery;
    use simsimd::SpatialSimilarity;
    /// Independent transcription of the pre-extraction reference arithmetic.
    fn legacy_distances(
        s: &SourceLeaf,
        r: &ScanRequest,
        metric: &DistanceFunction,
        dim: usize,
    ) -> (Vec<u32>, Vec<u32>, usize) {
        let mut order: Vec<_> = (0..s.ids.len()).collect();
        let mut i = 0;
        while i < order.len() {
            let j = order[i];
            if s.versions[j] != s.current[j].unwrap_or(0) {
                order.swap_remove(i);
            } else {
                i += 1;
            }
        }
        let norm = |v: &[f32]| (f32::dot(v, v).unwrap_or(0.0) as f32).sqrt();
        let c = norm(&s.centroid);
        let prep = |v: &[f32]| {
            let residual: Vec<_> = v.iter().zip(&s.centroid).map(|(a, b)| a - b).collect();
            QuantizedQuery::new(
                &residual,
                Code::<1>::packed_len(dim),
                c,
                f32::dot(&s.centroid, v).unwrap_or(0.) as f32,
                norm(v),
            )
        };
        let old = prep(&r.old);
        let left = prep(&r.left);
        let right = prep(&r.right);
        let neighbor = QuantizedQuery::new(&vec![0.; dim], Code::<1>::packed_len(dim), c, c * c, c);
        let mut seen: HashSet<_> = r.previously_evaluated.iter().copied().collect();
        let mut inserted = vec![];
        let mut eligible = vec![];
        let mut evaluated = 0;
        for i in order {
            let id = s.ids[i];
            if s.versions[i] != s.current[i].unwrap_or(0) || !seen.insert(id) {
                continue;
            }
            inserted.push(id);
            let (l, rr, n, o) = if r.fp {
                let Some(v) = &s.vectors[i] else {
                    continue;
                };
                evaluated += 1;
                (
                    metric.distance(v, &r.left),
                    metric.distance(v, &r.right),
                    metric.distance(v, &s.centroid),
                    metric.distance(v, &r.old),
                )
            } else {
                evaluated += 1;
                let size = Code::<1>::size(dim);
                let code = Code::<1, _>::new(&s.codes[i * size..(i + 1) * size]);
                (
                    code.distance_quantized_query(metric, &left),
                    code.distance_quantized_query(metric, &right),
                    code.distance_quantized_query(metric, &neighbor),
                    code.distance_quantized_query(metric, &old),
                )
            };
            if n <= l && n <= rr {
                continue;
            }
            if o <= l && o <= rr {
                continue;
            }
            eligible.push(id);
        }
        (inserted, eligible, evaluated)
    }
    fn source() -> SourceLeaf {
        let centroid = vec![10., 1., -0., 0.];
        let vectors: Vec<_> = (0..8)
            .map(|i| Some(vec![i as f32 / 8., 1., -0., 0.]))
            .collect();
        let mut codes = vec![];
        for v in &vectors {
            codes.extend_from_slice(Code::<1>::quantize(v.as_ref().unwrap(), &centroid).as_ref());
        }
        SourceLeaf {
            leaf: 9,
            centroid,
            ids: vec![0, 1, 2, 3, 4, 5, 6, 0],
            versions: vec![1, 1, 0, 128, 1, 0, 1, 1],
            current: vec![
                Some(1),
                Some(2),
                None,
                Some(128),
                Some(1),
                Some(1),
                Some(1),
                Some(1),
            ],
            vectors: vectors
                .into_iter()
                .enumerate()
                .map(|(i, v)| if i == 4 { None } else { v })
                .collect(),
            codes,
        }
    }
    #[test]
    fn owner_scan_actor_matches_legacy_order_versions_and_both_predicates() {
        let s = source();
        let main = format!("{:?}", thread::current().id());
        for metric in [DistanceFunction::Euclidean, DistanceFunction::InnerProduct] {
            for fp in [true, false] {
                let mut fleet =
                    OwnerFleet::new(BTreeMap::from([(7, vec![s.clone()])]), 4, 4, metric.clone())
                        .unwrap();
                let (request, reply) = fleet
                    .request(
                        7,
                        9,
                        &[20., 1., 0., 0.],
                        &[0., 1., -0., 0.],
                        &[1., 1., 0., 0.],
                        fp,
                        &HashSet::from([6]),
                    )
                    .unwrap();
                let expected = legacy_distances(&s, &request, &metric, 4);
                assert_eq!(
                    (
                        reply.inserted_evaluated.clone(),
                        reply.eligible_ids.clone(),
                        reply.evaluated
                    ),
                    expected
                );
                assert_ne!(reply.thread, main);
                assert_eq!(reply.physical_before_scrub, 8);
                assert_eq!(reply.physical_after_scrub, 6);
                assert!(reply.inserted_evaluated.contains(&2));
                assert!(reply.inserted_evaluated.contains(&3));
                assert!(!reply.inserted_evaluated.contains(&1));
                assert!(!reply.inserted_evaluated.contains(&5));
                assert!(!reply.inserted_evaluated.contains(&6));
            }
        }
    }
    #[test]
    fn owner_scan_missing_wrong_owner_and_source_bindings_fail_closed() {
        let s = source();
        let mut fleet = OwnerFleet::new(
            BTreeMap::from([(0, vec![s.clone()]), (1, vec![])]),
            2,
            4,
            DistanceFunction::Euclidean,
        )
        .unwrap();
        assert!(fleet
            .request(99, 9, &[0.; 4], &[0.; 4], &[0.; 4], true, &HashSet::new())
            .unwrap_err()
            .to_string()
            .contains("owner missing"));
        assert!(fleet
            .request(1, 9, &[0.; 4], &[0.; 4], &[0.; 4], true, &HashSet::new())
            .unwrap_err()
            .to_string()
            .contains("leaf absent"));
        let mut bad = s.clone();
        bad.codes.pop();
        assert!(OwnerFleet::new(
            BTreeMap::from([(0, vec![bad])]),
            0,
            4,
            DistanceFunction::Euclidean
        )
        .is_err());
        let request = ScanRequest {
            owner: 0,
            epoch: 2,
            sequence: 1,
            leaf: 9,
            source_sha256: "wrong".into(),
            old: vec![0.; 4],
            left: vec![0.; 4],
            right: vec![0.; 4],
            fp: true,
            previously_evaluated: vec![],
        };
        assert!(scan(&s, &request, &DistanceFunction::Euclidean, 4)
            .unwrap_err()
            .to_string()
            .contains("binding"));
    }
}
