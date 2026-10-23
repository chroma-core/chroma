//! Serialized construction with explicit limits and batched immutable vectors.
//! This path does not prepare whole-owner/coordinator rollback copies.
use super::split::{OwnerCommand, OwnerReply};
use super::*;
#[cfg(test)]
#[path = "authoritative_dynamic_tests.rs"]
mod tests;
#[derive(Clone, Debug, Serialize)]
pub struct ConstructionLimits {
    pub max_vectors: usize,
    pub max_nodes: usize,
    pub max_owners: usize,
    pub max_leaf_rows: usize,
    pub max_owner_physical_rows: usize,
    pub max_batch_rows: usize,
    pub max_dimension: usize,
    pub max_events: usize,
    pub trace: bool,
    pub move_above: Option<usize>,
}
impl Default for ConstructionLimits {
    fn default() -> Self {
        Self {
            max_vectors: 2_000_000,
            max_nodes: 100_000,
            max_owners: 128,
            max_leaf_rows: 262_144,
            max_owner_physical_rows: 4_000_000,
            max_batch_rows: 8192,
            max_dimension: 4096,
            max_events: 100_000,
            trace: false,
            move_above: None,
        }
    }
}
impl ConstructionLimits {
    pub(super) fn legacy() -> Self {
        Self {
            max_vectors: 128,
            max_nodes: 1024,
            max_owners: 4,
            max_leaf_rows: 128,
            max_owner_physical_rows: 256,
            max_batch_rows: 128,
            max_dimension: 4096,
            max_events: 10_000,
            trace: true,
            move_above: None,
        }
    }
    fn validate(&self) -> Result<()> {
        ensure!(
            self.max_vectors > 0
                && self.max_vectors <= u32::MAX as usize
                && self.max_nodes >= 3
                && self.max_nodes <= 1_000_000
                && self.max_owners > 0
                && self.max_owners <= 128
                && self.max_leaf_rows >= 2
                && self.max_leaf_rows <= 262_144
                && self.max_owner_physical_rows >= self.max_leaf_rows
                && self.max_batch_rows > 0
                && self.max_batch_rows <= 8192
                && self.max_dimension > 0
                && self.max_dimension <= 4096
                && self.max_events > 0
                && self.max_events <= 1_000_000,
            "construction limits differ"
        );
        ensure!(
            self.move_above
                .is_none_or(|n| n > 0 && n <= self.max_vectors),
            "placement threshold differs"
        );
        Ok(())
    }
}
impl AuthoritativeTree {
    pub async fn open_dynamic(
        p: BlockfileProvider,
        ids: HierarchicalSpannIds,
        metric: DistanceFunction,
        config: HierarchicalSpannConfig,
        rows: usize,
        owner_slots: usize,
        limits: ConstructionLimits,
    ) -> Result<Self> {
        limits.validate()?;
        ensure!(
            config.write_navigation == NavigationMode::Fp
                && config.fp_npa
                && config.max_replicas == 1
                && config.write_level_taus.is_empty()
                && config.write_level_min_pcts.is_empty(),
            "dynamic construction supports reference FP defaults with one replica"
        );
        ensure!(
            config.branching_factor >= 2
                && config.branching_factor < limits.max_nodes
                && config.merge_threshold <= config.split_threshold
                && config.split_threshold > 0
                && config.split_threshold <= limits.max_leaf_rows,
            "dynamic structural configuration differs"
        );
        Self::open_limited(p, ids, metric, config, rows, owner_slots, limits, true).await
    }
    /// Same-binary diagnostic toggle. Normal construction retains its baseline.
    pub fn set_lazy_disabled_evaluation_trace(&mut self, enabled: bool) -> Result<()> {
        self.usable()?;
        ensure!(self.dynamic_mode && !self.event_counts.contains_key("owner-evaluation"), "trace policy must precede evaluation");
        self.lazy_disabled_evaluation_trace=enabled;
        Ok(())
    }
    /// Optional counts and disjoint coordinator spans; no reset during work.
    pub fn enable_before_scrub_attribution(&mut self) -> Result<()> {
        self.usable()?;
        ensure!(self.dynamic_mode && self.before_scrub_attribution.is_none()
            && !self.event_counts.contains_key("before_scrub"), "profile must precede before-scrub work");
        self.before_scrub_attribution=Some(BeforeScrubAttribution::default());
        Ok(())
    }
    pub fn set_lazy_disabled_before_scrub_trace(&mut self, enabled:bool) -> Result<()> {
        self.usable()?;
        ensure!(self.dynamic_mode && !self.event_counts.contains_key("before_scrub")
            && (!enabled || self.before_scrub_attribution.is_some()), "batch trace policy requires observation before work");
        self.lazy_disabled_before_scrub_trace=enabled;
        Ok(())
    }
    pub fn before_scrub_profile(&self) -> Value {
        json!({"enabled":self.before_scrub_attribution.is_some(),"lazy_trace":self.lazy_disabled_before_scrub_trace,"trace_enabled":self.limits.trace,"cumulative":self.before_scrub_attribution})
    }
    pub fn disabled_trace_attribution(&self) -> Value {
        json!({"lazy":self.lazy_disabled_evaluation_trace,"trace_enabled":self.limits.trace,
            "evaluation_events":self.event_counts.get("owner-evaluation").copied().unwrap_or(0),
            "elided_reference_checks":self.elided_trace_reference_checks,
            "retained_reference_checks":self.retained_trace_reference_checks})
    }
    pub(super) fn record(&mut self, event: Value) {
        let kind = event["kind"].as_str().unwrap_or("unknown").to_string();
        *self.event_counts.entry(kind).or_default() += 1;
        if !self.dynamic_mode || self.limits.trace {
            if self.dynamic_mode && self.events.len() >= self.limits.max_events {
                self.trace_overflow = true;
            } else {
                self.events.push(event);
            }
        }
    }
    /// The caller matches this batch boundary and balance cadence to its reference.
    /// A failed direct operation poisons this instance; it has no rollback protocol.
    pub async fn add_dynamic_batch(
        &mut self,
        rows: &[(u32, Vec<f32>)],
        balance_each_row: bool,
    ) -> Result<()> {
        self.apply_dynamic_batch(rows, Some(balance_each_row)).await
    }
    /// Register bounded storage chunks without forcing a reference balance cut.
    pub async fn register_dynamic_batch(&mut self, rows: &[(u32, Vec<f32>)]) -> Result<()> {
        self.apply_dynamic_batch(rows, None).await
    }
    async fn apply_dynamic_batch(
        &mut self,
        rows: &[(u32, Vec<f32>)],
        balance_cadence: Option<bool>,
    ) -> Result<()> {
        self.usable()?;
        self.phase_admit_chunk(rows.len(), balance_cadence)?;
        ensure!(self.dynamic_mode, "dynamic constructor required");
        ensure!(
            !rows.is_empty()
                && rows.len() <= self.limits.max_batch_rows
                && self
                    .locations
                    .len()
                    .checked_add(rows.len())
                    .is_some_and(|n| n <= self.limits.max_vectors),
            "dynamic batch count cap"
        );
        for (index, (id, values)) in rows.iter().enumerate() {
            ensure!(
                *id as usize == self.locations.len() + index
                    && values.len() == self.routing.dim
                    && values.iter().all(|v| v.is_finite()),
                "dense dynamic row/dimension differs"
            );
        }
        self.poisoned = true;
        let vector_writer = storage(
            self.provider
                .write::<u32, Vec<f32>>(
                    BlockfileWriterOptions::new(String::new()).fork(self.latest_vectors),
                )
                .await,
        )?;
        for (id, values) in rows {
            storage(
                vector_writer
                    .set(PREFIX_EMBEDDING, *id, values.clone())
                    .await,
            )?;
        }
        let vectors = storage(vector_writer.commit::<u32, Vec<f32>>().await)?;
        let root = vectors.id();
        storage(vectors.flush::<u32, Vec<f32>>().await)?;
        self.latest_vectors = root;
        self.vector_batch_forks += 1;
        self.record(json!({"kind":"vector-batch","rows":rows.len(),"root":root,"balance_each_row":balance_cadence}));
        if self.phase.is_some() {
            self.register_phased_rows(rows,root)?;
        } else {
        for (id, values) in rows {
            let candidates = self.routing.candidates(values)?;
            let leaf = candidates.first().context("dynamic destination missing")?.0;
            let owner = *self.branches.get(&leaf).context("dynamic owner missing")?;
            let Reply::Ack(reply) = self.owner(owner)?.call(Command::RegisterNew(
                leaf,
                *id,
                VectorRef { root, key: *id },
            ))?
            else {
                bail!("dynamic register reply");
            };
            self.locations.insert(*id, (owner, leaf));
            self.record(json!({"kind":"add_destination","id":id,"version":1,"candidates":candidates.iter().map(|(i,d)|(*i,d.to_bits())).collect::<Vec<_>>(),"selected":[leaf]}));
            self.record(json!({"kind":"leaf_registered","leaf":leaf,"owner":owner,"id":id,"version":1,"bits":values.iter().map(|v|v.to_bits()).collect::<Vec<_>>(),"code":reply["code"]}));
            if balance_cadence == Some(true) {
                self.balance_dynamic_all()?;
            }
        }
        }
        if balance_cadence == Some(false) {
            self.balance_dynamic_all()?;
        }
        ensure!(
            !self.trace_overflow,
            "dynamic trace overflow; profile unusable"
        );
        self.poisoned = false;
        if self.phase.is_none() && self.limits.move_above.is_some() {
            self.rebalance_dynamic_placement()?;
        }
        if self.trace_overflow {
            self.poisoned = true;
            bail!("dynamic trace overflow; profile unusable");
        }
        Ok(())
    }
    fn balance_dynamic_all(&mut self) -> Result<()> {
        for round in 0..=100 {
            let mut needed = vec![];
            for &leaf in self.branches.keys() {
                let n = self.leaf_count(leaf)?;
                if n > self.routing.config.split_threshold
                    || (self.routing.nodes[&leaf].parent.is_some()
                        && n > 0
                        && n < self.routing.config.merge_threshold)
                {
                    needed.push(leaf);
                }
            }
            #[cfg(test)]
            ensure!(
                !self.require_split_tape || !self.balance_rounds.is_empty(),
                "missing reference balance round"
            );
            #[cfg(test)]
            if let Some(planned) = self.balance_rounds.pop_front() {
                ensure!(
                    planned.iter().copied().collect::<BTreeSet<_>>()
                        == needed.iter().copied().collect::<BTreeSet<_>>(),
                    "reference outer balance set differs"
                );
                needed = planned;
            }
            if needed.is_empty() {
                break;
            }
            if round == 100 {
                self.record(json!({"kind":"balance-round-limit","pending_leaves":needed}));
                break;
            }
            for leaf in needed {
                self.balance_owned(leaf, 0, &mut BTreeSet::new())?;
            }
        }
        Ok(())
    }
    pub(super) fn rebalance_dynamic_placement(&mut self) -> Result<()> {
        let cap = self.limits.move_above.unwrap();
        for _ in 0..self.limits.max_nodes {
            let mut loads = vec![];
            for &owner in self.owners.keys() {
                let OwnerReply::LiveCount(n) = self.structural(owner, OwnerCommand::LiveCount)?
                else {
                    bail!("owner load reply");
                };
                loads.push((owner, n));
            }
            let Some(&(source, _)) = loads.iter().find(|(_, n)| *n > cap) else {
                return Ok(());
            };
            let OwnerReply::BranchCounts(branches) =
                self.structural(source, OwnerCommand::BranchCounts)?
            else {
                bail!("branch loads reply");
            };
            let mut options = vec![];
            for (leaf, n) in branches {
                if n == 0 {
                    continue;
                }
                for &(owner, load) in &loads {
                    if owner != source && load.checked_add(n).is_some_and(|total| total <= cap) {
                        options.push((std::cmp::Reverse(n), load, leaf, owner));
                    }
                }
            }
            options.sort();
            let Some(&(_, _, leaf, to)) = options.first() else {
                self.record(json!({"kind":"placement-capacity-unavailable","source":source,"loads":loads,"threshold":cap}));
                return Ok(());
            };
            self.move_branch(leaf, to)?;
        }
        bail!("placement iteration cap")
    }
    pub fn configure_dynamic(&mut self, config: HierarchicalSpannConfig) -> Result<()> {
        self.usable()?;
        self.phase_require_ready()?;
        ensure!(
            self.dynamic_mode
                && config.write_navigation == NavigationMode::Fp
                && config.fp_npa
                && config.max_replicas == 1
                && config.write_level_taus.is_empty()
                && config.write_level_min_pcts.is_empty()
                && config.branching_factor >= 2
                && config.branching_factor < self.limits.max_nodes
                && config.split_threshold > 0
                && config.merge_threshold <= config.split_threshold
                && config.split_threshold <= self.limits.max_leaf_rows,
            "dynamic structural configuration differs"
        );
        self.routing.config = config;
        Ok(())
    }
    /// Bounded diagnostic metadata collected outside the timed fixed-access interval.
    pub fn construction_phase_state(&self) -> Result<Value> {
        self.usable()?;
        ensure!(self.routing.nodes.len()<=10000, "phase node report cap");
        let mut levels=BTreeMap::<usize,usize>::new();
        for node in self.routing.nodes.values() {
            let mut parent=node.parent;let mut depth=0;
            while let Some(id)=parent {depth+=1;ensure!(depth<=self.routing.nodes.len(), "phase parent cycle");parent=self.routing.nodes.get(&id).context("phase parent missing")?.parent;}
            *levels.entry(depth).or_default()+=1;
        }
        let mut leaves=vec![];
        for (&leaf,&owner) in &self.branches {leaves.push(json!({"leaf":leaf,"owner":owner,"live_rows":self.leaf_count(leaf)?}));}
        Ok(json!({"embedding_root":self.latest_vectors,"dense_rows":self.locations.len(),"nodes":self.routing.nodes.len(),"levels":levels,"leaves":leaves}))
    }
    /// Publish a joined append-only embedding view before a balance phase.
    /// Chunk insertion enforces a dense new-ID range and writes no old embedding keys.
    /// Saved readers and centroid roots remain independent of this construction view.
    pub fn publish_current_embedding_view(&mut self) -> Result<()> {
        self.usable()?;
        self.phase_require_finished_add()?;
        ensure!(self.dynamic_mode, "dynamic constructor required");
        self.poisoned=true;
        let count=self.locations.len();
        for owner in self.owners.keys().copied().collect::<Vec<_>>() {
            ensure!(matches!(self.structural(owner,OwnerCommand::PublishEmbeddingView {root:self.latest_vectors,count})?,OwnerReply::Ack), "embedding view reply");
        }
        self.construction_embedding_view_active=true;
        self.poisoned=false;
        Ok(())
    }
    pub fn balance_dynamic_current(&mut self) -> Result<()> {
        self.usable()?;
        self.phase_require_balancing()?;
        ensure!(self.dynamic_mode, "dynamic constructor required");
        self.poisoned = true;
        self.balance_dynamic_all()?;
        ensure!(
            !self.trace_overflow,
            "dynamic trace overflow; profile unusable"
        );
        self.poisoned = false;
        Ok(())
    }
    /// Select bounded read-only preparation workers at a completed phase boundary.
    /// Zero retains the original serial owner-cache path. One/two use the same
    /// independent-reader preparation implementation for controlled comparison.
    pub fn set_split_preparation_workers(&mut self, workers:usize) -> Result<()> {
        self.usable()?;self.phase_require_ready()?;
        ensure!(self.dynamic_mode && workers<=8, "split preparation policy differs");
        self.poisoned=true;
        for owner in self.owners.keys().copied().collect::<Vec<_>>() {ensure!(matches!(self.structural(owner,OwnerCommand::SplitPreparationWorkers(workers))?,OwnerReply::Ack),"split preparation policy reply");}
        self.poisoned=false;Ok(())
    }
    pub fn construction_accounting(&self) -> Value {
        json!({"limits":self.limits,"dynamic":self.dynamic_mode,"vector_batch_forks":self.vector_batch_forks,"placement_directory_point_checks":self.placement_id_checks,"retained_events":self.events.len(),"trace_overflow":self.trace_overflow,"event_counts":self.event_counts,"whole_owner_preparation":"not called by direct dynamic batch path","coordinator_backups":"not allocated by direct dynamic batch path","remaining":"full owner bootstrap/save; central routing and ID-location metadata; per-balance leaf metadata sweep; trace construction overhead; shared local storage/provider"})
    }
}
