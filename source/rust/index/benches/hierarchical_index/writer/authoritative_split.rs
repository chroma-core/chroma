//! One serialized structural operation prepares inside the actual owner actors.
//! The coordinator carries topology and bounded split input, never owner tables.
use super::*;
use chroma_index::spann::utils::EmbeddingPoint;
use std::sync::Arc;

#[cfg(test)]
#[path = "authoritative_split_tests.rs"]
mod tests;

pub(super) struct PreparedBackup {
    leaves: BTreeMap<u32, Leaf>,
    current: BTreeMap<u32, Current>,
    limit: usize,
}
#[derive(Clone)]
pub(super) struct Row {
    id: u32,
    version: u8,
    vector: VectorRef,
}
pub(super) enum OwnerCommand {
    Begin,
    Abort,
    Commit,
    Rows {
        leaf: u32,
        scrub: bool,
    },
    SplitInput(u32),
    Reference(u32),
    Physical(u32),
    Install {
        old: u32,
        left: u32,
        right: u32,
        parent: u32,
        centers: [Vec<f32>; 2],
        groups: [Vec<u32>; 2],
    },
    Evaluate {
        leaf: u32,
        row: Row,
        old: Vec<f32>,
        left: Vec<f32>,
        right: Option<Vec<f32>>,
        centroid: Option<Vec<f32>>,
        seen: bool,
    },
}
pub(super) enum OwnerReply {
    Ack,
    Rows(Vec<Row>),
    Reference(Row),
    Physical(Vec<(u32, u8)>),
    Input(Vec<EmbeddingPoint>),
    Evaluation {
        valid: bool,
        eligible: bool,
        current: Option<u8>,
        values: Option<Vec<f32>>,
    },
}
impl State {
    pub(super) async fn structure_handle(&mut self, command: OwnerCommand) -> Result<OwnerReply> {
        match command {
            OwnerCommand::Begin => {
                ensure!(self.prepared.is_none(), "owner preparation already active");
                self.counters.prepared_posting_payload_bytes +=
                    self.leaves.values().map(Leaf::bytes).sum::<usize>();
                self.counters.prepared_current_records += self.current.len();
                // The old authoritative maps stay in the backup until admission.
                self.prepared = Some(PreparedBackup {
                    leaves: self.leaves.clone(),
                    current: self.current.clone(),
                    limit: self.posting_limit,
                });
                self.posting_limit = 129;
                Ok(OwnerReply::Ack)
            }
            OwnerCommand::Abort => {
                let backup = self.prepared.take().context("no owner preparation")?;
                self.leaves = backup.leaves;
                self.current = backup.current;
                self.posting_limit = backup.limit;
                Ok(OwnerReply::Ack)
            }
            OwnerCommand::Commit => {
                let backup = self.prepared.take().context("no owner preparation")?;
                self.posting_limit = backup.limit;
                Ok(OwnerReply::Ack)
            }
            OwnerCommand::Rows { leaf, scrub } => {
                ensure!(self.prepared.is_some(), "rows require prepared operation");
                let l = self
                    .leaves
                    .get_mut(&leaf)
                    .context("structural leaf not owned")?;
                if scrub {
                    let size = Code::<1>::size(self.dim);
                    let mut i = 0;
                    while i < l.ids.len() {
                        let valid = self.current.get(&l.ids[i]).is_some_and(|c| {
                            c.leaf == leaf && c.version == l.versions[i] && c.version & 128 == 0
                        });
                        if valid {
                            i += 1;
                        } else {
                            let last = l.ids.len() - 1;
                            l.ids.swap_remove(i);
                            l.versions.swap_remove(i);
                            if i != last {
                                l.codes
                                    .copy_within(last * size..(last + 1) * size, i * size);
                            }
                            l.codes.truncate(last * size);
                        }
                    }
                }
                let rows = l
                    .ids
                    .iter()
                    .zip(&l.versions)
                    .filter_map(|(&id, &version)| {
                        self.current
                            .get(&id)
                            .filter(|c| {
                                c.leaf == leaf && c.version == version && version & 128 == 0
                            })
                            .map(|c| Row {
                                id,
                                version,
                                vector: c.vector,
                            })
                    })
                    .collect();
                Ok(OwnerReply::Rows(rows))
            }
            OwnerCommand::Physical(leaf) => {
                ensure!(self.prepared.is_some(), "physical rows require preparation");
                let l = self.leaves.get(&leaf).context("physical leaf not owned")?;
                Ok(OwnerReply::Physical(
                    l.ids
                        .iter()
                        .copied()
                        .zip(l.versions.iter().copied())
                        .collect(),
                ))
            }
            OwnerCommand::Reference(id) => {
                let c = self.current.get(&id).context("reference ID not owned")?;
                Ok(OwnerReply::Reference(Row {
                    id,
                    version: c.version,
                    vector: c.vector,
                }))
            }
            OwnerCommand::SplitInput(leaf) => {
                ensure!(self.prepared.is_some(), "split requires preparation");
                let l = self.leaves.get(&leaf).context("split source not owned")?;
                let rows: Vec<_> = l
                    .ids
                    .iter()
                    .zip(&l.versions)
                    .filter_map(|(&id, &version)| {
                        self.current
                            .get(&id)
                            .filter(|c| c.leaf == leaf && c.version == version)
                            .map(|c| (id, version, c.vector))
                    })
                    .collect();
                let mut input = Vec::with_capacity(rows.len());
                for (id, version, reference) in rows {
                    input.push((
                        id,
                        u32::from(version),
                        Arc::from(self.vector(reference, PREFIX_EMBEDDING).await?),
                    ));
                }
                Ok(OwnerReply::Input(input))
            }
            OwnerCommand::Install {
                old,
                left,
                right,
                parent,
                centers,
                groups,
            } => {
                ensure!(
                    self.prepared.is_some()
                        && left != right
                        && !self.leaves.contains_key(&left)
                        && !self.leaves.contains_key(&right),
                    "replacement admission"
                );
                let source = self
                    .leaves
                    .get(&old)
                    .context("replacement source not owned")?;
                let expected: BTreeSet<_> = source.ids.iter().copied().collect();
                let actual: BTreeSet<_> = groups.iter().flatten().copied().collect();
                ensure!(
                    expected == actual
                        && groups.iter().map(Vec::len).sum::<usize>() == actual.len()
                        && groups.iter().all(|g| !g.is_empty()),
                    "split membership cover"
                );
                let mut replacements = Vec::new();
                for ((leaf, center), ids) in [left, right].into_iter().zip(centers).zip(groups) {
                    ensure!(
                        center.len() == self.dim && center.iter().all(|v| v.is_finite()),
                        "split center"
                    );
                    let mut versions = vec![];
                    let mut codes = vec![];
                    for &id in &ids {
                        let c = self
                            .current
                            .get(&id)
                            .context("split current missing")?
                            .clone();
                        ensure!(
                            c.leaf == old && c.version & 128 == 0,
                            "split source current mismatch"
                        );
                        let values = self.vector(c.vector, PREFIX_EMBEDDING).await?;
                        versions.push(c.version);
                        codes.extend_from_slice(Code::<1>::quantize(&values, &center).as_ref());
                    }
                    replacements.push((
                        leaf,
                        Leaf {
                            parent,
                            centroid_code: Code::<1>::quantize(&center, &vec![0.; self.dim])
                                .as_ref()
                                .to_vec(),
                            centroid: center,
                            ids,
                            versions,
                            codes,
                        },
                    ));
                }
                self.leaves.remove(&old);
                for (leaf, data) in replacements {
                    for id in &data.ids {
                        self.current.get_mut(id).unwrap().leaf = leaf;
                    }
                    self.leaves.insert(leaf, data);
                }
                Ok(OwnerReply::Ack)
            }
            OwnerCommand::Evaluate {
                leaf: _,
                row,
                old,
                left,
                right,
                centroid,
                seen,
            } => {
                ensure!(self.prepared.is_some(), "evaluation requires preparation");
                let current = self.current.get(&row.id).cloned();
                let valid = current
                    .as_ref()
                    .is_some_and(|c| c.version == row.version && c.version & 128 == 0);
                let version = current.as_ref().map(|c| c.version);
                if !valid || seen {
                    return Ok(OwnerReply::Evaluation {
                        valid,
                        eligible: false,
                        current: version,
                        values: None,
                    });
                }
                ensure!(
                    old.len() == self.dim
                        && left.len() == self.dim
                        && right.as_ref().is_none_or(|v| v.len() == self.dim),
                    "predicate dimension"
                );
                let centroid = centroid.unwrap_or_default();
                let values = self
                    .vector(current.unwrap().vector, PREFIX_EMBEDDING)
                    .await?;
                self.counters.structural_evaluations += 1;
                let eligible = match right {
                    Some(right) => {
                        fp_eligible(&self.metric, &values, &centroid, &old, &left, &right)
                    }
                    None => {
                        self.metric.distance(&values, &left) > self.metric.distance(&values, &old)
                    }
                };
                Ok(OwnerReply::Evaluation {
                    valid,
                    eligible,
                    current: version,
                    values: eligible.then_some(values),
                })
            }
        }
    }
}
impl AuthoritativeTree {
    fn structural(&self, owner: usize, command: OwnerCommand) -> Result<OwnerReply> {
        let Reply::Structure(reply) = self.owner(owner)?.call(Command::Structure(command))? else {
            bail!("structural reply");
        };
        Ok(reply)
    }
    fn structural_admission(&self) -> Result<()> {
        self.usable()?;
        let c = &self.routing.config;
        ensure!(
            c.write_navigation == NavigationMode::Fp && c.fp_npa && c.merge_threshold == 0,
            "evolving authority requires FP navigation/FP NPA and merges disabled"
        );
        ensure!(
            c.branching_factor >= 2 && c.branching_factor <= 128 && c.split_threshold >= 2,
            "structural configuration cap"
        );
        let root = &self.routing.nodes[&self.routing.root];
        if let Some(children) = &root.children {
            ensure!(
                children.len() < c.branching_factor,
                "internal parent headroom required before mutation"
            );
            ensure!(
                children.len() <= c.branching_factor
                    && children
                        .iter()
                        .all(|id| self.routing.nodes[id].children.is_none()),
                "internal split and multi-level parent edits unsupported"
            );
        }
        ensure!(self.events.len() < 10_000, "structural event cap");
        ensure!(
            self.branches
                .values()
                .all(|owner| self.owners.contains_key(owner)),
            "structural owner missing"
        );
        Ok(())
    }
    /// Add and balance on owner-private prepared state; publish authority only when
    /// the supported structural operation completes. No reference writer is used.
    pub async fn add_with_owner_balance(&mut self, id: u32, values: Vec<f32>) -> Result<()> {
        self.structural_admission()?;
        ensure!(
            self.locations.len() < 128 && id == self.locations.len() as u32,
            "dense new ID required"
        );
        let candidates = self.routing.candidates(&values)?;
        let leaf = candidates.first().context("no destination")?.0;
        let owner = *self
            .branches
            .get(&leaf)
            .context("destination owner missing")?;
        let old_routing = self.routing.clone();
        let old_locations = self.locations.clone();
        let old_branches = self.branches.clone();
        let old_tombstones = self.tombstones.clone();
        let old_vectors = self.latest_vectors;
        // This immutable vector fork is private until the complete operation saves.
        let vector = self.save_vector(id, values.clone()).await?;
        self.poisoned = true;
        for actor in self.owners.keys() {
            self.structural(*actor, OwnerCommand::Begin)?;
        }
        let result = (|| -> Result<()> {
            let Reply::Ack(reply) = self
                .owner(owner)?
                .call(Command::RegisterNew(leaf, id, vector))?
            else {
                bail!("prepared register reply");
            };
            self.locations.insert(id, (owner, leaf));
            self.events.push(json!({"kind":"add_destination","id":id,"version":1,"candidates":bits(&candidates),"selected":[leaf],"reply":reply}));
            self.events.push(json!({"kind":"leaf_registered","leaf":leaf,"owner":owner,"id":id,"version":1,"bits":values.iter().map(|v|v.to_bits()).collect::<Vec<_>>(),"code":reply["code"]}));
            self.balance_owned(leaf, 0, &mut BTreeSet::new())?;
            for &leaf in self.branches.keys() {
                ensure!(
                    self.leaf_count(leaf)? <= self.routing.config.split_threshold,
                    "additional outer balance rounds unsupported before authority installation"
                );
            }

            ensure!(
                self.routing.nodes.len() <= 1024 && self.events.len() <= 10_000,
                "prepared operation cap"
            );
            Ok(())
        })();
        if let Err(error) = result {
            for owner in self.owners.keys() {
                self.structural(*owner, OwnerCommand::Abort)?;
            }
            self.routing = old_routing;
            self.locations = old_locations;
            self.branches = old_branches;
            self.tombstones = old_tombstones;
            self.latest_vectors = old_vectors;
            self.poisoned = false;
            self.events.push(json!({"kind":"unsupported-preparation-discarded","id":id,"error":error.to_string()}));
            return Err(error);
        }
        for owner in self.owners.keys() {
            self.structural(*owner, OwnerCommand::Commit)?;
        }
        self.poisoned = false;
        self.events.push(json!({"kind":"owner-operation-installed","id":id,"root":self.routing.root,"branches":self.branches}));
        Ok(())
    }
    fn rows_owned(&self, leaf: u32, scrub: bool) -> Result<Vec<Row>> {
        let owner = *self.branches.get(&leaf).context("rows owner missing")?;
        let OwnerReply::Rows(rows) = self.structural(owner, OwnerCommand::Rows { leaf, scrub })?
        else {
            bail!("rows reply");
        };
        Ok(rows)
    }
    fn balance_owned(
        &mut self,
        leaf: u32,
        depth: u32,
        balancing: &mut BTreeSet<u32>,
    ) -> Result<()> {
        if depth > 4 || !self.branches.contains_key(&leaf) {
            return Ok(());
        }
        let rows = self.rows_owned(leaf, true)?;
        if rows.len() > self.routing.config.split_threshold && balancing.insert(leaf) {
            self.split_owned(leaf, depth, balancing)?;
            balancing.remove(&leaf);
        }
        Ok(())
    }
    fn allocate_owned_node(&mut self) -> Result<u32> {
        let id = self.routing.next;
        self.routing.next = id.checked_add(1).context("node namespace exhausted")?;
        ensure!(
            !self.routing.nodes.contains_key(&id) && self.routing.nodes.len() < 1024,
            "node allocation conflict/cap"
        );
        Ok(id)
    }
    fn split_owned(&mut self, old: u32, depth: u32, balancing: &mut BTreeSet<u32>) -> Result<()> {
        let owner = *self.branches.get(&old).context("split owner missing")?;
        let old_node = self
            .routing
            .nodes
            .get(&old)
            .context("split topology missing")?
            .clone();
        ensure!(old_node.children.is_none(), "internal split unsupported");
        if let Some(parent) = old_node.parent {
            ensure!(
                parent == self.routing.root
                    && self.routing.nodes[&parent]
                        .children
                        .as_ref()
                        .context("noninternal parent")?
                        .len()
                        < self.routing.config.branching_factor,
                "internal parent overflow unsupported before authority installation"
            );
        }
        let OwnerReply::Input(points) = self.structural(owner, OwnerCommand::SplitInput(old))?
        else {
            bail!("split input reply");
        };
        ensure!(
            points.len() > self.routing.config.split_threshold && points.len() <= 128,
            "split input bound"
        );
        #[cfg(test)]
        let (lc, lg, rc, rg) = super::super::live_owner_tests::replay_one_split(
            self.split_decisions
                .pop_front()
                .context("missing reference split decision")?,
            points,
            &self.routing.metric,
        );
        #[cfg(not(test))]
        let (lc, lg, rc, rg) = chroma_index::spann::utils::split(points, &self.routing.metric);
        let left = self.allocate_owned_node()?;
        let right = self.allocate_owned_node()?;
        let parent = if let Some(p) = old_node.parent {
            p
        } else {
            self.allocate_owned_node()?
        };
        let groups = [
            lg.iter().map(|p| p.0).collect::<Vec<_>>(),
            rg.iter().map(|p| p.0).collect::<Vec<_>>(),
        ];
        self.structural(
            owner,
            OwnerCommand::Install {
                old,
                left,
                right,
                parent,
                centers: [lc.to_vec(), rc.to_vec()],
                groups: groups.clone(),
            },
        )?;
        self.routing.nodes.remove(&old);
        self.tombstones.insert(old);
        self.branches.remove(&old);
        for (id, center) in [(left, lc.to_vec()), (right, rc.to_vec())] {
            self.routing.nodes.insert(
                id,
                Node {
                    parent: Some(parent),
                    centroid_code: Code::<1>::quantize(&center, &vec![0.; self.routing.dim])
                        .as_ref()
                        .to_vec(),
                    centroid: center,
                    children: None,
                },
            );
            self.branches.insert(id, owner);
        }
        for (leaf, group) in [left, right].into_iter().zip(&groups) {
            for id in group {
                self.locations.insert(*id, (owner, leaf));
            }
        }
        let children = if old_node.parent.is_some() {
            let children = self
                .routing
                .nodes
                .get_mut(&parent)
                .unwrap()
                .children
                .as_mut()
                .unwrap();
            children.retain(|&c| c != old);
            children.extend([left, right]);
            children.clone()
        } else {
            self.routing.root = parent;
            vec![left, right]
        };
        let mut centroid = vec![0f32; self.routing.dim];
        for child in &children {
            for (a, &b) in centroid.iter_mut().zip(&self.routing.nodes[child].centroid) {
                *a += b;
            }
        }
        let scale = 1.0 / children.len() as f32;
        for value in &mut centroid {
            *value *= scale;
        }
        self.routing.nodes.insert(
            parent,
            Node {
                parent: None,
                centroid_code: Code::<1>::quantize(&centroid, &vec![0.; self.routing.dim])
                    .as_ref()
                    .to_vec(),
                centroid,
                children: Some(children),
            },
        );
        self.events.push(json!({"kind":"split","old":old,"left":left,"right":right,"owner":owner,"parent":parent,"left_ids":groups[0],"right_ids":groups[1]}));
        if depth < 4 {
            let mut evaluated = BTreeSet::new();
            for (leaf, group, new) in [(left, &lg, lc.as_ref()), (right, &rg, rc.as_ref())] {
                for (id, version, _) in group {
                    let row = Row {
                        id: *id,
                        version: *version as u8,
                        vector: self.reference_owned(*id)?,
                    };
                    self.evaluate_owned(
                        leaf,
                        row,
                        &old_node.centroid,
                        new,
                        None,
                        None,
                        None,
                        &mut evaluated,
                        depth,
                        balancing,
                    )?;
                }
            }
            let neighbors = self.routing.candidates(&old_node.centroid)?;
            self.events.push(json!({"kind":"discovery","old":old,"left":left,"right":right,"neighbors":bits(&neighbors)}));
            for (neighbor, _) in neighbors
                .into_iter()
                .take(self.routing.config.reassign_neighbor_count)
            {
                if [old, left, right].contains(&neighbor) {
                    continue;
                }
                let before = if let Some(&o) = self.branches.get(&neighbor) {
                    let OwnerReply::Physical(rows) =
                        self.structural(o, OwnerCommand::Physical(neighbor))?
                    else {
                        bail!("physical rows reply");
                    };
                    let mut versions = vec![];
                    for (id, stored) in rows {
                        versions.push((id, stored, Some(self.current_owned(id)?)));
                    }
                    Some(versions)
                } else {
                    None
                };
                self.events
                    .push(json!({"kind":"before_scrub","leaf":neighbor,"versions":before}));
                if !self.branches.contains_key(&neighbor) {
                    continue;
                }
                let source_owner = self.branches[&neighbor];
                let neighbor_center = self.routing.nodes[&neighbor].centroid.clone();
                let rows = self.rows_owned(neighbor, true)?;
                self.events.push(json!({"kind":"neighbor-begin","split_owner":owner,"owner":self.branches[&neighbor],"leaf":neighbor,"ids":rows.iter().map(|r|r.id).collect::<Vec<_>>(),"versions":rows.iter().map(|r|r.version).collect::<Vec<_>>()}));
                for (index, row) in rows.into_iter().enumerate() {
                    self.evaluate_owned(
                        neighbor,
                        row,
                        &old_node.centroid,
                        &lc,
                        Some(&rc),
                        Some(index),
                        Some((source_owner, neighbor_center.clone())),
                        &mut evaluated,
                        depth,
                        balancing,
                    )?;
                }
                self.events.push(json!({"kind":"neighbor-end"}));
            }
        }
        Ok(())
    }
    fn current_owned(&self, id: u32) -> Result<u8> {
        let (owner, _) = *self
            .locations
            .get(&id)
            .context("current location missing")?;
        let OwnerReply::Reference(row) = self.structural(owner, OwnerCommand::Reference(id))?
        else {
            bail!("current reply");
        };
        Ok(row.version)
    }
    fn reference_owned(&self, id: u32) -> Result<VectorRef> {
        let (owner, _) = *self
            .locations
            .get(&id)
            .context("current location missing")?;
        let OwnerReply::Reference(row) = self.structural(owner, OwnerCommand::Reference(id))?
        else {
            bail!("reference reply");
        };
        Ok(row.vector)
    }
    #[allow(clippy::too_many_arguments)]
    fn evaluate_owned(
        &mut self,
        leaf: u32,
        row: Row,
        old: &[f32],
        left: &[f32],
        right: Option<&[f32]>,
        index: Option<usize>,
        frame: Option<(usize, Vec<f32>)>,
        evaluated: &mut BTreeSet<u32>,
        depth: u32,
        balancing: &mut BTreeSet<u32>,
    ) -> Result<()> {
        let owner = frame.as_ref().map(|(owner, _)| *owner).unwrap_or(
            self.locations
                .get(&row.id)
                .context("evaluation location missing")?
                .0,
        );
        let seen = evaluated.contains(&row.id);
        let OwnerReply::Evaluation {
            valid,
            eligible,
            current,
            values,
        } = self.structural(
            owner,
            OwnerCommand::Evaluate {
                leaf,
                row: row.clone(),
                old: old.to_vec(),
                left: left.to_vec(),
                right: right.map(<[f32]>::to_vec),
                centroid: frame.map(|(_, center)| center),
                seen,
            },
        )?
        else {
            bail!("evaluation reply");
        };
        self.events.push(json!({"kind":"owner-evaluation","owner":owner,"leaf":leaf,"id":row.id,"stored":row.version,"current":self.current_owned(row.id)?,"source_current":current,"valid":valid,"seen":seen,"eligible":eligible,"neighbor":right.is_some(),"index":index}));
        if !valid || seen {
            return Ok(());
        }
        evaluated.insert(row.id);
        if eligible {
            self.reassign_owned(
                leaf,
                row.id,
                values.context("eligible vector missing")?,
                depth,
                balancing,
            )?;
        }
        Ok(())
    }
    fn reassign_owned(
        &mut self,
        from_leaf: u32,
        id: u32,
        values: Vec<f32>,
        depth: u32,
        balancing: &mut BTreeSet<u32>,
    ) -> Result<()> {
        let (from, actual_leaf) = *self.locations.get(&id).context("reassign source missing")?;
        let candidates = self.routing.candidates(&values)?;
        let leaf = candidates
            .first()
            .context("reassign destination missing")?
            .0;
        let lookup = self.lookup_at(from, id)?;
        self.events.push(json!({"kind":"destination","id":id,"from":from_leaf,"current":lookup.version,"candidates":bits(&candidates),"selected":[leaf]}));
        if leaf == from_leaf {
            return Ok(());
        }
        let to = *self.branches.get(&leaf).context("reassign owner missing")?;
        let vector = self.reference_owned(id)?;
        let Reply::Ack(reply) = self.owner(from)?.call(Command::Reroute {
            id,
            from_leaf: actual_leaf,
            to,
            to_leaf: leaf,
            vector,
            destination: self.owner(to)?.sender()?,
        })?
        else {
            bail!("immediate reassignment reply");
        };
        self.locations.insert(id, (to, leaf));
        self.events.push(json!({"kind":"leaf_registered","leaf":leaf,"owner":to,"id":id,"version":reply["version"],"bits":values.iter().map(|v|v.to_bits()).collect::<Vec<_>>(),"code":reply["code"]}));
        self.events.push(json!({"kind":"registered","id":id,"current":reply["version"],"destinations":[[leaf,to]],"reply":reply}));
        self.balance_owned(leaf, depth + 1, balancing)?;
        Ok(())
    }
    pub(super) async fn save_structure(
        &self,
        pl: &BlockfileWriter,
        sm: &BlockfileWriter,
        ln: &BlockfileWriter,
    ) -> Result<(Uuid, Uuid)> {
        if self.tombstones.is_empty() {
            return Ok((self.latest_vectors, self.parent.internal_node_id));
        }
        let inn = storage(
            self.provider
                .write::<u32, HierarchicalInternalNode<'static>>(
                    BlockfileWriterOptions::new(String::new()).fork(self.parent.internal_node_id),
                )
                .await,
        )?;
        let vectors = storage(
            self.provider
                .write::<u32, Vec<f32>>(
                    BlockfileWriterOptions::new(String::new()).fork(self.latest_vectors),
                )
                .await,
        )?;
        for &id in &self.tombstones {
            storage(
                pl.delete::<u32, HierarchicalSpannPostingList<'static>>("", id)
                    .await,
            )?;
            storage(
                ln.delete::<u32, HierarchicalLeafNode<'static>>("", id)
                    .await,
            )?;
            storage(
                inn.delete::<u32, HierarchicalInternalNode<'static>>("", id)
                    .await,
            )?;
            storage(vectors.delete::<u32, Vec<f32>>(PREFIX_CENTROID, id).await)?;
        }
        for (&id, node) in &self.routing.nodes {
            storage(
                vectors
                    .set(PREFIX_CENTROID, id, node.centroid.clone())
                    .await,
            )?;
            if let Some(children) = &node.children {
                storage(
                    inn.set(
                        "",
                        id,
                        HierarchicalInternalNode {
                            parent: node.parent.unwrap_or(NO_PARENT),
                            centroid_code: &node.centroid_code,
                            children,
                        },
                    )
                    .await,
                )?;
            }
        }
        storage(sm.set(PREFIX_ROOT, SINGLETON_KEY, self.routing.root).await)?;
        storage(
            sm.set(PREFIX_NEXT_NODE, SINGLETON_KEY, self.routing.next)
                .await,
        )?;
        let vectors = storage(vectors.commit::<u32, Vec<f32>>().await)?;
        let inn = storage(inn.commit::<u32, HierarchicalInternalNode<'static>>().await)?;
        let ids = (vectors.id(), inn.id());
        storage(vectors.flush::<u32, Vec<f32>>().await)?;
        storage(inn.flush::<u32, HierarchicalInternalNode<'static>>().await)?;
        Ok(ids)
    }
}
fn bits(scores: &[(u32, f32)]) -> Vec<(u32, u32)> {
    scores.iter().map(|(id, d)| (*id, d.to_bits())).collect()
}
