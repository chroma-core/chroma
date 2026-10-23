//! Reference structural edits use coordinator centroids and actual owner state.
use super::split::{OwnerCommand, OwnerReply};
use super::*;
use chroma_index::spann::utils::{EmbeddingPoint, SplitResult};
use std::sync::Arc;
impl AuthoritativeTree {
    pub(super) fn split_points_owned(
        &mut self,
        points: Vec<EmbeddingPoint>,
    ) -> Result<SplitResult> {
        let _cost=self.balance_cost_span(super::balance_cost::Stage::Clustering);
        #[cfg(test)]
        {
            if let Some(decision) = self.split_decisions.pop_front() {
                Ok(super::super::live_owner_tests::replay_one_split(
                    decision,
                    points,
                    &self.routing.metric,
                ))
            } else {
                ensure!(
                    self.dynamic_mode && !self.require_split_tape,
                    "missing reference structural decision"
                );
                Ok(super::super::live_owner_tests::reference_split(
                    points,
                    &self.routing.metric,
                ))
            }
        }
        #[cfg(not(test))]
        {
            Ok(chroma_index::spann::utils::split(
                points,
                &self.routing.metric,
            ))
        }
    }
    fn origin_code(&self, centroid: &[f32]) -> Vec<u8> {
        Code::<1>::quantize(centroid, &vec![0.; self.routing.dim])
            .as_ref()
            .to_vec()
    }
    fn child_mean(&self, children: &[u32]) -> Vec<f32> {
        let mut mean = vec![0.; self.routing.dim];
        let mut count = 0;
        for id in children {
            if let Some(n) = self.routing.nodes.get(id) {
                for (a, &b) in mean.iter_mut().zip(&n.centroid) {
                    *a += b;
                }
                count += 1;
            }
        }
        if count > 0 {
            let scale = 1.0 / count as f32;
            for x in &mut mean {
                *x *= scale;
            }
        }
        mean
    }
    fn update_mean(&mut self, id: u32) -> Result<()> {
        let children = self.routing.nodes[&id]
            .children
            .clone()
            .context("mean requires internal")?;
        let mean = self.child_mean(&children);
        let code = self.origin_code(&mean);
        let n = self.routing.nodes.get_mut(&id).unwrap();
        n.centroid = mean;
        n.centroid_code = code;
        Ok(())
    }
    fn set_owned_parent(&mut self, id: u32, parent: Option<u32>) -> Result<()> {
        let n = self
            .routing
            .nodes
            .get_mut(&id)
            .context("parent target missing")?;
        n.parent = parent;
        if n.children.is_none() {
            let owner = *self
                .branches
                .get(&id)
                .context("parent leaf owner missing")?;
            self.structural(
                owner,
                OwnerCommand::SetParent {
                    leaf: id,
                    parent: parent.unwrap_or(NO_PARENT),
                },
            )?;
        }
        Ok(())
    }
    pub(super) fn create_owned_root_at(&mut self, root: u32, children: &[u32]) -> Result<()> {
        let centroid = self.child_mean(children);
        let code = self.origin_code(&centroid);
        self.routing.nodes.insert(
            root,
            Node {
                parent: None,
                centroid,
                centroid_code: code,
                children: Some(children.to_vec()),
            },
        );
        for &child in children {
            self.set_owned_parent(child, Some(root))?;
        }
        self.routing.root = root;
        self.record(json!({"kind":"root-grown","root":root,"children":children}));
        Ok(())
    }
    pub(super) fn replace_owned_child(
        &mut self,
        parent: u32,
        old: u32,
        new: &[u32],
        balancing: &mut BTreeSet<u32>,
    ) -> Result<()> {
        let n = self
            .routing
            .nodes
            .get_mut(&parent)
            .context("replacement parent missing")?;
        let children = n.children.as_mut().context("replacement parent is leaf")?;
        children.retain(|&c| c != old);
        children.extend_from_slice(new);
        let overflow = children.len() > self.routing.config.branching_factor;
        self.update_mean(parent)?;
        for &child in new {
            self.set_owned_parent(child, Some(parent))?;
        }
        if overflow {
            self.split_owned_internal(parent, balancing)?;
        }
        Ok(())
    }
    fn split_owned_internal(&mut self, id: u32, balancing: &mut BTreeSet<u32>) -> Result<()> {
        if !balancing.insert(id) {
            return Ok(());
        }
        let old = self
            .routing
            .nodes
            .remove(&id)
            .context("internal split missing")?;
        let children = old.children.context("internal split leaf")?;
        ensure!(
            children.len() > self.routing.config.branching_factor,
            "internal split not overflowing"
        );
        self.tombstones.insert(id);
        let points = children
            .iter()
            .map(|&child| {
                (
                    child,
                    0,
                    Arc::from(self.routing.nodes[&child].centroid.clone()),
                )
            })
            .collect();
        let (lc, lg, rc, rg) = self.split_points_owned(points)?;
        let left = self.allocate_owned_node()?;
        let right = self.allocate_owned_node()?;
        let groups = [
            lg.iter().map(|p| p.0).collect::<Vec<_>>(),
            rg.iter().map(|p| p.0).collect::<Vec<_>>(),
        ];
        for (node, center, children) in [
            (left, lc.to_vec(), groups[0].clone()),
            (right, rc.to_vec(), groups[1].clone()),
        ] {
            let code = self.origin_code(&center);
            self.routing.nodes.insert(
                node,
                Node {
                    parent: None,
                    centroid: center,
                    centroid_code: code,
                    children: Some(children.clone()),
                },
            );
            for child in children {
                self.set_owned_parent(child, Some(node))?;
            }
        }
        self.record(
            json!({"kind":"internal-split","old":id,"left":left,"right":right,"children":groups}),
        );
        balancing.remove(&id);
        if let Some(parent) = old.parent {
            self.replace_owned_child(parent, id, &[left, right], balancing)?;
        } else {
            let root = self.allocate_owned_node()?;
            self.create_owned_root_at(root, &[left, right])?;
        }
        Ok(())
    }
    fn remove_owned_child(&mut self, parent: u32, child: u32, empty_owner: usize) -> Result<()> {
        let n = self
            .routing
            .nodes
            .get_mut(&parent)
            .context("remove parent missing")?;
        let grandparent = n.parent;
        let children = n.children.as_mut().context("remove parent is leaf")?;
        children.retain(|&c| c != child);
        let remaining = children.clone();
        if remaining.is_empty() {
            self.routing.nodes.remove(&parent);
            self.tombstones.insert(parent);
            if parent == self.routing.root {
                let root = self.allocate_owned_node()?;
                let centroid = vec![0.; self.routing.dim];
                let code = self.origin_code(&centroid);
                self.structural(
                    empty_owner,
                    OwnerCommand::EmptyLeaf {
                        leaf: root,
                        parent: NO_PARENT,
                        centroid: centroid.clone(),
                        code: code.clone(),
                    },
                )?;
                self.branches.insert(root, empty_owner);
                self.routing.nodes.insert(
                    root,
                    Node {
                        parent: None,
                        centroid,
                        centroid_code: code,
                        children: None,
                    },
                );
                self.routing.root = root;
            } else if let Some(gp) = grandparent {
                self.remove_owned_child(gp, parent, empty_owner)?;
            }
        } else if remaining.len() == 1 && parent == self.routing.root {
            let child = remaining[0];
            self.routing.nodes.remove(&parent);
            self.tombstones.insert(parent);
            self.set_owned_parent(child, None)?;
            self.routing.root = child;
            self.record(json!({"kind":"root-contracted","old":parent,"root":child}));
        } else {
            self.update_mean(parent)?;
        }
        Ok(())
    }
    pub(super) fn merge_owned(
        &mut self,
        leaf: u32,
        depth: u32,
        balancing: &mut BTreeSet<u32>,
    ) -> Result<()> {
        let _cost=self.balance_cost_span(super::balance_cost::Stage::Structure);
        if depth > 4 {
            return Ok(());
        }
        let old = self
            .routing
            .nodes
            .remove(&leaf)
            .context("merge topology missing")?;
        ensure!(old.children.is_none(), "merge requires leaf");
        // Reference discovery observes the source missing while its parent still
        // lists it. Navigation skips that missing child; there is no stale alias.
        let candidates = {let _discovery_cost=self.balance_cost_span(super::balance_cost::Stage::Discovery);self.routing.candidates(&old.centroid)?};
        let Some(target) = candidates
            .iter()
            .find(|(id, _)| *id != leaf)
            .map(|(id, _)| *id)
        else {
            self.routing.nodes.insert(leaf, old);
            return Ok(());
        };
        let target_center = self.routing.nodes[&target].centroid.clone();
        let source = *self
            .branches
            .get(&leaf)
            .context("merge source owner missing")?;
        let OwnerReply::Rows(rows) = self.structural(source, OwnerCommand::Detach(leaf))? else {
            bail!("merge source reply");
        };
        self.branches.remove(&leaf);
        self.tombstones.insert(leaf);
        if let Some(parent) = old.parent {
            self.remove_owned_child(parent, leaf, source)?;
        }
        self.record(json!({"kind":"merge","source":leaf,"source_owner":source,"target":target,"rows":rows.iter().map(|r|(r.id,r.version)).collect::<Vec<_>>()}));
        for row in rows {
            let _row_cost=self.balance_cost_span(super::balance_cost::Stage::Eligibility);
            if self.current_owned(row.id)? != row.version {
                continue;
            }
            let from = self.locations[&row.id].0;
            let lookup = self.lookup_at(from, row.id)?;
            let values: Vec<_> = lookup
                .vector_bits
                .iter()
                .map(|&bits| f32::from_bits(bits))
                .collect();
            let direct = self.routing.metric.distance(&values, &target_center)
                <= self.routing.metric.distance(&values, &old.centroid);
            if direct && self.branches.contains_key(&target) {
                let _registration_cost=self.balance_cost_span(super::balance_cost::Stage::Registration);
                let to = self.branches[&target];
                let Reply::Ack(reply) = self.owner(from)?.call(Command::Reroute {
                    id: row.id,
                    from_leaf: self.locations[&row.id].1,
                    to,
                    to_leaf: target,
                    vector: row.vector,
                    destination: self.owner(to)?.sender()?,
                    preserve_version: true,
                })?
                else {
                    bail!("merge direct registration reply");
                };
                self.locations.insert(row.id, (to, target));
                self.record(json!({"kind":"leaf_registered","leaf":target,"owner":to,"id":row.id,"version":reply["version"],"bits":lookup.vector_bits,"code":reply["code"]}));
            } else {
                self.reassign_owned(leaf, row.id, values, depth, balancing)?;
            }
        }
        self.balance_owned(target, depth + 1, balancing)?;
        Ok(())
    }
}
