//! Deterministic, even placement through movement of existing branches. Refinement
//! changes ownership boundaries, never clustering, vector membership or IDs.
use super::protocol::{AuthorityId, NodeId};
use super::routing::NodeRef;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnershipUnit {
    pub root: NodeRef,
    pub vectors: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerBudget {
    pub owner: AuthorityId,
    pub vector_capacity: u64,
    /// Explicit headroom for the next insertion interval; not a prediction
    /// that every new input vector will be evenly distributed.
    pub next_interval_reserve: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedMove {
    pub root: NodeRef,
    pub destination: AuthorityId,
    pub vectors: u64,
    pub new_placement: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementPlan {
    pub promoted_shared: Vec<NodeId>,
    pub units: Vec<OwnershipUnit>,
    pub moves: Vec<PlannedMove>,
    pub final_vectors: BTreeMap<AuthorityId, u64>,
}

fn fraction(n: u64, numerator: u64, denominator: u64) -> u64 {
    ((n as u128 * numerator as u128) / denominator as u128) as u64
}

/// Ask for existing children only when a branch cannot fit. The caller obtains
/// these summaries from its current authority; no global posting snapshot is
/// needed. Returned units retain their source owner/counter until handoff.
pub fn plan<F>(
    mut units: Vec<OwnershipUnit>,
    budgets: &[OwnerBudget],
    mut children: F,
) -> Result<PlacementPlan, String>
where
    F: FnMut(NodeId) -> Result<Vec<OwnershipUnit>, String>,
{
    let by_owner = budgets
        .iter()
        .map(|b| (b.owner, b))
        .collect::<BTreeMap<_, _>>();
    if by_owner.len() != budgets.len() || budgets.is_empty() {
        return Err("owner budgets must be nonempty and unique".into());
    }
    if budgets
        .iter()
        .any(|b| b.vector_capacity == 0 || b.next_interval_reserve > b.vector_capacity)
    {
        return Err("invalid owner capacity/headroom".into());
    }
    let mut ids = BTreeSet::new();
    let mut loads = budgets
        .iter()
        .map(|b| (b.owner, 0_u64))
        .collect::<BTreeMap<_, _>>();
    for unit in &units {
        if !ids.insert(unit.root.id) {
            return Err("duplicate ownership root".into());
        }
        let load = loads
            .get_mut(&unit.root.authority)
            .ok_or("ownership root references unknown owner")?;
        *load = load
            .checked_add(unit.vectors)
            .ok_or("vector accounting overflow")?;
    }
    let mut result = PlacementPlan::default();
    let mut refined = BTreeSet::new();
    let mut moved = BTreeSet::new();
    // Each move strictly reduces the sum of squared owner loads. Capacity
    // ceilings retain headroom, and indivisible leaves bound residual imbalance.
    for _ in 0..1_000_000 {
        let ceiling = |owner: AuthorityId| {
            let budget = by_owner[&owner];
            fraction(budget.vector_capacity, 7, 10).saturating_sub(budget.next_interval_reserve)
        };
        let total: u128 = loads.values().map(|n| *n as u128).sum();
        if total
            > budgets
                .iter()
                .map(|b| ceiling(b.owner) as u128)
                .sum::<u128>()
        {
            return Err("insufficient aggregate receiving headroom under the 70% target".into());
        }
        let mut possible = Vec::new();
        for (index, unit) in units
            .iter()
            .enumerate()
            .filter(|(_, u)| u.vectors > 0 && !moved.contains(&u.root.id))
        {
            let source_load = loads[&unit.root.authority];
            for dest in budgets.iter().filter(|b| b.owner != unit.root.authority) {
                let dest_load = loads[&dest.owner];
                // Moving fewer than the load difference strictly improves evenness.
                if unit.vectors < source_load.saturating_sub(dest_load)
                    && dest_load
                        .checked_add(unit.vectors)
                        .is_some_and(|n| n <= ceiling(dest.owner))
                {
                    let improvement = 2u128
                        * unit.vectors as u128
                        * (source_load - dest_load - unit.vectors) as u128;
                    possible.push((
                        improvement,
                        std::cmp::Reverse(dest_load),
                        std::cmp::Reverse(unit.root.id),
                        std::cmp::Reverse(dest.owner),
                        index,
                        dest.owner,
                    ));
                }
            }
        }
        if let Some((_, _, _, _, index, destination)) = possible.into_iter().max() {
            let unit = &mut units[index];
            let old = unit.root;
            let next = old
                .placement
                .checked_add(1)
                .ok_or("placement counter exhausted")?;
            loads
                .entry(old.authority)
                .and_modify(|n| *n -= unit.vectors);
            loads.entry(destination).and_modify(|n| *n += unit.vectors);
            moved.insert(old.id);
            result.moves.push(PlannedMove {
                root: old,
                destination,
                vectors: unit.vectors,
                new_placement: next,
            });
            unit.root.authority = destination;
            unit.root.placement = next;
            // One plan does not shuttle a branch repeatedly between machines.
            continue;
        }
        let candidate = units
            .iter()
            .enumerate()
            .filter(|(_, u)| {
                budgets.iter().any(|b| {
                    b.owner != u.root.authority
                        && loads[&u.root.authority] > loads[&b.owner].saturating_add(1)
                        && loads[&b.owner] < ceiling(b.owner)
                }) && !refined.contains(&u.root.id)
                    && !moved.contains(&u.root.id)
            })
            .max_by_key(|(_, u)| (u.vectors, std::cmp::Reverse(u.root.id)))
            .map(|(i, _)| i);
        let Some(index) = candidate else {
            if budgets.iter().any(|b| loads[&b.owner] > ceiling(b.owner)) {
                return Err(
                    "no existing ownership refinement can provide capacity headroom".into(),
                );
            }
            result.units = units;
            result.final_vectors = loads;
            return Ok(result);
        };
        let unit = units[index].clone();
        refined.insert(unit.root.id);
        let descendants = children(unit.root.id)?;
        if descendants.is_empty() {
            continue;
        }
        if descendants.iter().any(|c| {
            c.root.authority != unit.root.authority
                || c.root.placement != unit.root.placement
                || c.root.id == unit.root.id
                || ids.contains(&c.root.id)
        }) {
            return Err("invalid existing-child refinement".into());
        }
        let total = descendants
            .iter()
            .try_fold(0u64, |n, c| n.checked_add(c.vectors))
            .ok_or("refinement accounting overflow")?;
        if total != unit.vectors {
            return Err("refinement changes membership count".into());
        }
        units.remove(index);
        ids.remove(&unit.root.id);
        for child in &descendants {
            if !ids.insert(child.root.id) {
                return Err("duplicate refined ownership root".into());
            }
        }
        result.promoted_shared.push(unit.root.id);
        units.extend(descendants);
    }
    Err("placement exceeded its progress bound".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn unit(id: u128, count: u64) -> OwnershipUnit {
        OwnershipUnit {
            root: NodeRef {
                id,
                authority: 1,
                placement: 0,
            },
            vectors: count,
        }
    }
    #[test]
    fn refines_existing_root_and_uses_both_owners() {
        let budgets = vec![
            OwnerBudget {
                owner: 1,
                vector_capacity: 80,
                next_interval_reserve: 0,
            },
            OwnerBudget {
                owner: 2,
                vector_capacity: 80,
                next_interval_reserve: 0,
            },
        ];
        let p = plan(vec![unit(1, 100)], &budgets, |id| {
            Ok(if id == 1 {
                vec![unit(2, 55), unit(3, 45)]
            } else {
                vec![]
            })
        })
        .unwrap();
        assert_eq!(p.promoted_shared, vec![1]);
        assert_eq!(p.moves.len(), 1);
        assert_eq!(p.final_vectors.values().sum::<u64>(), 100);
        assert!(p.final_vectors.values().all(|n| *n >= 30));
        assert_eq!(p.moves[0].root.id, 2);
    }
    #[test]
    fn rejects_missing_capacity_instead_of_using_idle_second_owner() {
        let budgets = vec![
            OwnerBudget {
                owner: 1,
                vector_capacity: 80,
                next_interval_reserve: 0,
            },
            OwnerBudget {
                owner: 2,
                vector_capacity: 80,
                next_interval_reserve: 0,
            },
        ];
        assert!(plan(vec![unit(1, 100)], &budgets, |_| Ok(vec![])).is_err());
    }
    #[test]
    fn triggered_source_continues_to_relief_target() {
        let budgets = vec![
            OwnerBudget {
                owner: 1,
                vector_capacity: 100,
                next_interval_reserve: 0,
            },
            OwnerBudget {
                owner: 2,
                vector_capacity: 100,
                next_interval_reserve: 0,
            },
        ];
        let p = plan(
            vec![unit(1, 25), unit(2, 25), unit(3, 25), unit(4, 25)],
            &budgets,
            |_| panic!("fitting units need no refinement"),
        )
        .unwrap();
        assert_eq!(p.moves.len(), 2);
        assert_eq!(p.final_vectors[&1], 50);
        assert_eq!(p.final_vectors[&2], 50);
        assert_eq!(
            p.moves
                .iter()
                .map(|m| m.root.id)
                .collect::<BTreeSet<_>>()
                .len(),
            2
        );
    }
    #[test]
    fn receiving70_target_cannot_be_replaced_by80_trigger() {
        let budgets = vec![
            OwnerBudget {
                owner: 1,
                vector_capacity: 100,
                next_interval_reserve: 0,
            },
            OwnerBudget {
                owner: 2,
                vector_capacity: 100,
                next_interval_reserve: 0,
            },
        ];
        let p = plan(vec![unit(1, 75), unit(2, 25)], &budgets, |id| {
            Ok(if id == 1 {
                vec![unit(3, 40), unit(4, 35)]
            } else {
                vec![]
            })
        })
        .unwrap();
        assert!(p.moves.iter().all(|m| m.vectors <= 70));
        assert_eq!(p.promoted_shared, vec![1]);
    }
}
