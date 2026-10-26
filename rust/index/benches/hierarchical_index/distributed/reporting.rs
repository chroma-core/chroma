//! Benchmark reports use decimal strings for node identities that exceed JSON's
//! integer representation. Internal records and RPC serialization keep their types.
use super::{placement::PlacementPlan, routing::NodeRef};
use serde_json::{json, Value};

fn node_ref_json(node: NodeRef) -> Value {
    json!({"id":node.id.to_string(),"authority":node.authority,"placement":node.placement})
}
/// Adapt the benchmark artifact only. Decimal node IDs survive Value conversion
/// and report consumers without changing placement records or RPC encoding.
pub fn placement_plan_json(plan: &PlacementPlan) -> Value {
    let promoted = plan
        .promoted_shared
        .iter()
        .map(u128::to_string)
        .collect::<Vec<_>>();
    let units = plan
        .units
        .iter()
        .map(|unit| json!({"root":node_ref_json(unit.root),"vectors":unit.vectors}))
        .collect::<Vec<_>>();
    let moves=plan.moves.iter().map(|movement|json!({"root":node_ref_json(movement.root),"destination":movement.destination,"vectors":movement.vectors,"new_placement":movement.new_placement})).collect::<Vec<_>>();
    json!({"promoted_shared":promoted,"units":units,"moves":moves,"final_vectors":plan.final_vectors,"node_id_encoding":"decimal strings"})
}
/// Preserve ordinary numeric counters while representing overflow exactly.
/// Summing u64 endpoint counters can exceed JSON Value's u64 integer range.
pub fn unsigned_count_json(value: u128) -> Value {
    match u64::try_from(value) {
        Ok(number) => json!(number),
        Err(_) => json!(value.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::super::placement::{OwnershipUnit, PlannedMove};
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn placement_report_preserves_wide_roots_promotions_moves_and_work_counts() {
        let root = NodeRef {
            id: (1u128 << 64) + 17,
            authority: 1,
            placement: 3,
        };
        let moved = NodeRef {
            id: (1u128 << 80) + 29,
            authority: 1,
            placement: 5,
        };
        let plan = PlacementPlan {
            promoted_shared: vec![root.id, u128::MAX],
            units: vec![OwnershipUnit {
                root,
                vectors: 60_000,
            }],
            moves: vec![PlannedMove {
                root: moved,
                destination: 2,
                vectors: 40_000,
                new_placement: 6,
            }],
            final_vectors: BTreeMap::from([(1, 60_000), (2, 40_000)]),
        };
        let report = placement_plan_json(&plan);
        let encoded = serde_json::to_vec(&report).unwrap();
        let report: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            report["promoted_shared"],
            json!([root.id.to_string(), u128::MAX.to_string()])
        );
        assert_eq!(
            report["units"][0]["root"],
            json!({"id":root.id.to_string(),"authority":1,"placement":3})
        );
        assert_eq!(report["units"][0]["vectors"], 60_000);
        assert_eq!(
            report["moves"][0],
            json!({"root":{"id":moved.id.to_string(),"authority":1,"placement":5},"destination":2,"vectors":40_000,"new_placement":6})
        );
        assert_eq!(report["final_vectors"], json!({"1":60_000,"2":40_000}));
        // Report adaptation does not alter the numeric model or RPC encoding.
        let wire = serde_json::to_vec(&plan).unwrap();
        let restored: PlacementPlan = serde_json::from_slice(&wire).unwrap();
        assert_eq!(restored, plan);
        assert!(plan.units[0].root.id > u64::MAX as u128);
    }
}

#[cfg(test)]
mod counter_tests {
    use super::*;
    #[test]
    fn aggregate_byte_counts_remain_exact_at_and_above_the_json_number_limit() {
        assert_eq!(unsigned_count_json(42), json!(42));
        assert_eq!(unsigned_count_json(u64::MAX as u128), json!(u64::MAX));
        let total = 4 * u64::MAX as u128;
        let report = json!({"sent":unsigned_count_json(2*u64::MAX as u128),"total":unsigned_count_json(total)});
        let encoded = serde_json::to_vec(&report).unwrap();
        let decoded: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            decoded["total"].as_str().unwrap().parse::<u128>().unwrap(),
            total
        );
    }
}
