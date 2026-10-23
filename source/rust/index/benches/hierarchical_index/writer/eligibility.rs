//! Shared reference eligibility arithmetic. Version checks, deduplication and
//! reassignment remain ordered by the caller.
use chroma_distance::DistanceFunction;
use chroma_index::quantization::{Code, QuantizedQuery};
use simsimd::SpatialSimilarity;

pub(super) fn fp_eligible(
    metric: &DistanceFunction,
    vector: &[f32],
    neighbor: &[f32],
    old: &[f32],
    left: &[f32],
    right: &[f32],
) -> bool {
    let left_dist = metric.distance(vector, left);
    let right_dist = metric.distance(vector, right);
    let neighbor_dist = metric.distance(vector, neighbor);
    if neighbor_dist <= left_dist && neighbor_dist <= right_dist {
        return false;
    }
    let old_dist = metric.distance(vector, old);
    !(old_dist <= left_dist && old_dist <= right_dist)
}
pub(super) struct QuantizedEligibility {
    left: QuantizedQuery,
    right: QuantizedQuery,
    neighbor: QuantizedQuery,
    old: QuantizedQuery,
}
impl QuantizedEligibility {
    pub fn new(
        neighbor: &[f32],
        old: &[f32],
        left: &[f32],
        right: &[f32],
        padded_bytes: usize,
    ) -> Self {
        let norm = |v: &[f32]| (f32::dot(v, v).unwrap_or(0.0) as f32).sqrt();
        let old_q_norm = norm(old);
        let left_q_norm = norm(left);
        let right_q_norm = norm(right);
        let c_norm = norm(neighbor);
        let old_r_q: Vec<_> = old.iter().zip(neighbor).map(|(a, b)| a - b).collect();
        let old_c_dot_q = f32::dot(neighbor, old).unwrap_or(0.0) as f32;
        let left_r_q: Vec<_> = left.iter().zip(neighbor).map(|(a, b)| a - b).collect();
        let left_c_dot_q = f32::dot(neighbor, left).unwrap_or(0.0) as f32;
        let right_r_q: Vec<_> = right.iter().zip(neighbor).map(|(a, b)| a - b).collect();
        let right_c_dot_q = f32::dot(neighbor, right).unwrap_or(0.0) as f32;
        let neighbor_r_q = vec![0.0f32; neighbor.len()];
        let neighbor_c_dot_q = c_norm * c_norm;
        let neighbor_q_norm = c_norm;
        let left = QuantizedQuery::new(&left_r_q, padded_bytes, c_norm, left_c_dot_q, left_q_norm);
        let right = QuantizedQuery::new(
            &right_r_q,
            padded_bytes,
            c_norm,
            right_c_dot_q,
            right_q_norm,
        );
        let neighbor = QuantizedQuery::new(
            &neighbor_r_q,
            padded_bytes,
            c_norm,
            neighbor_c_dot_q,
            neighbor_q_norm,
        );
        let old = QuantizedQuery::new(&old_r_q, padded_bytes, c_norm, old_c_dot_q, old_q_norm);
        Self {
            left,
            right,
            neighbor,
            old,
        }
    }
    pub fn eligible(&self, metric: &DistanceFunction, bytes: &[u8]) -> bool {
        let code = Code::<1, _>::new(bytes);
        let left_dist = code.distance_quantized_query(metric, &self.left);
        let right_dist = code.distance_quantized_query(metric, &self.right);
        let neighbor_dist = code.distance_quantized_query(metric, &self.neighbor);
        if neighbor_dist <= left_dist && neighbor_dist <= right_dist {
            return false;
        }
        let old_dist = code.distance_quantized_query(metric, &self.old);
        !(old_dist <= left_dist && old_dist <= right_dist)
    }
}
