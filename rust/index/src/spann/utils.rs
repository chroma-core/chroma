use std::{cmp::min, collections::HashMap};

use chroma_distance::DistanceFunction;
use rand::{seq::SliceRandom, Rng};

// TODO(Sanket): I don't understand why the reference implementation defined
// max_distance this way.
const MAX_DISTANCE: f32 = f32::MAX / 10.0;
const NUM_ITERS_FOR_CENTER_INIT: usize = 3;
const NUM_ITERS_FOR_MAIN_LOOP: usize = 100;
const NUM_ITERS_NO_IMPROVEMENT: usize = 5;

struct KMeansAlgorithmInput<'referred_data> {
    indices: Vec<u32>,
    embeddings: &'referred_data [f32],
    embedding_dimension: usize,
    k: usize,
    first: usize,
    // Exclusive range.
    last: usize,
    num_samples: usize,
    distance_function: DistanceFunction,
    initial_lambda: f32,
}

#[allow(dead_code)]
pub struct KMeansAlgorithmOutput {
    cluster_centers: Vec<Vec<f32>>,
    cluster_counts: Vec<usize>,
    cluster_labels: HashMap<u32, i32>,
}

pub struct KMeansAlgorithm<'referred_data> {
    input: KMeansAlgorithmInput<'referred_data>,
}

#[derive(Debug)]
struct KMeansAssignForCenterInitOutput {
    cluster_counts: Vec<usize>,
    cluster_weighted_counts: Vec<f32>,
    cluster_farthest_distance: Vec<f32>,
    total_distance: f32,
}

#[allow(dead_code)]
#[derive(Debug)]
struct KMeansAssignForMainLoopOutput {
    cluster_counts: Vec<usize>,
    cluster_farthest_point_idx: Vec<i32>,
    cluster_farthest_distance: Vec<f32>,
    cluster_new_centers: Vec<Vec<f32>>,
    total_distance: f32,
}

#[allow(dead_code)]
#[derive(Debug)]
struct KMeansAssignFinishOutput {
    cluster_counts: Vec<usize>,
    cluster_nearest_point_idx: Vec<i32>,
    cluster_nearest_distance: Vec<f32>,
    cluster_labels: HashMap<u32, i32>,
}

impl<'referred_data> KMeansAlgorithm<'referred_data> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        indices: Vec<u32>,
        embeddings: &'referred_data [f32],
        embedding_dimension: usize,
        k: usize,
        first: usize,
        last: usize,
        num_samples: usize,
        distance_function: DistanceFunction,
        initial_lambda: f32,
    ) -> Self {
        KMeansAlgorithm {
            input: KMeansAlgorithmInput {
                indices,
                embeddings,
                embedding_dimension,
                k,
                first,
                last,
                num_samples,
                distance_function,
                initial_lambda,
            },
        }
    }

    fn get_nearest_center(
        &self,
        centers: &[Vec<f32>],
        idx: usize,
        lambda: f32,
        previous_counts: &[usize],
    ) -> (i32, f32) {
        let point_idx = self.input.indices[idx];
        let dim = self.input.embedding_dimension;
        let start_idx = point_idx * dim as u32;
        let end_idx = (point_idx + 1) * dim as u32;
        let mut min_distance = MAX_DISTANCE;
        let mut min_center: i32 = -1;
        for center_idx in 0..self.input.k {
            let distance = self.input.distance_function.distance(
                &self.input.embeddings[start_idx as usize..end_idx as usize],
                &centers[center_idx],
            ) + lambda * previous_counts[center_idx] as f32;
            if distance > -MAX_DISTANCE && distance < min_distance {
                min_distance = distance;
                min_center = center_idx as i32;
            }
        }
        if min_center == -1 {
            panic!("Invariant violation. Every point should be assigned to a center.");
        }
        (min_center, min_distance)
    }

    fn kmeansassign_for_centerinit(&self, centers: &[Vec<f32>]) -> KMeansAssignForCenterInitOutput {
        // Assign to only a sample.
        let batch_end = min(self.input.first + self.input.num_samples, self.input.last);
        let mut cluster_counts = vec![0; self.input.k];
        let mut cluster_weighted_counts = vec![0.0; self.input.k];
        let mut cluster_farthest_distance = vec![-MAX_DISTANCE; self.input.k];
        let mut total_distance = 0.0;
        // Assign all points the nearest center.
        // TODO(Sanket): Normalize the points if needed for cosine similarity.
        // TODO(Sanket): Scope for perf improvements here. Like Paralleization, SIMD, etc.
        // Actual value of previous_counts does not matter since lambda is 0.
        let previous_counts = vec![0; self.input.k];
        for idx in self.input.first..batch_end {
            let (min_center, min_distance) =
                self.get_nearest_center(centers, idx, 0.0, &previous_counts);
            total_distance += min_distance;
            cluster_counts[min_center as usize] += 1;
            cluster_weighted_counts[min_center as usize] += min_distance;
            if min_distance > cluster_farthest_distance[min_center as usize] {
                cluster_farthest_distance[min_center as usize] = min_distance;
            }
        }
        KMeansAssignForCenterInitOutput {
            cluster_counts,
            cluster_weighted_counts,
            cluster_farthest_distance,
            total_distance,
        }
    }

    fn kmeansassign_for_main_loop(
        &self,
        centers: &[Vec<f32>],
        previous_counts: &[usize],
        lambda: f32,
    ) -> KMeansAssignForMainLoopOutput {
        let batch_end = min(self.input.last, self.input.first + self.input.num_samples);
        let dim = self.input.embedding_dimension;
        let mut cluster_counts = vec![0; self.input.k];
        let mut cluster_farthest_point_idx: Vec<i32> = vec![-1; self.input.k];
        let mut cluster_farthest_distance = vec![-MAX_DISTANCE; self.input.k];
        let mut cluster_new_centers = vec![vec![0.0; dim]; self.input.k];
        let mut total_distance = 0.0;
        // Assign all points the nearest center.
        // TODO(Sanket): Normalize the points if needed for cosine similarity.
        // TODO(Sanket): Scope for perf improvements here. Like Paralleization, SIMD, etc.
        for idx in self.input.first..batch_end {
            let (min_center, min_distance) =
                self.get_nearest_center(centers, idx, lambda, previous_counts);
            total_distance += min_distance;
            cluster_counts[min_center as usize] += 1;
            let point_idx = self.input.indices[idx];
            if min_distance > cluster_farthest_distance[min_center as usize] {
                cluster_farthest_point_idx[min_center as usize] = point_idx as i32;
                cluster_farthest_distance[min_center as usize] = min_distance;
            }
            let start_idx = point_idx * dim as u32;
            let end_idx = (point_idx + 1) * dim as u32;
            self.input.embeddings[start_idx as usize..end_idx as usize]
                .iter()
                .enumerate()
                .for_each(|(index, emb)| cluster_new_centers[min_center as usize][index] += *emb);
        }
        KMeansAssignForMainLoopOutput {
            cluster_counts,
            cluster_farthest_point_idx,
            cluster_farthest_distance,
            cluster_new_centers,
            total_distance,
        }
    }

    fn kmeansassign_finish(
        &self,
        centers: &[Vec<f32>],
        generate_labels: bool,
    ) -> KMeansAssignFinishOutput {
        // Assign all the points.
        let batch_end = self.input.last;
        let mut cluster_counts = vec![0; self.input.k];
        let mut cluster_nearest_point_idx: Vec<i32> = vec![-1; self.input.k];
        let mut cluster_nearest_distance = vec![MAX_DISTANCE; self.input.k];
        let mut cluster_labels;
        if generate_labels {
            cluster_labels = HashMap::with_capacity(batch_end - self.input.first);
        } else {
            cluster_labels = HashMap::new();
        }
        // Assign all points the nearest center.
        // TODO(Sanket): Normalize the points if needed for cosine similarity.
        // TODO(Sanket): Scope for perf improvements here. Like Paralleization, SIMD, etc.
        // The actual value of previous_counts does not matter since lambda is 0.
        let previous_counts = vec![0; self.input.k];
        for idx in self.input.first..batch_end {
            let (min_center, min_distance) =
                self.get_nearest_center(centers, idx, 0.0, &previous_counts);
            cluster_counts[min_center as usize] += 1;
            let point_idx = self.input.indices[idx];
            if min_distance <= cluster_nearest_distance[min_center as usize] {
                cluster_nearest_distance[min_center as usize] = min_distance;
                cluster_nearest_point_idx[min_center as usize] = point_idx as i32;
            }
            if generate_labels {
                cluster_labels.insert(point_idx, min_center);
            }
        }
        KMeansAssignFinishOutput {
            cluster_counts,
            cluster_nearest_point_idx,
            cluster_nearest_distance,
            cluster_labels,
        }
    }

    pub fn refine_lambda(
        &self,
        cluster_counts: &[usize],
        cluster_weighted_counts: &[f32],
        cluster_farthest_distance: &[f32],
    ) -> f32 {
        let batch_end = min(self.input.last, self.input.first + self.input.num_samples);
        let dataset_size = batch_end - self.input.first;
        let mut max_count = 0;
        let mut max_cluster: i32 = -1;
        // Find the cluster with the max count.
        for (index, count) in cluster_counts.iter().enumerate() {
            if *count > 0 && max_count < *count {
                max_count = *count;
                max_cluster = index as i32;
            }
        }
        if max_cluster < 0 {
            panic!("Invariant violation. There should be atleast one data point");
        }
        let avg_distance = cluster_weighted_counts[max_cluster as usize]
            / cluster_counts[max_cluster as usize] as f32;
        let lambda =
            (cluster_farthest_distance[max_cluster as usize] - avg_distance) / dataset_size as f32;
        f32::max(0.0, lambda)
    }

    pub fn init_centers(&self, num_iters: usize) -> (Vec<Vec<f32>>, Vec<usize>, f32) {
        let batch_end = min(self.input.first + self.input.num_samples, self.input.last);
        let mut min_dist = MAX_DISTANCE;
        let embedding_dim = self.input.k;
        let mut final_cluster_count = vec![0; embedding_dim];
        let mut final_centers = vec![vec![0.0; self.input.embedding_dimension]; embedding_dim];
        let mut lambda = 0.0;
        // Randomly choose centers.
        for _ in 0..num_iters {
            let mut centers = vec![vec![0.0; self.input.embedding_dimension]; self.input.k];
            for center in centers.iter_mut() {
                let random_center = rand::thread_rng().gen_range(self.input.first..batch_end);
                center.copy_from_slice(
                    &self.input.embeddings[self.input.indices[random_center] as usize
                        * self.input.embedding_dimension
                        ..((self.input.indices[random_center] + 1) as usize)
                            * self.input.embedding_dimension],
                );
            }
            let kmeans_assign = self.kmeansassign_for_centerinit(&centers);
            if kmeans_assign.total_distance < min_dist {
                min_dist = kmeans_assign.total_distance;
                final_cluster_count = kmeans_assign.cluster_counts;
                final_centers = centers;
                lambda = self.refine_lambda(
                    &final_cluster_count,
                    &kmeans_assign.cluster_weighted_counts,
                    &kmeans_assign.cluster_farthest_distance,
                );
            }
        }
        (final_centers, final_cluster_count, lambda)
    }

    fn refine_centers(
        &self,
        kmeansassign_output: &mut KMeansAssignForMainLoopOutput,
        previous_centers: &[Vec<f32>],
    ) -> f32 {
        let mut max_count = 0;
        let mut max_cluster_idx: i32 = -1;
        #[allow(clippy::needless_range_loop)]
        for cluster_idx in 0..self.input.k {
            let start = kmeansassign_output.cluster_farthest_point_idx[cluster_idx] as usize
                * self.input.embedding_dimension;
            let end = (kmeansassign_output.cluster_farthest_point_idx[cluster_idx] + 1) as usize
                * self.input.embedding_dimension;
            if kmeansassign_output.cluster_counts[cluster_idx] > 0
                && kmeansassign_output.cluster_counts[cluster_idx] > max_count
                && self.input.distance_function.distance(
                    &previous_centers[cluster_idx],
                    &self.input.embeddings[start..end],
                ) > 1e-6
            {
                max_count = kmeansassign_output.cluster_counts[cluster_idx];
                max_cluster_idx = cluster_idx as i32;
            }
        }

        // Refine centers.
        let mut diff = 0.0;
        #[allow(clippy::needless_range_loop)]
        for cluster_idx in 0..self.input.k {
            let count = kmeansassign_output.cluster_counts[cluster_idx];
            if count > 0 {
                kmeansassign_output.cluster_new_centers[cluster_idx]
                    .iter_mut()
                    .for_each(|x| {
                        *x /= count as f32;
                    });
            } else if max_cluster_idx == -1 {
                kmeansassign_output.cluster_new_centers[cluster_idx]
                    .copy_from_slice(&previous_centers[cluster_idx]);
            } else {
                // copy the farthest point embedding to the center.
                let start = kmeansassign_output.cluster_farthest_point_idx[max_cluster_idx as usize]
                    as usize
                    * self.input.embedding_dimension;
                let end = (kmeansassign_output.cluster_farthest_point_idx[max_cluster_idx as usize]
                    + 1) as usize
                    * self.input.embedding_dimension;
                kmeansassign_output.cluster_new_centers[cluster_idx]
                    .copy_from_slice(&self.input.embeddings[start..end]);
            }
            diff += self.input.distance_function.distance(
                &previous_centers[cluster_idx],
                &kmeansassign_output.cluster_new_centers[cluster_idx],
            );
        }
        diff
    }

    pub fn cluster(&mut self) -> KMeansAlgorithmOutput {
        let (initial_centers, initial_counts, adjusted_lambda) =
            self.init_centers(NUM_ITERS_FOR_CENTER_INIT);
        let end = min(self.input.last, self.input.first + self.input.num_samples);
        let baseline_lambda =
            1.0 * 1.0 / self.input.initial_lambda / (end - self.input.first) as f32;
        // Initialize.
        let mut current_centers = initial_centers;
        let mut current_counts = initial_counts;
        let mut min_dist = MAX_DISTANCE;
        let mut no_improvement = 0;
        let mut previous_centers = vec![];
        #[allow(unused_assignments)]
        let mut previous_counts = vec![];
        for _ in 0..NUM_ITERS_FOR_MAIN_LOOP {
            // Prepare for the next iteration.
            let previous_centers = current_centers;
            let previous_counts = current_counts;
            self.input.indices.shuffle(&mut rand::thread_rng());
            let mut kmeans_assign = self.kmeansassign_for_main_loop(
                &previous_centers,
                &previous_counts,
                f32::min(adjusted_lambda, baseline_lambda),
            );
            if kmeans_assign.total_distance < min_dist {
                min_dist = kmeans_assign.total_distance;
                no_improvement = 0;
            } else {
                no_improvement += 1;
            }
            let curr_diff = self.refine_centers(&mut kmeans_assign, &previous_centers);
            // Prepare for the next iteration.
            current_centers = kmeans_assign.cluster_new_centers;
            current_counts = kmeans_assign.cluster_counts;
            if curr_diff < 1e-3 || no_improvement >= NUM_ITERS_NO_IMPROVEMENT {
                break;
            }
        }
        // Assign points to the refined center one last time and get nearest points of each cluster.
        let kmeans_assign =
            self.kmeansassign_finish(&previous_centers, /* generate_labels */ false);
        #[allow(clippy::needless_range_loop)]
        for center_ids in 0..self.input.k {
            if kmeans_assign.cluster_nearest_point_idx[center_ids] >= 0 {
                let start_emb_idx = kmeans_assign.cluster_nearest_point_idx[center_ids] as usize
                    * self.input.embedding_dimension;
                let end_emb_idx = (kmeans_assign.cluster_nearest_point_idx[center_ids] as usize
                    + 1)
                    * self.input.embedding_dimension;
                previous_centers[center_ids]
                    .copy_from_slice(&self.input.embeddings[start_emb_idx..end_emb_idx]);
            }
        }
        // Finally assign points to these nearest points in the cluster.
        // Previous counts does not matter since lambda is 0.
        let kmeans_assign =
            self.kmeansassign_finish(&previous_centers, /* generate_labels */ true);
        previous_counts = kmeans_assign.cluster_counts;

        KMeansAlgorithmOutput {
            cluster_centers: previous_centers,
            cluster_counts: previous_counts,
            cluster_labels: kmeans_assign.cluster_labels,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::KMeansAlgorithm;

    #[test]
    fn test_kmeans_assign_for_center_init() {
        // 2D embeddings.
        let dim = 2;
        let embeddings = [
            0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 10.0, 10.0, 11.0, 10.0, 10.0, 11.0, 11.0, 11.0,
        ];
        let indices: Vec<u32> = vec![0, 1, 2, 3, 4, 5, 6, 7];
        let kmeans_algo = KMeansAlgorithm::new(
            indices,
            &embeddings,
            dim,
            2,
            0,
            8,
            1000,
            chroma_distance::DistanceFunction::Euclidean,
            100.0,
        );
        let centers = vec![vec![0.0, 0.0], vec![10.0, 10.0]];
        let res = kmeans_algo.kmeansassign_for_centerinit(&centers);
        assert_eq!(res.cluster_counts, vec![4, 4]);
        assert_eq!(res.total_distance, 8.0);
        assert_eq!(res.cluster_weighted_counts, vec![4.0, 4.0]);
        assert_eq!(res.cluster_farthest_distance, vec![2.0, 2.0]);
    }

    #[test]
    fn test_kmeans_assign_for_main_loop() {
        // 2D embeddings.
        let dim = 2;
        let embeddings = [
            0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 5.0, 5.0, 10.0, 10.0, 11.0, 10.0, 10.0, 11.0,
            11.0, 11.0,
        ];
        let indices: Vec<u32> = vec![0, 1, 2, 3, 4, 5, 6, 7, 8];
        let kmeans_algo = KMeansAlgorithm::new(
            indices,
            &embeddings,
            dim,
            2,
            0,
            9,
            1000,
            chroma_distance::DistanceFunction::Euclidean,
            100.0,
        );
        let centers = vec![vec![0.0, 0.0], vec![10.0, 10.0]];
        // Penalize [10.0, 10.0] so that [5.0, 5.0] gets assigned to [0.0, 0.0]
        let previous_counts = vec![0, 9];
        let lambda = 1.0;
        let res = kmeans_algo.kmeansassign_for_main_loop(&centers, &previous_counts, lambda);
        assert_eq!(res.cluster_counts, vec![5, 4]);
        assert_eq!(res.total_distance, 94.0);
        assert_eq!(res.cluster_farthest_distance, vec![50.0, 11.0]);
        assert_eq!(res.cluster_farthest_point_idx, vec![4, 8]);
        assert_eq!(
            res.cluster_new_centers,
            vec![vec![7.0, 7.0], vec![42.0, 42.0]]
        );
    }

    #[test]
    fn test_kmeans_assign_finish() {
        // 2D embeddings.
        let dim = 2;
        let embeddings = [
            0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 10.0, 10.0, 11.0, 10.0, 10.0, 11.0, 11.0, 11.0,
        ];
        let indices: Vec<u32> = vec![0, 1, 2, 3, 4, 5, 6, 7];
        let kmeans_algo = KMeansAlgorithm::new(
            indices,
            &embeddings,
            dim,
            2,
            0,
            8,
            1000,
            chroma_distance::DistanceFunction::Euclidean,
            100.0,
        );
        let centers = vec![vec![0.0, 0.0], vec![10.0, 10.0]];
        let mut res = kmeans_algo.kmeansassign_finish(&centers, false);
        assert_eq!(res.cluster_counts, vec![4, 4]);
        assert_eq!(res.cluster_nearest_distance, vec![0.0, 0.0]);
        assert_eq!(res.cluster_nearest_point_idx, vec![0, 4]);
        assert_eq!(res.cluster_labels.len(), 0);
        res = kmeans_algo.kmeansassign_finish(&centers, true);
        let mut labels = HashMap::new();
        labels.insert(0, 0);
        labels.insert(1, 0);
        labels.insert(2, 0);
        labels.insert(3, 0);
        labels.insert(4, 1);
        labels.insert(5, 1);
        labels.insert(6, 1);
        labels.insert(7, 1);
        assert_eq!(res.cluster_counts, vec![4, 4]);
        assert_eq!(res.cluster_nearest_distance, vec![0.0, 0.0]);
        assert_eq!(res.cluster_nearest_point_idx, vec![0, 4]);
        assert_eq!(res.cluster_labels, labels);
    }
}
