use std::{cmp::min, collections::HashMap, sync::Arc};

use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::QuantizedCluster;
use rand::{seq::IteratorRandom, seq::SliceRandom, thread_rng, Rng};
use simsimd::SpatialSimilarity;
use thiserror::Error;

use crate::{hnsw_provider::HnswIndexRef, quantization::Code, SearchResult};

// TODO(Sanket): I don't understand why the reference implementation defined
// max_distance this way.
// TODO(Sanket): Make these configurable.
const MAX_DISTANCE: f32 = f32::MAX / 10.0;
const NUM_ITERS_FOR_CENTER_INIT: usize = 3;
const NUM_ITERS_FOR_MAIN_LOOP: usize = 100;
const NUM_ITERS_NO_IMPROVEMENT: usize = 5;

/// The input for kmeans algorithm.
/// - indices: The indices of the embeddings that we want to cluster.
/// - embeddings: The entire list of embeddings. We only cluster a subset from this
///   list based on the indices. This is a flattened out list so for e.g.
///   the first embedding will be stored from 0..embedding_dimension, the second
///   from embedding_dimension..2*embedding_dimension and so on.
/// - embedding_dimension: The dimension of the embeddings.
/// - k: The number of clusters.
/// - first: The start index in the indices array from where we start clustering.
/// - last: The end index in the indices array till where we cluster. It excludes this index.
/// - num_samples: Each run of kmeans only clusters num_samples number of points. This is
///   done to speed up clustering without losing much accuracy. In the end, we cluster all
///   the points.
/// - distance_function: The distance function to use for clustering.
/// - initial_lambda: Lambda is a parameter used to penalize large clusters. This is used
///   to generate balanced clusters. The algorithm generate a lambda on the fly using this
///   initial_lambda as the starting point.
pub struct KMeansAlgorithmInput<'referred_data> {
    indices: Vec<usize>,
    embeddings: &'referred_data [Arc<[f32]>],
    embedding_dimension: usize,
    k: usize,
    first: usize,
    // Exclusive range.
    last: usize,
    num_samples: usize,
    distance_function: DistanceFunction,
    initial_lambda: f32,
}

impl<'referred_data> KMeansAlgorithmInput<'referred_data> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        indices: Vec<usize>,
        embeddings: &'referred_data Vec<Arc<[f32]>>,
        embedding_dimension: usize,
        k: usize,
        first: usize,
        last: usize,
        num_samples: usize,
        distance_function: DistanceFunction,
        initial_lambda: f32,
    ) -> Self {
        KMeansAlgorithmInput {
            indices,
            embeddings,
            embedding_dimension,
            k,
            first,
            last,
            num_samples,
            distance_function,
            initial_lambda,
        }
    }
}

/// The output from kmeans.
/// - cluster_centers: The embeddings of the centers of the clusters.
/// - cluster_center_vector_ids: The point index (into input.embeddings) whose embedding is the center.
///   -1 if no points assigned to the cluster.
/// - cluster_counts: The number of points in each cluster.
/// - cluster_labels: The mapping of each point to the cluster it belongs to. Clusters are
///   identified by unsigned integers starting from 0. These ids are also indexes in the
///   cluster_centers and cluster_counts arrays.
#[allow(dead_code)]
#[derive(Debug)]
pub struct KMeansAlgorithmOutput {
    pub cluster_centers: Vec<Arc<[f32]>>,
    pub cluster_center_vector_ids: Vec<i32>,
    pub cluster_counts: Vec<usize>,
    pub cluster_labels: HashMap<usize, i32>,
    pub num_clusters: usize,
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
    cluster_labels: HashMap<usize, i32>,
}

#[derive(Error, Debug)]
pub enum KMeansError {
    #[error("There should be at least one cluster")]
    MaxClusterNotFound,
    #[error("Could not assign a point to a center")]
    PointAssignmentFailed,
    #[error("Returned 0 points in a cluster")]
    ZeroPointsInCluster,
}

impl ChromaError for KMeansError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::MaxClusterNotFound => ErrorCodes::Internal,
            Self::PointAssignmentFailed => ErrorCodes::Internal,
            Self::ZeroPointsInCluster => ErrorCodes::Internal,
        }
    }
}

// For a given point, get the nearest center and the distance to it.
// lambda is a parameter used to penalize large clusters.
// previous_counts is the number of points in each cluster in the previous iteration.
fn get_nearest_center<T: AsRef<[f32]>>(
    input: &KMeansAlgorithmInput,
    centers: &[T],
    idx: usize,
    lambda: f32,
    previous_counts: &[usize],
) -> Result<(i32, f32), KMeansError> {
    let point_idx = input.indices[idx];
    let mut min_distance = MAX_DISTANCE;
    let mut min_center: i32 = -1;
    for center_idx in 0..input.k {
        let distance = input
            .distance_function
            .distance(&input.embeddings[point_idx], centers[center_idx].as_ref())
            + lambda * previous_counts[center_idx] as f32;
        if distance > -MAX_DISTANCE && distance < min_distance {
            min_distance = distance;
            min_center = center_idx as i32;
        }
    }
    if min_center == -1 {
        return Err(KMeansError::PointAssignmentFailed);
    }
    Ok((min_center, min_distance))
}

// Center init is a process of choosing the initial centers for the kmeans algorithm.
// This function assigns all the points to their respective nearest centers without any regularization
// i.e. without penalizing large clusters using a regularization parameter like lambda.
fn kmeansassign_for_centerinit(
    input: &KMeansAlgorithmInput,
    centers: &[Vec<f32>],
) -> Result<KMeansAssignForCenterInitOutput, KMeansError> {
    // Assign to only a sample.
    let batch_end = min(input.first + input.num_samples, input.last);
    // Number of points in each cluster.
    let mut cluster_counts = vec![0; input.k];
    // Weighted counts are the sum of distances of all points assigned to a cluster.
    let mut cluster_weighted_counts = vec![0.0; input.k];
    // Distance of the farthest point from the cluster center.
    let mut cluster_farthest_distance = vec![-MAX_DISTANCE; input.k];
    // Sum of distances of all points to their nearest centers.
    let mut total_distance = 0.0;
    // Assign all points their nearest centers.
    // TODO(Sanket): Scope for perf improvements here. Like Paralleization, SIMD, etc.
    // Actual value of previous_counts does not matter since lambda is 0.
    // Passing a vector of 0s.
    let previous_counts = vec![0; input.k];
    for idx in input.first..batch_end {
        let (min_center, min_distance) =
            get_nearest_center(input, centers, idx, /* lambda */ 0.0, &previous_counts)?;
        total_distance += min_distance;
        cluster_counts[min_center as usize] += 1;
        cluster_weighted_counts[min_center as usize] += min_distance;
        if min_distance > cluster_farthest_distance[min_center as usize] {
            cluster_farthest_distance[min_center as usize] = min_distance;
        }
    }
    Ok(KMeansAssignForCenterInitOutput {
        cluster_counts,
        cluster_weighted_counts,
        cluster_farthest_distance,
        total_distance,
    })
}

// This function assigns all the points to their respective nearest centers with regularization
// i.e. it penalizes large clusters using a regularization parameter like lambda.
fn kmeansassign_for_main_loop(
    input: &KMeansAlgorithmInput,
    centers: &[Vec<f32>],
    previous_counts: &[usize],
    lambda: f32,
) -> Result<KMeansAssignForMainLoopOutput, KMeansError> {
    let batch_end = min(input.last, input.first + input.num_samples);
    let dim = input.embedding_dimension;
    // Number of points in each cluster.
    let mut cluster_counts = vec![0; input.k];
    // Index of the farthest point from the cluster center.
    let mut cluster_farthest_point_idx: Vec<i32> = vec![-1; input.k];
    // Distance of the farthest point from the cluster center.
    let mut cluster_farthest_distance = vec![-MAX_DISTANCE; input.k];
    // New centers for each cluster. This is simply the sum of embeddings of all points
    // that belong to the cluster.
    let mut cluster_new_centers = vec![vec![0.0; dim]; input.k];
    // Sum of distances of all points to their nearest centers.
    let mut total_distance = 0.0;
    // Assign all points the nearest center.
    // TODO(Sanket): Scope for perf improvements here. Like Paralleization, SIMD, etc.
    for idx in input.first..batch_end {
        let (min_center, min_distance) =
            get_nearest_center(input, centers, idx, lambda, previous_counts)?;
        total_distance += min_distance;
        cluster_counts[min_center as usize] += 1;
        let point_idx = input.indices[idx];
        if min_distance > cluster_farthest_distance[min_center as usize] {
            cluster_farthest_point_idx[min_center as usize] = point_idx as i32;
            cluster_farthest_distance[min_center as usize] = min_distance;
        }
        input.embeddings[point_idx]
            .iter()
            .enumerate()
            .for_each(|(index, emb)| cluster_new_centers[min_center as usize][index] += *emb);
    }
    Ok(KMeansAssignForMainLoopOutput {
        cluster_counts,
        cluster_farthest_point_idx,
        cluster_farthest_distance,
        cluster_new_centers,
        total_distance,
    })
}

// This is run in the end to assign all points to their nearest centers.
// It does not penalize the clusters via lambda. Also, it assigns ALL the
// points instead of just a sample.
// generate_labels is used to denote if this method is expected to also return
// the assignment labels of the points.
fn kmeansassign_finish<T: AsRef<[f32]>>(
    input: &KMeansAlgorithmInput,
    centers: &[T],
    generate_labels: bool,
) -> Result<KMeansAssignFinishOutput, KMeansError> {
    // Assign ALL the points.
    let batch_end = input.last;
    // Number of points in each cluster.
    let mut cluster_counts = vec![0; input.k];
    // Index and Distance of the nearest point from the cluster center.
    let mut cluster_nearest_point_idx: Vec<i32> = vec![-1; input.k];
    let mut cluster_nearest_distance = vec![MAX_DISTANCE; input.k];
    // Point id -> label id mapping for the cluster assignment.
    let mut cluster_labels;
    if generate_labels {
        cluster_labels = HashMap::with_capacity(batch_end - input.first);
    } else {
        cluster_labels = HashMap::new();
    }
    // Assign all points the nearest center.
    // TODO(Sanket): Scope for perf improvements here. Like Paralleization, SIMD, etc.
    // The actual value of previous_counts does not matter since lambda is 0. Using a vector of 0s.
    let previous_counts = vec![0; input.k];
    for idx in input.first..batch_end {
        let (min_center, min_distance) =
            get_nearest_center(input, centers, idx, 0.0, &previous_counts)?;
        cluster_counts[min_center as usize] += 1;
        let point_idx = input.indices[idx];
        if min_distance <= cluster_nearest_distance[min_center as usize] {
            cluster_nearest_distance[min_center as usize] = min_distance;
            cluster_nearest_point_idx[min_center as usize] = point_idx as i32;
        }
        if generate_labels {
            cluster_labels.insert(point_idx, min_center);
        }
    }
    Ok(KMeansAssignFinishOutput {
        cluster_counts,
        cluster_nearest_point_idx,
        cluster_nearest_distance,
        cluster_labels,
    })
}

fn refine_lambda(
    input: &KMeansAlgorithmInput,
    cluster_counts: &[usize],
    cluster_weighted_counts: &[f32],
    cluster_farthest_distance: &[f32],
) -> Result<f32, KMeansError> {
    let batch_end = min(input.last, input.first + input.num_samples);
    let dataset_size = batch_end - input.first;
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
        return Err(KMeansError::MaxClusterNotFound);
    }
    let avg_distance =
        cluster_weighted_counts[max_cluster as usize] / cluster_counts[max_cluster as usize] as f32;
    let lambda =
        (cluster_farthest_distance[max_cluster as usize] - avg_distance) / dataset_size as f32;
    Ok(f32::max(0.0, lambda))
}

// This function initializes the centers for the kmeans algorithm.
// It runs the kmeans algorithm multiple times with random centers
// and chooses the centers that give the minimum distance.
// It also computes lambda from the chosen centers.
#[allow(clippy::type_complexity)]
fn init_centers(
    input: &KMeansAlgorithmInput,
    num_iters: usize,
) -> Result<(Vec<Vec<f32>>, Vec<usize>, f32), KMeansError> {
    let batch_end = min(input.first + input.num_samples, input.last);
    let mut min_dist = MAX_DISTANCE;
    let mut final_cluster_count = vec![0; input.k];
    let mut final_centers = vec![vec![0.0; input.embedding_dimension]; input.k];
    let mut lambda = 0.0;
    // Randomly choose centers.
    for _ in 0..num_iters {
        // TODO(Sanket): Instead of copying full centers, we can use indices to avoid copying.
        let mut centers = vec![vec![0.0; input.embedding_dimension]; input.k];
        for center in centers.iter_mut() {
            let random_center = rand::thread_rng().gen_range(input.first..batch_end);
            center.copy_from_slice(&input.embeddings[input.indices[random_center]]);
        }
        let kmeans_assign = kmeansassign_for_centerinit(input, &centers)?;
        if kmeans_assign.total_distance < min_dist {
            min_dist = kmeans_assign.total_distance;
            final_cluster_count = kmeans_assign.cluster_counts;
            final_centers = centers;
            lambda = refine_lambda(
                input,
                &final_cluster_count,
                &kmeans_assign.cluster_weighted_counts,
                &kmeans_assign.cluster_farthest_distance,
            )?;
        }
    }
    Ok((final_centers, final_cluster_count, lambda))
}

// This function refines the centers of the clusters.
// It calculates the new centers by averaging the embeddings of all points
// assigned to a cluster.
fn refine_centers(
    input: &KMeansAlgorithmInput,
    kmeansassign_output: &mut KMeansAssignForMainLoopOutput,
    previous_centers: &[Vec<f32>],
) -> f32 {
    let mut max_count = 0;
    let mut max_cluster_idx: i32 = -1;
    #[allow(clippy::needless_range_loop)]
    for cluster_idx in 0..input.k {
        if kmeansassign_output.cluster_counts[cluster_idx] > 0
            && kmeansassign_output.cluster_counts[cluster_idx] > max_count
            && input.distance_function.distance(
                &previous_centers[cluster_idx],
                &input.embeddings
                    [kmeansassign_output.cluster_farthest_point_idx[cluster_idx] as usize],
            ) > 1e-6
        {
            max_count = kmeansassign_output.cluster_counts[cluster_idx];
            max_cluster_idx = cluster_idx as i32;
        }
    }

    // Refine centers.
    let mut diff = 0.0;
    #[allow(clippy::needless_range_loop)]
    for cluster_idx in 0..input.k {
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
            kmeansassign_output.cluster_new_centers[cluster_idx].copy_from_slice(
                &input.embeddings[kmeansassign_output.cluster_farthest_point_idx
                    [max_cluster_idx as usize] as usize],
            );
        }
        diff += input.distance_function.distance(
            &previous_centers[cluster_idx],
            &kmeansassign_output.cluster_new_centers[cluster_idx],
        );
    }
    diff
}

pub fn cluster(input: &mut KMeansAlgorithmInput) -> Result<KMeansAlgorithmOutput, KMeansError> {
    let (initial_centers, initial_counts, adjusted_lambda) =
        init_centers(input, NUM_ITERS_FOR_CENTER_INIT)?;
    let end = min(input.last, input.first + input.num_samples);
    let baseline_lambda = 1.0 * 1.0 / input.initial_lambda / (end - input.first) as f32;
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
        previous_centers = current_centers;
        previous_counts = current_counts;
        input.indices[input.first..input.last].shuffle(&mut rand::thread_rng());
        let mut kmeans_assign = kmeansassign_for_main_loop(
            input,
            &previous_centers,
            &previous_counts,
            f32::min(adjusted_lambda, baseline_lambda),
        )?;
        if kmeans_assign.total_distance < min_dist {
            min_dist = kmeans_assign.total_distance;
            no_improvement = 0;
        } else {
            no_improvement += 1;
        }
        let curr_diff = refine_centers(input, &mut kmeans_assign, &previous_centers);
        // Prepare for the next iteration.
        current_centers = kmeans_assign.cluster_new_centers;
        current_counts = kmeans_assign.cluster_counts;
        if curr_diff < 1e-3 || no_improvement >= NUM_ITERS_NO_IMPROVEMENT {
            break;
        }
    }
    // Assign points to the refined center one last time and get nearest points of each cluster.
    let kmeans_assign =
        kmeansassign_finish(input, &previous_centers, /* generate_labels */ false)?;
    let mut final_centers = Vec::with_capacity(input.k);
    let mut final_center_vector_ids = Vec::with_capacity(input.k);
    #[allow(clippy::needless_range_loop)]
    for center_ids in 0..input.k {
        if kmeans_assign.cluster_nearest_point_idx[center_ids] >= 0 {
            final_centers.push(
                input.embeddings[kmeans_assign.cluster_nearest_point_idx[center_ids] as usize]
                    .clone(),
            );
            final_center_vector_ids.push(kmeans_assign.cluster_nearest_point_idx[center_ids]);
        } else {
            // Arc::from(Vec) = takes ownership of Vec's buffer, zero data copy
            final_centers.push(Arc::from(std::mem::take(&mut previous_centers[center_ids])));
            final_center_vector_ids.push(-1);
        }
    }
    // Finally assign points to these nearest points in the cluster.
    // Previous counts does not matter since lambda is 0.
    let kmeans_assign =
        kmeansassign_finish(input, &final_centers, /* generate_labels */ true)?;
    previous_counts = kmeans_assign.cluster_counts;
    let mut total_non_zero_clusters = 0;
    for count in previous_counts.iter() {
        if *count > 0 {
            total_non_zero_clusters += 1;
        }
    }

    Ok(KMeansAlgorithmOutput {
        cluster_centers: final_centers,
        cluster_center_vector_ids: final_center_vector_ids,
        cluster_counts: previous_counts,
        cluster_labels: kmeans_assign.cluster_labels,
        num_clusters: total_non_zero_clusters,
    })
}

#[derive(Error, Debug)]
pub enum RngQueryError {
    #[error("Error searching Hnsw graph")]
    HnswSearchError,
}

impl ChromaError for RngQueryError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::HnswSearchError => ErrorCodes::Internal,
        }
    }
}

// Assumes that query is already normalized.
#[allow(clippy::too_many_arguments)]
pub async fn rng_query(
    normalized_query: &[f32],
    hnsw_index: HnswIndexRef,
    k: usize,
    replica_count: Option<usize>,
    rng_epsilon: f32,
    rng_factor: f32,
    distance_function: DistanceFunction,
    apply_rng_rule: bool,
) -> Result<(Vec<usize>, Vec<f32>, Vec<Vec<f32>>), RngQueryError> {
    let mut nearby_ids: Vec<usize> = vec![];
    let mut nearby_distances: Vec<f32> = vec![];
    let mut embeddings: Vec<Vec<f32>> = vec![];
    {
        let read_guard = hnsw_index.inner.read();
        let allowed_ids = vec![];
        let disallowed_ids = vec![];
        let (ids, distances) = read_guard
            .hnsw_index
            .query(normalized_query, k, &allowed_ids, &disallowed_ids)
            .map_err(|_| RngQueryError::HnswSearchError)?;
        for (id, distance) in ids.iter().zip(distances.iter()) {
            let within_epsilon = if distances[0] < 0.0 && *distance < 0.0 {
                // Both negative: reverse the comparison
                *distance >= (1_f32 + rng_epsilon) * distances[0]
            } else {
                // At least one is non-negative: use normal comparison
                *distance <= (1_f32 + rng_epsilon) * distances[0]
            };

            if !apply_rng_rule || within_epsilon {
                nearby_ids.push(*id);
                nearby_distances.push(*distance);
            }
        }
        // Get the embeddings also for distance computation.
        for id in nearby_ids.iter() {
            let emb = read_guard
                .hnsw_index
                .get(*id)
                .map_err(|_| RngQueryError::HnswSearchError)?
                .ok_or(RngQueryError::HnswSearchError)?;
            embeddings.push(emb);
        }
    }
    if !apply_rng_rule {
        return Ok((nearby_ids, nearby_distances, embeddings));
    }
    // Apply the RNG rule to prune.
    let mut res_ids = vec![];
    let mut res_distances = vec![];
    let mut res_embeddings: Vec<Vec<f32>> = vec![];
    // Embeddings that were obtained are already normalized.
    for (id, (distance, embedding)) in nearby_ids
        .iter()
        .zip(nearby_distances.iter().zip(embeddings))
    {
        if let Some(replica_count) = replica_count {
            if res_ids.len() >= replica_count {
                break;
            }
        }
        let mut rng_accepted = true;
        for nbr_embedding in res_embeddings.iter() {
            let dist = distance_function.distance(&embedding[..], &nbr_embedding[..]);

            let fails_check = if dist < 0.0 && *distance < 0.0 {
                // Both negative: reverse the comparison
                rng_factor * dist >= *distance
            } else {
                // At least one is non-negative: use normal comparison
                rng_factor * dist <= *distance
            };

            if fails_check {
                rng_accepted = false;
                break;
            }
        }
        if !rng_accepted {
            continue;
        }
        res_ids.push(*id);
        res_distances.push(*distance);
        res_embeddings.push(embedding);
    }

    Ok((res_ids, res_distances, res_embeddings))
}

/// Split a set of embeddings into two groups using 2-means clustering.
///
/// Returns (left_center, left_group, right_center, right_group) where centers
/// are the nearest actual vectors to the computed cluster centroids.
/// Each element in the groups is (id, version, embedding).
pub fn split(
    embeddings: Vec<(u32, u32, Arc<[f32]>)>,
    distance_function: &DistanceFunction,
) -> (
    Arc<[f32]>,
    Vec<(u32, u32, Arc<[f32]>)>,
    Arc<[f32]>,
    Vec<(u32, u32, Arc<[f32]>)>,
) {
    let n = embeddings.len();

    if n < 2 {
        let dim = embeddings.first().map(|(_, _, e)| e.len()).unwrap_or(0);
        let c = Arc::<[f32]>::from(vec![0.0; dim]);
        return (c.clone(), embeddings, c, Vec::new());
    }

    let dim = embeddings[0].2.len();

    // Initialization: try 4 random seeds, keep best
    let mut rng = thread_rng();
    let mut best_c_0 = embeddings[0].2.as_ref();
    let mut best_c_1 = embeddings[1].2.as_ref();
    let mut best_total_dist = f32::MAX;

    for _ in 0..4 {
        let picked = embeddings.iter().choose_multiple(&mut rng, 2);
        let c_0 = picked[0].2.as_ref();
        let c_1 = picked[1].2.as_ref();

        let total_dist: f32 = embeddings
            .iter()
            .map(|(_, _, e)| {
                distance_function
                    .distance(e, c_0)
                    .min(distance_function.distance(e, c_1))
            })
            .sum();

        if total_dist < best_total_dist {
            best_total_dist = total_dist;
            best_c_0 = picked[0].2.as_ref();
            best_c_1 = picked[1].2.as_ref();
        }
    }

    let mut c_0 = best_c_0.to_vec();
    let mut c_1 = best_c_1.to_vec();

    // 2-means iteration
    let mut labels = vec![false; n];
    let mut prev_total_dist = f32::MAX;
    let mut no_improvement = 0;

    for _ in 0..128 {
        // Assignment
        let mut total_dist = 0.0;
        for (i, (_, _, e)) in embeddings.iter().enumerate() {
            let dist_0 = distance_function.distance(e, &c_0);
            let dist_1 = distance_function.distance(e, &c_1);
            labels[i] = dist_1 < dist_0;
            total_dist += dist_0.min(dist_1);
        }

        // Update centers (inline average)
        let mut new_c_0 = vec![0.0; dim];
        let mut new_c_1 = vec![0.0; dim];
        let mut count_0 = 0usize;
        let mut count_1 = 0usize;

        for (i, (_, _, e)) in embeddings.iter().enumerate() {
            if labels[i] {
                for (j, v) in e.iter().enumerate() {
                    new_c_1[j] += v;
                }
                count_1 += 1;
            } else {
                for (j, v) in e.iter().enumerate() {
                    new_c_0[j] += v;
                }
                count_0 += 1;
            }
        }

        if count_0 > 0 {
            new_c_0.iter_mut().for_each(|v| *v /= count_0 as f32);
        }
        if count_1 > 0 {
            new_c_1.iter_mut().for_each(|v| *v /= count_1 as f32);
        }

        // Check convergence
        let c_0_c_1_dist = distance_function.distance(&c_0, &c_1);
        let relative_diff = if c_0_c_1_dist > f32::EPSILON {
            (distance_function.distance(&c_0, &new_c_0)
                + distance_function.distance(&c_1, &new_c_1))
                / c_0_c_1_dist
        } else {
            0.0
        };

        c_0 = new_c_0;
        c_1 = new_c_1;

        if relative_diff < f32::EPSILON {
            break;
        }

        if total_dist >= prev_total_dist {
            no_improvement += 1;
            if no_improvement >= 4 {
                break;
            }
        } else {
            no_improvement = 0;
        }
        prev_total_dist = total_dist;
    }

    // Find nearest actual vectors as centers
    let mut nearest_0_idx = 0;
    let mut nearest_0_dist = f32::MAX;
    let mut nearest_1_idx = 0;
    let mut nearest_1_dist = f32::MAX;

    for (i, (_, _, e)) in embeddings.iter().enumerate() {
        let dist_0 = distance_function.distance(e, &c_0);
        let dist_1 = distance_function.distance(e, &c_1);

        if !labels[i] && dist_0 < nearest_0_dist {
            nearest_0_dist = dist_0;
            nearest_0_idx = i;
        }
        if labels[i] && dist_1 < nearest_1_dist {
            nearest_1_dist = dist_1;
            nearest_1_idx = i;
        }
    }

    let left_center = embeddings[nearest_0_idx].2.clone();
    let right_center = embeddings[nearest_1_idx].2.clone();

    // Build output groups
    let count_0 = labels.iter().filter(|&&l| !l).count();
    let count_1 = n - count_0;

    let mut group_0 = Vec::with_capacity(count_0);
    let mut group_1 = Vec::with_capacity(count_1);

    for ((id, version, embedding), label) in embeddings.into_iter().zip(labels) {
        if label {
            group_1.push((id, version, embedding));
        } else {
            group_0.push((id, version, embedding));
        }
    }

    (left_center, group_0, right_center, group_1)
}

/// Query a quantized cluster, returning all points sorted by estimated distance.
///
/// Uses RaBitQ distance estimation between the query and each quantized point.
pub fn query_quantized_cluster(
    cluster: &QuantizedCluster<'_>,
    query: &[f32],
    distance_function: &DistanceFunction,
) -> SearchResult {
    let dim = cluster.center.len();
    if cluster.ids.is_empty() || dim == 0 {
        return SearchResult::default();
    }

    // Precompute query-related values
    let c_norm = (f32::dot(cluster.center, cluster.center).unwrap_or(0.0) as f32).sqrt();
    let c_dot_q = f32::dot(cluster.center, query).unwrap_or(0.0) as f32;
    let q_norm = (f32::dot(query, query).unwrap_or(0.0) as f32).sqrt();
    let r_q = query
        .iter()
        .zip(cluster.center.iter())
        .map(|(q, c)| q - c)
        .collect::<Vec<_>>();

    // Compute distances for each point
    let code_size = cluster.codes.len() / cluster.ids.len().max(1);
    let (keys, distances): (Vec<u32>, Vec<f32>) = cluster
        .ids
        .iter()
        .zip(cluster.codes.chunks(code_size))
        .map(|(id, code_bytes)| {
            let code = Code::<&[u8]>::new(code_bytes);
            let distance = code.distance_query(distance_function, &r_q, c_norm, c_dot_q, q_norm);
            (*id as u32, distance)
        })
        .unzip();

    SearchResult { keys, distances }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::spann::utils::{
        cluster, kmeansassign_finish, kmeansassign_for_centerinit, kmeansassign_for_main_loop,
        KMeansAlgorithmInput,
    };

    #[test]
    fn test_kmeans_assign_for_center_init() {
        // 2D embeddings.
        let dim = 2;
        let embeddings: Vec<Arc<[f32]>> = vec![
            Arc::from([-1.0_f32, -1.0].as_slice()),
            Arc::from([0.0_f32, 0.0].as_slice()),
            Arc::from([1.0_f32, 0.0].as_slice()),
            Arc::from([0.0_f32, 1.0].as_slice()),
            Arc::from([1.0_f32, 1.0].as_slice()),
            Arc::from([10.0_f32, 10.0].as_slice()),
            Arc::from([11.0_f32, 10.0].as_slice()),
            Arc::from([10.0_f32, 11.0].as_slice()),
            Arc::from([11.0_f32, 11.0].as_slice()),
            Arc::from([12.0_f32, 12.0].as_slice()),
        ];
        let indices = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let kmeans_input = KMeansAlgorithmInput::new(
            indices,
            &embeddings,
            dim,
            2,
            1,
            10,
            8,
            chroma_distance::DistanceFunction::Euclidean,
            100.0,
        );
        let centers = vec![vec![0.0, 0.0], vec![10.0, 10.0]];
        let res = kmeansassign_for_centerinit(&kmeans_input, &centers).expect("Failed to assign");
        assert_eq!(res.cluster_counts, vec![4, 4]);
        assert_eq!(res.total_distance, 8.0);
        assert_eq!(res.cluster_weighted_counts, vec![4.0, 4.0]);
        assert_eq!(res.cluster_farthest_distance, vec![2.0, 2.0]);
    }

    #[test]
    fn test_kmeans_assign_for_main_loop() {
        // 2D embeddings.
        let dim = 2;
        let embeddings: Vec<Arc<[f32]>> = vec![
            Arc::from([-1.0_f32, -1.0].as_slice()),
            Arc::from([0.0_f32, 0.0].as_slice()),
            Arc::from([1.0_f32, 0.0].as_slice()),
            Arc::from([0.0_f32, 1.0].as_slice()),
            Arc::from([1.0_f32, 1.0].as_slice()),
            Arc::from([5.0_f32, 5.0].as_slice()),
            Arc::from([10.0_f32, 10.0].as_slice()),
            Arc::from([11.0_f32, 10.0].as_slice()),
            Arc::from([10.0_f32, 11.0].as_slice()),
            Arc::from([11.0_f32, 11.0].as_slice()),
            Arc::from([12.0_f32, 12.0].as_slice()),
            Arc::from([13.0_f32, 13.0].as_slice()),
        ];
        let indices = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let kmeans_input = KMeansAlgorithmInput::new(
            indices,
            &embeddings,
            dim,
            2,
            1,
            12,
            9,
            chroma_distance::DistanceFunction::Euclidean,
            100.0,
        );
        let centers = vec![vec![0.0, 0.0], vec![10.0, 10.0]];
        // Penalize [10.0, 10.0] so that [5.0, 5.0] gets assigned to [0.0, 0.0]
        let previous_counts = vec![0, 9];
        let lambda = 1.0;
        let res = kmeansassign_for_main_loop(&kmeans_input, &centers, &previous_counts, lambda)
            .expect("Failed to assign");
        assert_eq!(res.cluster_counts, vec![5, 4]);
        assert_eq!(res.total_distance, 94.0);
        assert_eq!(res.cluster_farthest_distance, vec![50.0, 11.0]);
        assert_eq!(res.cluster_farthest_point_idx, vec![5, 9]);
        assert_eq!(
            res.cluster_new_centers,
            vec![vec![7.0, 7.0], vec![42.0, 42.0]]
        );
    }

    #[test]
    fn test_kmeans_assign_finish() {
        // 2D embeddings.
        let dim = 2;
        let embeddings: Vec<Arc<[f32]>> = vec![
            Arc::from([0.0_f32, 0.0].as_slice()),
            Arc::from([1.0_f32, 0.0].as_slice()),
            Arc::from([0.0_f32, 1.0].as_slice()),
            Arc::from([1.0_f32, 1.0].as_slice()),
            Arc::from([10.0_f32, 10.0].as_slice()),
            Arc::from([11.0_f32, 10.0].as_slice()),
            Arc::from([10.0_f32, 11.0].as_slice()),
            Arc::from([11.0_f32, 11.0].as_slice()),
        ];
        let indices = vec![0, 1, 2, 3, 4, 5, 6, 7];
        let kmeans_algo = KMeansAlgorithmInput::new(
            indices,
            &embeddings,
            dim,
            2,
            0,
            8,
            4,
            chroma_distance::DistanceFunction::Euclidean,
            100.0,
        );
        let centers = vec![vec![0.0, 0.0], vec![10.0, 10.0]];
        let mut res = kmeansassign_finish(&kmeans_algo, &centers, false).expect("Failed to assign");
        assert_eq!(res.cluster_counts, vec![4, 4]);
        assert_eq!(res.cluster_nearest_distance, vec![0.0, 0.0]);
        assert_eq!(res.cluster_nearest_point_idx, vec![0, 4]);
        assert_eq!(res.cluster_labels.len(), 0);
        res = kmeansassign_finish(&kmeans_algo, &centers, true).expect("Failed to assign");
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

    // Just tests that kmeans clustering runs without panicking.
    #[test]
    fn test_kmeans_clustering() {
        // 2D embeddings.
        let dim = 2;
        let embeddings: Vec<Arc<[f32]>> = vec![
            Arc::from([0.0_f32, 0.0].as_slice()),
            Arc::from([1.0_f32, 0.0].as_slice()),
            Arc::from([0.0_f32, 1.0].as_slice()),
            Arc::from([1.0_f32, 1.0].as_slice()),
            Arc::from([1000.0_f32, 10000.0].as_slice()),
            Arc::from([11000.0_f32, 10000.0].as_slice()),
            Arc::from([10000.0_f32, 11000.0].as_slice()),
            Arc::from([11000.0_f32, 11000.0].as_slice()),
        ];
        let indices = vec![0, 1, 2, 3, 4, 5, 6, 7];
        let mut kmeans_algo = KMeansAlgorithmInput::new(
            indices,
            &embeddings,
            dim,
            2,
            0,
            8,
            4,
            chroma_distance::DistanceFunction::Euclidean,
            100.0,
        );
        let _ = cluster(&mut kmeans_algo).expect("Failed to cluster");
    }
}
