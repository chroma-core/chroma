use std::cmp::min;

use chroma_distance::DistanceFunction;
use rand::Rng;

pub struct KMeansAlgorithmInput<'referred_data> {
    indices: Vec<u32>,
    embeddings: &'referred_data [f32],
    embedding_dimension: usize,
    k: usize,
    first: usize,
    // Exclusive range.
    last: usize,
    num_samples: usize,
    distance_function: DistanceFunction,
}

pub struct KMeansAlgorithm<'referred_data> {
    input: KMeansAlgorithmInput<'referred_data>,
}

impl<'referred_data> KMeansAlgorithm<'referred_data> {
    pub fn init_centers(&mut self, num_iters: usize) {
        let batch_end = min(self.input.first + self.input.num_samples, self.input.last);
        // Randomly choose centers.
        for run in 0..num_iters {
            for center in 0..self.input.k {
                let random_center = rand::thread_rng().gen_range(self.input.first..batch_end);
            }
        }
    }
}
