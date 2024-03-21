use crate::{distance::DistanceFunction, execution::operator::Operator};
use async_trait::async_trait;

#[derive(Debug)]
pub struct BruteForceKnnOperator {}

pub struct BruteForceKnnOperatorInput {
    pub query_vecs: Vec<Vec<f32>>,
    pub k: usize,
    pub distance_metric: DistanceFunction,
}

pub struct BruteForceKnnOperatorOutput {
    pub indices: Vec<Vec<usize>>,
    pub distances: Vec<Vec<f32>>,
}

#[async_trait]
impl Operator<BruteForceKnnOperatorInput, BruteForceKnnOperatorOutput> for BruteForceKnnOperator {
    type Error = ();

    async fn run(
        &self,
        input: &BruteForceKnnOperatorInput,
    ) -> Result<BruteForceKnnOperatorOutput, Self::Error> {
        // For now, this is implemented extremely poorly from a performance perspective.
        // We loop over every vector and do the distance calculation for every other vector.
        for vec in input.query_vecs.iter() {
            println!("{:?}", vec);
        }
        todo!("Implement brute force knn");
    }
}
