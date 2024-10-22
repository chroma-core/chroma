#[derive(Debug)]
pub struct KnnOperator {
    pub embedding: Vec<f32>,
    pub fetch: u32,
}
