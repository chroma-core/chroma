use crate::execution::operator::Operator;
use async_trait::async_trait;

#[derive(Debug)]
pub struct WriteSegmentsOperator {}

#[derive(Debug)]
pub struct WriteSegmentsInput {}

#[derive(Debug)]
pub struct WriteSegmentsOutput {}

pub type WriteSegmentsResult = Result<WriteSegmentsOutput, ()>;

#[async_trait]
impl Operator<WriteSegmentsInput, WriteSegmentsOutput> for WriteSegmentsOperator {
    type Error = ();

    async fn run(&self, input: &WriteSegmentsInput) -> WriteSegmentsResult {
        Ok(WriteSegmentsOutput {})
    }
}
