use crate::execution::operator::Operator;
use async_trait::async_trait;

#[derive(Debug)]
pub struct FlushS3Operator {}

#[derive(Debug)]
pub struct FlushS3Input {}

#[derive(Debug)]
pub struct FlushS3Output {}

pub type WriteSegmentsResult = Result<FlushS3Output, ()>;

#[async_trait]
impl Operator<FlushS3Input, FlushS3Output> for FlushS3Operator {
    type Error = ();

    async fn run(&self, input: &FlushS3Input) -> WriteSegmentsResult {
        Ok(FlushS3Output {})
    }
}
