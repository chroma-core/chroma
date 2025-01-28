use chroma_sysdb::{sysdb, SysDb};
use chroma_system::Operator;
use chroma_types::{CollectionUuid, Segment};
use thiserror::Error;
use tonic::async_trait;

#[derive(Debug, Default)]
pub struct GetSegmentsOperator {}

impl GetSegmentsOperator {
    pub fn new() -> Self {
        GetSegmentsOperator::default()
    }
}

#[derive(Debug)]
pub struct GetSegmentsInput {
    sysdb: SysDb,
    collection_id: CollectionUuid,
}

impl GetSegmentsInput {
    pub fn new(sysdb: SysDb, collection_id: CollectionUuid) -> Self {
        GetSegmentsInput {
            sysdb,
            collection_id,
        }
    }
}

#[derive(Debug)]
pub struct GetSegmentsOutput {
    pub segments: Vec<Segment>,
}

#[derive(Debug, Error)]
pub enum GetSegmentsError {
    #[error("Failed to get segments from SysDb: {0}")]
    SysDb(#[from] sysdb::GetSegmentsError),
}

#[async_trait]
impl Operator<GetSegmentsInput, GetSegmentsOutput> for GetSegmentsOperator {
    type Error = GetSegmentsError;

    async fn run(&self, input: &GetSegmentsInput) -> Result<GetSegmentsOutput, Self::Error> {
        let segments = input
            .sysdb
            .clone()
            .get_segments(None, None, None, input.collection_id)
            .await?;

        Ok(GetSegmentsOutput { segments })
    }

    fn get_type(&self) -> chroma_system::OperatorType {
        chroma_system::OperatorType::IO
    }
}
