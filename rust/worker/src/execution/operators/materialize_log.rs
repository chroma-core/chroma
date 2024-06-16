use crate::{
    errors::{ChromaError, ErrorCodes},
    execution::{data::data_chunk::Chunk, operator::Operator},
    segment::{LogMaterializer, MaterializedLogRecord},
};
use async_trait::async_trait;
use thiserror::Error;

#[derive(Debug)]
pub(crate) struct MaterializeLogOperator {}

impl MaterializeLogOperator {
    pub fn new() -> Box<Self> {
        Box::new(MaterializeLogOperator {})
    }
}

pub(crate) struct MaterializeLogInput<'materializer> {
    materializer: LogMaterializer<'materializer>,
}

pub(crate) struct MaterializeLogOutput<'materialized_log_record> {
    materialized: Chunk<MaterializedLogRecord<'materialized_log_record>>,
}

#[derive(Debug, Error)]
pub enum MaterializeLogError {}

impl ChromaError for MaterializeLogError {
    fn code(&self) -> ErrorCodes {
        return ErrorCodes::Internal;
    }
}

#[async_trait]
impl<'materializer, 'materialized_log_record>
    Operator<MaterializeLogInput<'materializer>, MaterializeLogOutput<'materialized_log_record>>
    for MaterializeLogOperator
{
    type Error = MaterializeLogError;

    async fn run(
        &self,
        input: &MaterializeLogInput<'materializer>,
    ) -> Result<MaterializeLogOutput<'materialized_log_record>, Self::Error> {
        let materialized = input.materializer.materialize().await;
        match materialized {
            Ok(materialized) => Ok(MaterializeLogOutput { materialized }),
            Err(_) => panic!("Failed to materialize log"),
        }
    }
}
