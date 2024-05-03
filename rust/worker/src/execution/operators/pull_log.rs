use crate::execution::data::data_chunk::Chunk;
use crate::execution::operator::Operator;
use crate::log::log::Log;
use crate::log::log::PullLogsError;
use crate::types::LogRecord;
use async_trait::async_trait;
use tracing::debug;
use tracing::trace;
use uuid::Uuid;

/// The pull logs operator is responsible for reading logs from the log service.
#[derive(Debug)]
pub struct PullLogsOperator {
    client: Box<dyn Log>,
}

impl PullLogsOperator {
    /// Create a new pull logs operator.
    /// # Parameters
    /// * `client` - The log client to use for reading logs.
    pub fn new(client: Box<dyn Log>) -> Box<Self> {
        Box::new(PullLogsOperator { client })
    }
}

/// The input to the pull logs operator.
/// # Parameters
/// * `collection_id` - The collection id to read logs from.
/// * `offset` - The offset to start reading logs from.
/// * `batch_size` - The number of log entries to read.
/// * `num_records` - The maximum number of records to read.
/// * `end_timestamp` - The end timestamp to read logs until.
#[derive(Debug)]
pub struct PullLogsInput {
    collection_id: Uuid,
    offset: i64,
    batch_size: i32,
    num_records: Option<i32>,
    end_timestamp: Option<i64>,
}

impl PullLogsInput {
    /// Create a new pull logs input.
    /// # Parameters
    /// * `collection_id` - The collection id to read logs from.
    /// * `offset` - The offset to start reading logs from.
    /// * `batch_size` - The number of log entries to read.
    /// * `num_records` - The maximum number of records to read.
    /// * `end_timestamp` - The end timestamp to read logs until.
    pub fn new(
        collection_id: Uuid,
        offset: i64,
        batch_size: i32,
        num_records: Option<i32>,
        end_timestamp: Option<i64>,
    ) -> Self {
        PullLogsInput {
            collection_id,
            offset,
            batch_size,
            num_records,
            end_timestamp,
        }
    }
}

/// The output of the pull logs operator.
#[derive(Debug)]
pub struct PullLogsOutput {
    logs: Chunk<LogRecord>,
}

impl PullLogsOutput {
    /// Create a new pull logs output.
    /// # Parameters
    /// * `logs` - The logs that were read.
    pub fn new(logs: Chunk<LogRecord>) -> Self {
        PullLogsOutput { logs }
    }

    /// Get the log entries that were read by an invocation of the pull logs operator.
    /// # Returns
    /// The log entries that were read.
    pub fn logs(&self) -> Chunk<LogRecord> {
        self.logs.clone()
    }
}

pub type PullLogsResult = Result<PullLogsOutput, PullLogsError>;

#[async_trait]
impl Operator<PullLogsInput, PullLogsOutput> for PullLogsOperator {
    type Error = PullLogsError;

    async fn run(&self, input: &PullLogsInput) -> PullLogsResult {
        // We expect the log to be cheaply cloneable, we need to clone it since we need
        // a mutable reference to it. Not necessarily the best, but it works for our needs.
        let mut client_clone = self.client.clone();
        let batch_size = input.batch_size;
        let mut num_records_read = 0;
        let mut offset = input.offset;
        let mut result = Vec::new();
        loop {
            let logs = client_clone
                .read(input.collection_id, offset, batch_size, input.end_timestamp)
                .await;

            let mut logs = match logs {
                Ok(logs) => logs,
                Err(e) => {
                    return Err(e);
                }
            };

            if logs.is_empty() {
                break;
            }

            num_records_read += logs.len();
            // unwrap here is safe because we just checked if empty
            offset = logs.last().unwrap().log_offset + 1;
            result.append(&mut logs);

            // We used a a timestamp and we didn't get a full batch, so we have retrieved
            // the last batch of logs relevant to our query
            if input.end_timestamp.is_some() && num_records_read < batch_size as usize {
                break;
            }

            // We have read all the records up to the size we wanted
            if input.num_records.is_some()
                && num_records_read >= input.num_records.unwrap() as usize
            {
                break;
            }
        }
        trace!("Log records {:?}", result);
        if input.num_records.is_some() && result.len() > input.num_records.unwrap() as usize {
            result.truncate(input.num_records.unwrap() as usize);
            trace!("Truncated log records {:?}", result);
        }
        // Convert to DataChunk
        let data_chunk = Chunk::new(result.into());
        Ok(PullLogsOutput::new(data_chunk))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::log::InMemoryLog;
    use crate::log::log::InternalLogRecord;
    use crate::types::LogRecord;
    use crate::types::Operation;
    use crate::types::OperationRecord;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_pull_logs() {
        let mut log = Box::new(InMemoryLog::new());

        let collection_uuid_1 = Uuid::from_str("00000000-0000-0000-0000-000000000001").unwrap();
        log.add_log(
            collection_uuid_1.clone(),
            Box::new(InternalLogRecord {
                collection_id: collection_uuid_1.clone(),
                log_offset: 0,
                log_ts: 1,
                record: LogRecord {
                    log_offset: 0,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: None,
                        encoding: None,
                        metadata: None,
                        operation: Operation::Add,
                    },
                },
            }),
        );
        log.add_log(
            collection_uuid_1.clone(),
            Box::new(InternalLogRecord {
                collection_id: collection_uuid_1.clone(),
                log_offset: 1,
                log_ts: 2,
                record: LogRecord {
                    log_offset: 1,
                    record: OperationRecord {
                        id: "embedding_id_2".to_string(),
                        embedding: None,
                        encoding: None,
                        metadata: None,
                        operation: Operation::Add,
                    },
                },
            }),
        );

        let operator = PullLogsOperator::new(log);

        // Pull all logs from collection 1
        let input = PullLogsInput::new(collection_uuid_1, 0, 1, None, None);
        let output = operator.run(&input).await.unwrap();
        assert_eq!(output.logs().len(), 2);

        // Pull all logs from collection 1 with a large batch size
        let input = PullLogsInput::new(collection_uuid_1, 0, 100, None, None);
        let output = operator.run(&input).await.unwrap();
        assert_eq!(output.logs().len(), 2);

        // Pull logs from collection 1 with a limit
        let input = PullLogsInput::new(collection_uuid_1, 0, 1, Some(1), None);
        let output = operator.run(&input).await.unwrap();
        assert_eq!(output.logs().len(), 1);

        // Pull logs from collection 1 with an end timestamp
        let input = PullLogsInput::new(collection_uuid_1, 0, 1, None, Some(1));
        let output = operator.run(&input).await.unwrap();
        assert_eq!(output.logs().len(), 1);

        // Pull logs from collection 1 with an end timestamp
        let input = PullLogsInput::new(collection_uuid_1, 0, 1, None, Some(2));
        let output = operator.run(&input).await.unwrap();
        assert_eq!(output.logs().len(), 2);

        // Pull logs from collection 1 with an end timestamp and a limit
        let input = PullLogsInput::new(collection_uuid_1, 0, 1, Some(1), Some(2));
        let output = operator.run(&input).await.unwrap();
        assert_eq!(output.logs().len(), 1);

        // Pull logs from collection 1 with a limit and a large batch size
        let input = PullLogsInput::new(collection_uuid_1, 0, 100, Some(1), None);
        let output = operator.run(&input).await.unwrap();
        assert_eq!(output.logs().len(), 1);

        // Pull logs from collection 1 with an end timestamp and a large batch size
        let input = PullLogsInput::new(collection_uuid_1, 0, 100, None, Some(1));
        let output = operator.run(&input).await.unwrap();
        assert_eq!(output.logs().len(), 1);

        // Pull logs from collection 1 with an end timestamp and a large batch size
        let input = PullLogsInput::new(collection_uuid_1, 0, 100, None, Some(2));
        let output = operator.run(&input).await.unwrap();
        assert_eq!(output.logs().len(), 2);

        // Pull logs from collection 1 with an end timestamp and a limit and a large batch size
        let input = PullLogsInput::new(collection_uuid_1, 0, 100, Some(1), Some(2));
        let output = operator.run(&input).await.unwrap();
        assert_eq!(output.logs().len(), 1);
    }
}
