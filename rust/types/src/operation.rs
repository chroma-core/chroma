use super::ConversionError;
use crate::chroma_proto;
use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub enum Operation {
    Add,
    Update,
    Upsert,
    Delete,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MaterializedLogOperation {
    // Set when the record is initially read from the segment
    // before it is processed based on state of the log.
    Initial,
    // Set for records that don't exist in the segment and
    // have been encountered for the first time in the log.
    AddNew,
    // Assume there is a record in the segment and in the log
    // there is a DEL followed by an ADD for the same id.
    // In this case, the overwriteexisting state is set for
    // the record. Easy to construct other cases.
    OverwriteExisting,
    // Set for entries that are present in the segment and
    // have been updated/upserted in the log.
    UpdateExisting,
    // Set for entries that are present in the segment and
    // have been deleted in the log.
    DeleteExisting,
}

#[derive(Error, Debug)]
pub enum OperationConversionError {
    #[error("Invalid operation, valid operations are: Add, Upsert, Update, Delete")]
    InvalidOperation,
    #[error(transparent)]
    DecodeError(#[from] ConversionError),
}

impl_base_convert_error!(OperationConversionError, {
    OperationConversionError::InvalidOperation => ErrorCodes::InvalidArgument,
});

impl TryFrom<chroma_proto::Operation> for Operation {
    type Error = OperationConversionError;

    fn try_from(op: chroma_proto::Operation) -> Result<Self, Self::Error> {
        match op {
            chroma_proto::Operation::Add => Ok(Operation::Add),
            chroma_proto::Operation::Upsert => Ok(Operation::Upsert),
            chroma_proto::Operation::Update => Ok(Operation::Update),
            chroma_proto::Operation::Delete => Ok(Operation::Delete),
        }
    }
}

impl TryFrom<i32> for Operation {
    type Error = OperationConversionError;

    fn try_from(op: i32) -> Result<Self, Self::Error> {
        let maybe_op = chroma_proto::Operation::try_from(op);
        match maybe_op {
            Ok(op) => match op {
                chroma_proto::Operation::Add => Ok(Operation::Add),
                chroma_proto::Operation::Upsert => Ok(Operation::Upsert),
                chroma_proto::Operation::Update => Ok(Operation::Update),
                chroma_proto::Operation::Delete => Ok(Operation::Delete),
            },
            Err(_) => Err(OperationConversionError::DecodeError(
                ConversionError::DecodeError,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chroma_proto;

    #[test]
    fn test_operation_try_from() {
        let proto_op = chroma_proto::Operation::Add;
        let converted_op: Operation = proto_op.try_into().unwrap();
        assert_eq!(converted_op, Operation::Add);
    }
}
