use super::ConversionError;
use crate::{
    chroma_proto,
    errors::{ChromaError, ErrorCodes},
};
use thiserror::Error;

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum Operation {
    Add,
    Update,
    Upsert,
    Delete,
}

#[derive(Error, Debug)]
pub(crate) enum OperationConversionError {
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
            _ => Err(OperationConversionError::InvalidOperation),
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
                _ => Err(OperationConversionError::InvalidOperation),
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
