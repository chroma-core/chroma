use super::ConversionError;
use crate::chroma_proto;
use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub enum ScalarEncoding {
    FLOAT32,
    INT32,
}

#[derive(Error, Debug)]
pub enum ScalarEncodingConversionError {
    #[error("Invalid encoding, valid encodings are: Float32, Int32")]
    InvalidEncoding,
    #[error(transparent)]
    DecodeError(#[from] ConversionError),
}

impl_base_convert_error!(ScalarEncodingConversionError, {
    ScalarEncodingConversionError::InvalidEncoding => ErrorCodes::InvalidArgument,
});

impl TryFrom<chroma_proto::ScalarEncoding> for ScalarEncoding {
    type Error = ScalarEncodingConversionError;

    fn try_from(encoding: chroma_proto::ScalarEncoding) -> Result<Self, Self::Error> {
        match encoding {
            chroma_proto::ScalarEncoding::Float32 => Ok(ScalarEncoding::FLOAT32),
            chroma_proto::ScalarEncoding::Int32 => Ok(ScalarEncoding::INT32),
            _ => Err(ScalarEncodingConversionError::InvalidEncoding),
        }
    }
}

impl TryFrom<i32> for ScalarEncoding {
    type Error = ScalarEncodingConversionError;

    fn try_from(encoding: i32) -> Result<Self, Self::Error> {
        let maybe_encoding = chroma_proto::ScalarEncoding::try_from(encoding);
        match maybe_encoding {
            Ok(encoding) => match encoding {
                chroma_proto::ScalarEncoding::Float32 => Ok(ScalarEncoding::FLOAT32),
                chroma_proto::ScalarEncoding::Int32 => Ok(ScalarEncoding::INT32),
                _ => Err(ScalarEncodingConversionError::InvalidEncoding),
            },
            Err(_) => Err(ScalarEncodingConversionError::DecodeError(
                ConversionError::DecodeError,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_encoding_try_from() {
        let proto_encoding = chroma_proto::ScalarEncoding::Float32;
        let converted_encoding: ScalarEncoding = proto_encoding.try_into().unwrap();
        assert_eq!(converted_encoding, ScalarEncoding::FLOAT32);
    }
}
