use super::s3::S3GetError;
use super::GetError;
use aws_sdk_s3::primitives::ByteStream as AWSS3ByteStream;
use futures::stream::Stream;
use std::io::Read;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;

pub type ByteStreamItem = Result<Vec<u8>, GetError>;

pub trait ByteStream {
    type Stream: Stream<Item = ByteStreamItem> + Unpin;

    fn byte_stream(self) -> Self::Stream;
}

pub struct SyncFileStream {
    reader: std::fs::File,
}

impl SyncFileStream {
    pub fn new(file: std::fs::File) -> Self {
        let reader = file;
        SyncFileStream { reader }
    }
}

impl Stream for SyncFileStream {
    type Item = ByteStreamItem;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // hardcode buffer size since it is only for testing
        let mut buffer = vec![0; 8192];
        let result = self.reader.read(&mut buffer);
        match result {
            Ok(n) => {
                if n == 0 {
                    Poll::Ready(None)
                } else {
                    let mut data = Vec::new();
                    data.extend_from_slice(&buffer[..n]);
                    Poll::Ready(Some(Ok(data)))
                }
            }
            Err(e) => Poll::Ready(Some(Err(GetError::LocalError(e.to_string())))),
        }
    }
}

impl ByteStream for std::fs::File {
    type Stream = SyncFileStream;

    fn byte_stream(self) -> Self::Stream {
        SyncFileStream::new(self)
    }
}

pub struct AsyncFileStream {
    reader: tokio::fs::File,
}

impl AsyncFileStream {
    pub fn new(file: tokio::fs::File) -> Self {
        let reader = file;
        AsyncFileStream { reader }
    }
}

impl Stream for AsyncFileStream {
    type Item = ByteStreamItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // hardcode buffer size since it is only for testing
        let mut buffer = vec![0; 8192];
        let mut read_buf = tokio::io::ReadBuf::new(&mut buffer);
        match Pin::new(&mut self.reader).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => {
                let n = read_buf.filled().len();
                if n == 0 {
                    Poll::Ready(None)
                } else {
                    let mut data = Vec::new();
                    data.extend_from_slice(&read_buf.filled());
                    Poll::Ready(Some(Ok(data)))
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(GetError::LocalError(e.to_string())))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl ByteStream for tokio::fs::File {
    type Stream = AsyncFileStream;

    fn byte_stream(self) -> Self::Stream {
        AsyncFileStream::new(self)
    }
}

pub struct S3ByteStream {
    inner: AWSS3ByteStream,
}

impl S3ByteStream {
    pub fn new(body: AWSS3ByteStream) -> Self {
        S3ByteStream { inner: body }
    }
}

impl Stream for S3ByteStream {
    type Item = ByteStreamItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        match Pin::new(&mut me.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                let mut data = Vec::new();
                data.extend_from_slice(&chunk);
                Poll::Ready(Some(Ok(data)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(GetError::S3Error(
                S3GetError::ByteStreamError(e.to_string()),
            )))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// impl AsyncRead for S3ByteStream {
//     fn poll_read(
//         self: Pin<&mut Self>,
//         cx: &mut Context<'_>,
//         buf: &mut tokio::io::ReadBuf,
//     ) -> Poll<std::io::Result<()>> {
//         let me = self.get_mut();
//         let mut buffer = vec![0; buf.remaining()];
//         match Pin::new(&mut me.inner).poll_next(cx) {
//             Poll::Ready(Some(Ok(chunk))) => {
//                 buffer[..chunk.len()].copy_from_slice(&chunk);
//                 buf.put_slice(&buffer[..chunk.len()]);
//                 Poll::Ready(Ok(()))
//             }
//             Poll::Ready(Some(Err(e))) => Poll::Ready(Err(std::io::Error::new(
//                 std::io::ErrorKind::Other,
//                 e.to_string(),
//             ))),
//             Poll::Ready(None) => Poll::Ready(Ok(())),
//             Poll::Pending => Poll::Pending,
//         }
//     }
// }
