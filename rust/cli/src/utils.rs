use std::pin::Pin;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use std::io::Error as IoError;

pub const LOGO: &str = "
                \x1b[38;5;069m(((((((((    \x1b[38;5;203m(((((\x1b[38;5;220m####
             \x1b[38;5;069m(((((((((((((\x1b[38;5;203m(((((((((\x1b[38;5;220m#########
           \x1b[38;5;069m(((((((((((((\x1b[38;5;203m(((((((((((\x1b[38;5;220m###########
         \x1b[38;5;069m((((((((((((((\x1b[38;5;203m((((((((((((\x1b[38;5;220m############
        \x1b[38;5;069m(((((((((((((\x1b[38;5;203m((((((((((((((\x1b[38;5;220m#############
        \x1b[38;5;069m(((((((((((((\x1b[38;5;203m((((((((((((((\x1b[38;5;220m#############
         \x1b[38;5;069m((((((((((((\x1b[38;5;203m(((((((((((((\x1b[38;5;220m##############
         \x1b[38;5;069m((((((((((((\x1b[38;5;203m((((((((((((\x1b[38;5;220m##############
           \x1b[38;5;069m((((((((((\x1b[38;5;203m(((((((((((\x1b[38;5;220m#############
             \x1b[38;5;069m((((((((\x1b[38;5;203m((((((((\x1b[38;5;220m##############
                \x1b[38;5;069m(((((\x1b[38;5;203m((((    \x1b[38;5;220m#########\x1b[0m
";

#[derive(Debug, Error)]
pub enum UtilsError {
    #[error("Failed to write output to console")]
    ConsoleWriteFailed,
}

#[async_trait::async_trait]
pub trait AsyncCliWriter: AsyncWrite + Unpin {
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), UtilsError>;
}

pub struct AsyncStdOut {
    stdout: tokio::io::Stdout,
}

impl AsyncStdOut {
    pub fn new() -> Self {
        Self { stdout:  tokio::io::stdout()}
    }
}

impl AsyncWrite for AsyncStdOut {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, IoError>> {
        Pin::new(&mut self.stdout).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), IoError>> {
        Pin::new(&mut self.stdout).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), IoError>> {
        Pin::new(&mut self.stdout).poll_shutdown(cx)
    }
}

impl Unpin for AsyncStdOut {

}

#[async_trait::async_trait]
impl AsyncCliWriter for AsyncStdOut
{
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), UtilsError> {
        self.stdout.write_all(buf).await.map_err(|_| UtilsError::ConsoleWriteFailed)
    }
}

pub struct TestCliWriter {
    pub buffer: Vec<u8>,
}

impl TestCliWriter {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    pub fn output(&self) -> String {
        String::from_utf8_lossy(&self.buffer).to_string()
    }
}

impl AsyncWrite for TestCliWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, IoError>> {
        self.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), IoError>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), IoError>> {
        Poll::Ready(Ok(()))
    }
}

impl Unpin for TestCliWriter {}

#[async_trait::async_trait]
impl AsyncCliWriter for TestCliWriter {
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), UtilsError> {
        self.buffer.extend_from_slice(buf);
        Ok(())
    }
}