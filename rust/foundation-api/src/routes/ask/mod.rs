//! Reverse-proxy module forwarding `POST /api/ask` to a Modal `/ask`
//! endpoint.
//!
//! Split by concern, each submodule co-locating its own tests:
//! - [`handler`]: the axum handler that authenticates, scorecards, injects
//!   the caller's identity, forwards the Chroma user token, and relays
//!   Modal's response verbatim.
//! - [`merge`]: pure JSON body manipulation (`merge_user`).
//! - [`error`]: local error type with its `ChromaError` mapping.

mod error;
mod handler;
mod merge;
#[cfg(test)]
mod test_support;

pub(crate) use handler::ask;
