//! Reverse-proxy module forwarding `/api/ask` (and future foundation-domain
//! query endpoints) to the mullet backend.
//!
//! Split by concern, each submodule co-locating its own tests:
//! - [`handler`]: the axum handler that authenticates, scorecards, injects the
//!   caller's identity, and relays mullet's response verbatim.
//! - [`merge`]: pure JSON body manipulation (`merge_user`).
//! - [`error`]: local error type with its `ChromaError` mapping.

mod error;
mod handler;
mod merge;

pub(crate) use handler::ask;
