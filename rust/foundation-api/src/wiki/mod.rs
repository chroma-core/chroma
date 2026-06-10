//! Wiki-domain helpers for foundation-api.
//!
//! foundation-api does not embed the Chroma data plane. Instead it acts as a
//! Chroma client and proxies record I/O to the frontend (FE), letting the FE
//! enforce auth, quota, metering, and billing against the caller's
//! `x-chroma-token`. This module hosts that client surface; chunking and
//! embedding helpers land here in later changes.

// Wired into the server in this change but not yet exercised end to end; the
// resolve/cache surface is consumed once the wiki record-I/O routes land.
#[allow(dead_code)]
pub(crate) mod client;
pub(crate) use client::{WikiClient, WikiClientError};
