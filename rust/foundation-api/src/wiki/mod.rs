//! Wiki-domain helpers for foundation-api.
//!
//! foundation-api does not embed the Chroma data plane. Instead it acts as a
//! Chroma client and proxies record I/O to the frontend (FE), letting the FE
//! enforce auth, quota, metering, and billing against the caller's
//! `x-chroma-token`. This module hosts that client surface plus the
//! foundation-specific chunking the `/upsert-page` flow composes on top of it;
//! the embedding helper lands here in a later change.

// Wired into the server in this change but not yet exercised end to end; the
// resolve/cache surface is consumed once the wiki record-I/O routes land.
#[allow(dead_code)]
pub(crate) mod client;
// Pure markdown chunking ported from foundation-research; consumed by the
// `/upsert-page` route added later in the stack, so unused on its own for now.
#[allow(dead_code)]
pub(crate) mod chunking;
// SPLADE sparse embedding helper; also consumed by the `/upsert-page` route
// added later in the stack, so unused on its own for now.
#[allow(dead_code)]
pub(crate) mod embed;
pub(crate) use client::{WikiClient, WikiClientError};
