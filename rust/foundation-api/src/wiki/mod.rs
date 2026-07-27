//! Wiki-domain helpers for foundation-api.
//!
//! foundation-api does not embed the Chroma data plane. Instead it acts as a
//! Chroma client and proxies record I/O to the frontend (FE), letting the FE
//! enforce auth, quota, metering, and billing against the caller's
//! `x-chroma-token`. The generic proxying client lives in
//! [`crate::foundation_chroma`]; this module hosts the wiki-specific chunking
//! and SPLADE embedding the `/upsert-page` flow composes on top of it.

#[allow(dead_code)]
pub(crate) mod chunking;
pub(crate) mod embed;
pub(crate) mod page;
pub(crate) use crate::foundation_chroma::FoundationChromaClientError as WikiClientError;
