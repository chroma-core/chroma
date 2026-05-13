//! Pure scaffolding shared by HTTP frontends in this workspace.
//!
//! Per ADR "Foundation API: Long-Term Home", `frontend-core` is library-only
//! scaffolding that any HTTP server binary in the workspace can embed
//! (currently `chroma-frontend`; soon `foundation-api`). It contains no
//! product-specific route handlers. Each binary owns its own routes and depends
//! on `frontend-core` for the underlying Axum app, middleware, auth trait,
//! error types, config primitives, and OTEL bootstrap.

pub mod ac;
pub mod auth;
pub mod config;
pub mod errors;
pub mod middleware;
pub mod traced_json;
pub mod tracing;
