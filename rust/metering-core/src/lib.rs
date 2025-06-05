use std::{any::Any, fmt::Debug};

/// The trait implemented by all user-defined metering events.
pub trait MeteringEvent: Debug + Any + Send + 'static {
    chroma_metering_macros::generate_noop_mutators! {}
}
