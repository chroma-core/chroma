//! Provider format dispatch.
//!
//! Only [`ProviderFormat::Anthropic`] exists for now, but keeping this as an
//! enum preserves the dispatch seam so additional inference providers can slot
//! in without churning the `to_provider_format` APIs on `ToolSchema` and
//! `Trajectory` (added in later milestones).

use serde::{Deserialize, Serialize};

/// Wire format that a tool schema or trajectory is rendered into for a given
/// inference provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProviderFormat {
    Anthropic,
}
