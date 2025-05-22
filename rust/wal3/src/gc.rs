use std::ops::Add;

use setsum::Setsum;

use crate::{deserialize_setsum, serialize_setsum, ScrubError};

////////////////////////////////////////////// Garbage /////////////////////////////////////////////

pub struct Garbage {
    dropped_setsum: Setsum,
    actions: Vec<GarbageAction>,
}

impl Garbage {
    #[allow(clippy::result_large_err)]
    pub fn scrub(&self) -> Result<Setsum, ScrubError> {
        scrub(&self.actions, self.dropped_setsum)
    }
}

/////////////////////////////////////////// GarbageAction //////////////////////////////////////////

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum GarbageAction {
    DropSnapshot {
        path_to_snapshot: String,
        #[serde(
            deserialize_with = "deserialize_setsum",
            serialize_with = "serialize_setsum"
        )]
        snapshot_setsum: Setsum,
        children: Vec<GarbageAction>,
    },
    ReplaceSnapshot {
        old_path_to_snapshot: String,
        #[serde(
            deserialize_with = "deserialize_setsum",
            serialize_with = "serialize_setsum"
        )]
        old_snapshot_setsum: Setsum,
        new_path_to_snapshot: String,
        #[serde(
            deserialize_with = "deserialize_setsum",
            serialize_with = "serialize_setsum"
        )]
        new_snapshot_setsum: Setsum,
        children: Vec<GarbageAction>,
    },
    DropFragment {
        path_to_fragment: String,
        #[serde(
            deserialize_with = "deserialize_setsum",
            serialize_with = "serialize_setsum"
        )]
        fragment_setsum: Setsum,
    },
}

impl GarbageAction {
    #[allow(clippy::result_large_err)]
    pub fn scrub(&self) -> Result<Setsum, ScrubError> {
        match self {
            Self::DropFragment {
                fragment_setsum,
                path_to_fragment: _,
            } => Ok(*fragment_setsum),
            Self::DropSnapshot {
                snapshot_setsum,
                children,
                path_to_snapshot: _,
            } => scrub(children, *snapshot_setsum),
            Self::ReplaceSnapshot {
                old_snapshot_setsum,
                new_snapshot_setsum,
                children,
                old_path_to_snapshot: _,
                new_path_to_snapshot: _,
            } => scrub(children, *new_snapshot_setsum - *old_snapshot_setsum),
        }
    }
}

/////////////////////////////////////////////// util ///////////////////////////////////////////////

#[allow(clippy::result_large_err)]
fn scrub(actions: &[GarbageAction], expected_setsum: Setsum) -> Result<Setsum, ScrubError> {
    let to_drop = actions
        .iter()
        .map(GarbageAction::scrub)
        .collect::<Result<Vec<_>, ScrubError>>()?;
    let dropped_setsum = to_drop.into_iter().fold(Setsum::default(), Setsum::add);
    if dropped_setsum != expected_setsum {
        return Err(ScrubError::CorruptGarbage {
            expected_setsum,
            returned_setsum: dropped_setsum,
        });
    }
    Ok(dropped_setsum)
}
