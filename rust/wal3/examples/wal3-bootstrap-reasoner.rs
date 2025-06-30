//! This file is not intended for public consumption, but is kept for completeness.
//!
//! In this file you will find that we reason about the bootstrap process by completely exploring
//! the state space and pruning known good states.  The goal is to prune every state or print a
//! list of states that are bad.
//!
//! This is ad-hoc machine-assisted proving without an environment or theorem prover.

#[derive(Clone, Copy, Debug)]
enum FragmentState {
    BenignRace,
    Conflict,
    Success,
}

impl FragmentState {
    fn all_states() -> impl Iterator<Item = Self> {
        vec![
            FragmentState::BenignRace,
            FragmentState::Conflict,
            FragmentState::Success,
        ]
        .into_iter()
    }
}

#[derive(Clone, Copy, Debug)]
enum InitializeManifest {
    Uninitialized,
    AlreadyInitialized,
    Success,
}

impl InitializeManifest {
    fn all_states() -> impl Iterator<Item = Self> {
        vec![
            InitializeManifest::Uninitialized,
            InitializeManifest::AlreadyInitialized,
            InitializeManifest::Success,
        ]
        .into_iter()
    }
}

#[derive(Clone, Copy, Debug)]
enum RecoverManifest {
    Uninitialized,
    Failure,
    Success,
}

impl RecoverManifest {
    fn all_states() -> impl Iterator<Item = Self> {
        vec![
            RecoverManifest::Uninitialized,
            RecoverManifest::Failure,
            RecoverManifest::Success,
        ]
        .into_iter()
    }
}

enum Disposition {
    /// The combination of states is considered a good case.
    Good,
    /// The combination of states is not considered by the rule.
    Pass,
    /// The case can be dropped with good conscience for not mattering.  The string is the reason.
    Drop(
        &'static str,
        FragmentState,
        InitializeManifest,
        RecoverManifest,
    ),
    /// The case must lead to an error at runtime.
    Panic(
        &'static str,
        FragmentState,
        InitializeManifest,
        RecoverManifest,
    ),
    /// The case must be raised to the user for inspection.
    Raise(
        &'static str,
        FragmentState,
        InitializeManifest,
        RecoverManifest,
    ),
}

fn happy_paths(fs: FragmentState, im: InitializeManifest, rm: RecoverManifest) -> Disposition {
    match (fs, im, rm) {
        (
            FragmentState::Success | FragmentState::BenignRace,
            InitializeManifest::Uninitialized | InitializeManifest::Success,
            RecoverManifest::Success,
        ) => Disposition::Good,
        _ => Disposition::Pass,
    }
}

fn error_paths(fs: FragmentState, im: InitializeManifest, rm: RecoverManifest) -> Disposition {
    match (fs, im, rm) {
        (_, InitializeManifest::AlreadyInitialized, _) => {
            Disposition::Panic("cannot double-initialize manifest", fs, im, rm)
        }
        (_, _, RecoverManifest::Uninitialized) => {
            Disposition::Panic("cannot have manifest become uninitialized", fs, im, rm)
        }
        (_, _, RecoverManifest::Failure) => {
            Disposition::Panic("failed to install recovered manifest", fs, im, rm)
        }
        _ => Disposition::Pass,
    }
}

fn conflict_on_fragment(
    fs: FragmentState,
    im: InitializeManifest,
    rm: RecoverManifest,
) -> Disposition {
    if matches!(fs, FragmentState::Conflict) {
        Disposition::Drop(
            "no need to touch manifest if fragment conflicts",
            fs,
            im,
            rm,
        )
    } else {
        Disposition::Pass
    }
}

fn unconditionally_raise(
    fs: FragmentState,
    im: InitializeManifest,
    rm: RecoverManifest,
) -> Disposition {
    Disposition::Raise("unconditional raise", fs, im, rm)
}

pub fn main() {
    let mut states = vec![];
    for fs in FragmentState::all_states() {
        for im in InitializeManifest::all_states() {
            for rm in RecoverManifest::all_states() {
                states.push((fs, im, rm));
            }
        }
    }
    let rules = vec![
        happy_paths,
        conflict_on_fragment,
        error_paths,
        unconditionally_raise,
    ];
    for state in states.iter() {
        for rule in &rules {
            match (rule)(state.0, state.1, state.2) {
                Disposition::Pass => {}
                Disposition::Good => {
                    break;
                }
                Disposition::Panic(reason, fs, im, rm) => {
                    println!("panic({fs:?}, {im:?}, {rm:?}) -> {reason}");
                    break;
                }
                Disposition::Drop(reason, fs, im, rm) => {
                    _ = reason;
                    _ = fs;
                    _ = im;
                    _ = rm;
                    break;
                }
                Disposition::Raise(reason, fs, im, rm) => {
                    println!("raise({fs:?}, {im:?}, {rm:?}) -> {reason}");
                    break;
                }
            }
        }
    }
}
