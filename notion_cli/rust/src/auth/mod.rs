//! Login + token-acquisition path. Replaces the older python
//! `notion_auth.py` script -- everything ships as a single rust binary
//! now (no python dependency on the user's machine).
//!
//! Public surface used by `cmd/login.rs`:
//!
//!   - [`state`]    -- read/write `notion-token-v2.txt` /
//!                     `notion-file-token.txt`, persist
//!                     `NOTION_INTERNAL_SPACE_ID` to `.env`.
//!   - [`scan`]     -- enumerate every browser cookie store on disk via
//!                     `rookie::load`, return sessions that contain a
//!                     valid Notion `token_v2`.
//!   - [`paste`]    -- manual fallback: prompt the user to paste their
//!                     `token_v2` from DevTools.
//!   - [`cdp`]      -- launch a managed Chromium-family browser via
//!                     `chromiumoxide`, poll cookies until the user signs
//!                     in to Notion, return the captured tokens.
//!   - [`validate`] -- hit `/api/v3/loadUserContent` with a candidate
//!                     `token_v2`, return user/spaces summary if it works.
//!   - [`catalog`]  -- static catalogue of long-tail browser cookie file
//!                     paths to help users craft a `--cookie-file` arg
//!                     when their browser isn't in rookie's defaults.

pub mod catalog;
pub mod cdp;
pub mod paste;
pub mod scan;
pub mod state;
pub mod validate;

#[allow(unused_imports)]
pub use state::{
    save_file_token, save_token_v2, save_workspace_pin, Credentials,
};
#[allow(unused_imports)]
pub use validate::{validate_token, UserContent, WorkspaceInfo};
