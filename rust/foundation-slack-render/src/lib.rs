//! Shared renderer for Slack messages into Markdown.
//!
//! [`golden_cases`] pins the rendered output as a regression test both repos
//! can run. `chrono` is the only dependency.

use chrono::{DateTime, Utc};

pub mod golden;
pub use golden::{golden_cases, GoldenCase};

/// Metadata key names shared by both renderers.
pub mod meta_keys {
    pub const TEAM_ID: &str = "team_id";
    /// The channel *id*, not the human name.
    pub const CHANNEL: &str = "channel";
    pub const CHANNEL_NAME: &str = "channel_name";
    pub const TS: &str = "ts";
}

/// How a Slack document was ingested. Stamped on metadata under the `ingest`
/// key so the two populations can be filtered apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestMode {
    Realtime,
    Backfill,
}

impl IngestMode {
    /// The `ingest` metadata value for this mode.
    pub fn as_str(self) -> &'static str {
        match self {
            IngestMode::Realtime => "realtime",
            IngestMode::Backfill => "backfill",
        }
    }
}

/// Source tag for Slack-origin docs.
pub const SOURCE_SLACK: &str = "slack";

/// One Slack message to render. User fields are already resolved by the caller;
/// `is_reply` selects thread (blockquote) rendering.
#[derive(Debug, Clone, Copy)]
pub struct Message<'a> {
    pub user_name: &'a str,
    pub user_email: Option<&'a str>,
    pub user_id: &'a str,
    pub ts: &'a str,
    pub text: &'a str,
    pub is_reply: bool,
}

/// Render a Slack Markdown document: a `# #channel` header followed by one
/// block per message, thread replies (`is_reply`) nested as blockquotes.
///
/// Realtime callers pass a single message; backfill passes a page (and may
/// slice it for upload size limits). The byte layout is identical either way.
pub fn render_document(channel_name: &str, messages: &[Message]) -> String {
    let mut out = format!("# #{channel_name}\n\n");
    for m in messages {
        let meta = author_meta(m.user_email, m.user_id, m.ts);
        let body = m.text.trim_end();
        if m.is_reply {
            out.push_str(&format!("> **{}** ({meta}):\n>\n", m.user_name));
            for line in body.split('\n') {
                out.push_str("> ");
                out.push_str(line);
                out.push('\n');
            }
            out.push('\n');
        } else {
            out.push_str(&format!("**{}** ({meta}):\n\n", m.user_name));
            out.push_str(body);
            if !body.is_empty() {
                out.push('\n');
            }
            out.push('\n');
        }
    }
    out
}

/// `email - user_id - HH:MM:SS UTC`, dropping empty parts but always keeping
/// the timestamp.
fn author_meta(email: Option<&str>, user_id: &str, slack_ts: &str) -> String {
    let mut parts = Vec::with_capacity(3);
    if let Some(email) = email.filter(|e| !e.is_empty()) {
        parts.push(email.to_string());
    }
    if !user_id.is_empty() {
        parts.push(user_id.to_string());
    }
    parts.push(format_ts(slack_ts));
    parts.join(" - ")
}

/// A Slack timestamp (fractional unix seconds) as `HH:MM:SS UTC`, or the raw
/// string if it can't be parsed. Sub-second precision is dropped.
fn format_ts(slack_ts: &str) -> String {
    let secs = slack_ts.split_once('.').map(|(s, _)| s).unwrap_or(slack_ts);
    if let Ok(epoch) = secs.parse::<i64>() {
        if let Some(dt) = DateTime::<Utc>::from_timestamp(epoch, 0) {
            return dt.format("%H:%M:%S UTC").to_string();
        }
    }
    slack_ts.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_document_single_top_level() {
        let out = render_document(
            "engineering",
            &[Message {
                user_name: "Ada",
                user_email: Some("ada@example.com"),
                user_id: "U1",
                ts: "0",
                text: "hello",
                is_reply: false,
            }],
        );
        assert_eq!(
            out,
            "# #engineering\n\n**Ada** (ada@example.com - U1 - 00:00:00 UTC):\n\nhello\n\n"
        );
    }

    #[test]
    fn render_document_reply_is_blockquoted_per_line() {
        let out = render_document(
            "general",
            &[Message {
                user_name: "Bo",
                user_email: None,
                user_id: "U2",
                ts: "0",
                text: "line 1\nline 2",
                is_reply: true,
            }],
        );
        assert_eq!(
            out,
            "# #general\n\n> **Bo** (U2 - 00:00:00 UTC):\n>\n> line 1\n> line 2\n\n"
        );
    }

    #[test]
    fn format_ts_drops_subsecond_and_falls_back() {
        assert_eq!(format_ts("1715625600.123456"), format_ts("1715625600"));
        assert_eq!(format_ts("0"), "00:00:00 UTC");
        assert_eq!(format_ts("not-a-ts"), "not-a-ts");
        assert_eq!(format_ts(""), "");
    }

    #[test]
    fn author_meta_drops_empty_parts_keeps_ts() {
        assert_eq!(
            author_meta(Some("ada@example.com"), "U1", "0"),
            "ada@example.com - U1 - 00:00:00 UTC"
        );
        assert_eq!(author_meta(None, "U1", "0"), "U1 - 00:00:00 UTC");
        assert_eq!(author_meta(Some(""), "U1", "0"), "U1 - 00:00:00 UTC");
        assert_eq!(author_meta(None, "", "0"), "00:00:00 UTC");
    }

    #[test]
    fn ingest_mode_and_source_values() {
        assert_eq!(IngestMode::Realtime.as_str(), "realtime");
        assert_eq!(IngestMode::Backfill.as_str(), "backfill");
        assert_eq!(SOURCE_SLACK, "slack");
        assert_eq!(meta_keys::TEAM_ID, "team_id");
        assert_eq!(meta_keys::CHANNEL, "channel");
        assert_eq!(meta_keys::CHANNEL_NAME, "channel_name");
        assert_eq!(meta_keys::TS, "ts");
    }

    #[test]
    fn golden_documents_render_exactly() {
        for c in golden_cases() {
            assert_eq!(
                render_document(c.channel_name, c.messages),
                c.expected_markdown,
                "{}",
                c.name
            );
        }
    }
}
