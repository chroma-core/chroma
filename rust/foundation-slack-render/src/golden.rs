//! Shared fixtures: input messages and the exact Markdown they render to.

use crate::Message;

/// One shared fixture: input messages plus the exact Markdown they render to.
#[derive(Debug, Clone, Copy)]
pub struct GoldenCase {
    pub name: &'static str,
    pub channel_name: &'static str,
    pub messages: &'static [Message<'static>],
    pub expected_markdown: &'static str,
}

const GOLDEN: &[GoldenCase] = &[
    GoldenCase {
        name: "single_message",
        channel_name: "engineering",
        messages: &[Message {
            user_name: "Ada",
            user_email: Some("ada@example.com"),
            user_id: "U1",
            ts: "1715625600.123456",
            text: "hello world",
            is_reply: false,
        }],
        expected_markdown: "# #engineering\n\n**Ada** (ada@example.com - U1 - 18:40:00 UTC):\n\nhello world\n\n",
    },
    GoldenCase {
        name: "thread_page",
        channel_name: "general",
        messages: &[
            Message {
                user_name: "Ada",
                user_email: Some("ada@x.com"),
                user_id: "U1",
                ts: "0",
                text: "parent msg",
                is_reply: false,
            },
            Message {
                user_name: "Bo",
                user_email: None,
                user_id: "U2",
                ts: "0",
                text: "reply line 1\nreply line 2",
                is_reply: true,
            },
        ],
        expected_markdown: "# #general\n\n**Ada** (ada@x.com - U1 - 00:00:00 UTC):\n\nparent msg\n\n> **Bo** (U2 - 00:00:00 UTC):\n>\n> reply line 1\n> reply line 2\n\n",
    },
    GoldenCase {
        name: "empty_body_drops_email",
        channel_name: "ops",
        messages: &[Message {
            user_name: "Cy",
            user_email: Some(""),
            user_id: "U4",
            ts: "0",
            text: "",
            is_reply: false,
        }],
        expected_markdown: "# #ops\n\n**Cy** (U4 - 00:00:00 UTC):\n\n\n",
    },
    GoldenCase {
        name: "unparseable_ts_falls_back",
        channel_name: "design",
        messages: &[Message {
            user_name: "De",
            user_email: None,
            user_id: "U5",
            ts: "not-a-ts",
            text: "x",
            is_reply: false,
        }],
        expected_markdown: "# #design\n\n**De** (U5 - not-a-ts):\n\nx\n\n",
    },
];

/// Fixtures both renderers assert against to pin the rendered format.
pub fn golden_cases() -> &'static [GoldenCase] {
    GOLDEN
}
