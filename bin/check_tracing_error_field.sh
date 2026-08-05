#!/usr/bin/env bash
#
# Reject a tracing field literally named `error`.
#
# Honeycomb assigns a column its type on first write and keeps it forever. Its
# OTLP ingest derives a *boolean* `error` from OpenTelemetry span status, so in
# any service with a busy error path that boolean lands first and in volume, and
# `error` becomes a boolean column. Every string written to it afterwards is
# coerced to `false` — the message is destroyed, silently, and cannot be
# recovered later because it was never stored.
#
# The failure is invisible from the code: the same `error = %e` keeps its text in
# a dataset whose column happened to type as a string, and loses it in one that
# typed as boolean. `error` is boolean in every dataset the data plane reports
# into.
#
# Write `error_message` instead. It never collides with the derived boolean.
#
#   tracing::error!(error = %e, "upload failed");            // dropped
#   tracing::error!(error_message = %e, "upload failed");    // kept
#
# A more specific name is fine and often better — `storage_error`, `manifest_error`
# all pass. The only banned spelling is the bare `error`.
#
# Clippy cannot catch this: the field name lives inside a macro, where no lint can
# see it.

set -euo pipefail

# Directories this check covers.
ROOTS=(rust)

# A field named `error` has four spellings, and all four have to be caught.

# 1. Sigil form. `%` and `?` are the tracing sigils for Display and Debug.
#    Tolerate the spacing, because `error =% err` is as common here as
#    `error = %err`.
P_SIGIL='\berror[[:space:]]*=[[:space:]]*[%?]'

# 2. Shorthand. `warn!(%error, …)` names the field after the local variable, so a
#    binding called `error` produces a column called `error` with no `=` on the
#    line at all. A key of its own names the column, so `error_message = %error`
#    is fine and is filtered back out below.
P_SHORTHAND='[%?]error\b'

# 3. A value that is already a `String` needs no sigil: `error = last_err,`. An
#    assignment ends in `;`, so requiring a trailing comma is what separates a
#    macro field from ordinary Rust such as `let error = ...;`.
P_ARG_LINE='^[[:space:]]*error[[:space:]]*=[[:space:]]*[^;]*,[[:space:]]*$'

# 4. The same, written inline. Naming the macros and refusing to cross an opening
#    quote keeps message text such as `println!("… error={err}")` out of the
#    results.
P_ARG_INLINE='\b(tracing::)?(error|warn|info|debug|trace|event)!\([^"]*\berror[[:space:]]*=[[:space:]]*[^=%?]'

hits=$(
    {
        grep -rnE "$P_SIGIL" --include='*.rs' "${ROOTS[@]}" 2>/dev/null || true
        { grep -rnE "$P_SHORTHAND" --include='*.rs' "${ROOTS[@]}" 2>/dev/null || true; } \
            | { grep -vE '=[[:space:]]*[%?]error\b' || true; }
        grep -rnE "$P_ARG_LINE" --include='*.rs' "${ROOTS[@]}" 2>/dev/null || true
        grep -rnE "$P_ARG_INLINE" --include='*.rs' "${ROOTS[@]}" 2>/dev/null || true
    } | sort -u -t: -k1,1 -k2,2n
)

if [[ -n "$hits" ]]; then
    echo "error: tracing field named \`error\` — Honeycomb will discard the message." >&2
    echo >&2
    echo "$hits" >&2
    echo >&2
    echo "Rename it to \`error_message\` (or something more specific, like" >&2
    echo "\`storage_error\`). See the comment at the top of $0 for why." >&2
    exit 1
fi
