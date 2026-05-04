#!/usr/bin/env python3
"""
Notion CDC — proof-of-concept change data capture (internal integration).

Why an internal integration:
  - CDC is long-lived. We want the sync identity to be a bot that's
    independent of any specific human; an OAuth token bound to a single
    user can disappear when that user leaves the workspace.
  - Volume is low (events are sparse + reconciler polls a few times/hour),
    so the 3 req/s rate limit on a single internal connection is plenty.
  - One-time setup lives in `setup-token` below: workspace owner creates
    one internal integration, shares root pages with it, the script
    validates and writes the token to disk.

  (Note: bulk backfill is handled by `notion_internal_dump.py`, which
  goes through the undocumented /api/v3 with a session cookie -- a
  different tradeoff. CDC stays on the official API because durability
  matters more than coverage here.)

Subcommands:

  setup-token  Walk a workspace owner through creating an internal
               integration in the Notion UI, validate via GET /v1/users/me,
               count visible pages, and write the token to
               <foundation_notes>/notion_cli/notion-internal-token.txt.
               (Also accepted as NOTION_TOKEN env var.)

  register     Create a connection-webhook subscription via POST /v1/webhooks
               and print the subscription id. The first time Notion delivers to
               your endpoint it sends a one-time `verification_token` body; the
               `listen` command captures it.

  listen       Run an HTTP server that:
                 - on first POST with a `verification_token`, persists it to
                   <output>/verification_token.txt
                 - on every subsequent POST, verifies the X-Notion-Signature
                   HMAC-SHA256 against the stored verification_token and
                   appends the (verified, raw) payload to <output>/events.jsonl
                 - if --fetch-on-event is set and a token is available,
                   re-fetches the page (and block tree) for any event whose
                   entity is a page, writing it under <output>/cdc-pages/

  reconcile    Poll a list of data sources (POST /v1/data_sources/{id}/query)
               with `filter.timestamp = "last_edited_time", after = watermark - 60s`
               and re-fetch anything new. Run this on a cron as a safety net
               for missed webhooks.

Env (loads `foundation_notes/.env`):
  NOTION_TOKEN          internal integration token. Falls back to
                        <foundation_notes>/notion_cli/notion-internal-token.txt.

POC notes:
  - The HTTP server is plain HTTP. Notion requires HTTPS in production. Front
    it with ngrok / cloudflared / a TLS-terminating LB during the POC.
  - The script does NOT confirm/activate the webhook for you. After receiving
    the verification_token, paste it into the Notion connection's Webhooks tab
    (or use Notion's confirm endpoint when GA).

Usage:
  # 0. one-time setup of the internal integration
  python3 notion_cdc.py setup-token

  # 1. start listener (in one terminal); expose with `cloudflared tunnel`
  python3 notion_cdc.py listen --port 8787 --fetch-on-event

  # 2. register the webhook against your tunnel URL (in another terminal)
  python3 notion_cdc.py register --url https://your-tunnel.example/notion \\
      --events page.created page.properties_updated page.content_updated \\
                page.deleted page.moved database.content_updated database.schema_updated

  # 3. paste verification_token from <output>/verification_token.txt into
  #    Notion's Webhooks UI to activate the subscription.

  # 4. reconcile loop, every 5 minutes
  python3 notion_cdc.py reconcile --data-sources "ds_abc,ds_def" --interval-seconds 300
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2026-03-11"
NOTION_INTEGRATIONS_URL = "https://www.notion.so/profile/integrations"


def _internal_token_path() -> Path:
    return Path(__file__).resolve().parent / "notion-internal-token.txt"


def _load_internal_token() -> str:
    tok = os.environ.get("NOTION_TOKEN", "").strip()
    if tok:
        return tok
    p = _internal_token_path()
    if p.is_file():
        for line in p.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if s and not s.startswith("#"):
                return s
    return ""


def load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip()
        if (val.startswith('"') and val.endswith('"')) or (
            val.startswith("'") and val.endswith("'")
        ):
            val = val[1:-1]
        if key and key not in os.environ:
            os.environ[key] = val


def notion_request(
    method: str,
    path: str,
    token: str,
    body: dict | None = None,
    timeout: int = 60,
    max_retries: int = 8,
) -> dict:
    url = f"{NOTION_API}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }
    data = json.dumps(body).encode("utf-8") if body is not None else None
    last_err: Exception | None = None
    for attempt in range(max_retries):
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = float(e.headers.get("Retry-After", "1"))
                time.sleep(retry_after * (1 + 0.1 * attempt))
                last_err = e
                continue
            if 500 <= e.code < 600:
                time.sleep(min(60.0, 2.0**attempt))
                last_err = e
                continue
            body_excerpt = e.read().decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"HTTP {e.code} {method} {url}: {body_excerpt}") from e
        except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
            time.sleep(min(60.0, 2.0**attempt))
            last_err = e
            continue
    raise RuntimeError(f"max retries exceeded: {method} {url}: {last_err!r}")


# --------------------------------------------------------------------------
# subcommand: setup-token (internal integration walkthrough)
# --------------------------------------------------------------------------


def cmd_setup_token(args: argparse.Namespace) -> int:
    out_path = Path(args.tokens_file).resolve() if args.tokens_file else _internal_token_path()
    print("--- Notion internal-integration setup (for ongoing CDC) ---")
    print()
    print("CDC uses an INTERNAL integration so the syncing identity is a bot")
    print("decoupled from any human user. The bot's token outlives any one")
    print("person's account, which matters because syncs run forever.")
    print()
    print("Steps (the workspace OWNER must do this; there is no API for it):")
    print(f"  1. Open: {NOTION_INTEGRATIONS_URL}")
    print("  2. Click 'New integration' → choose 'Internal'.")
    print("  3. Pick the workspace, grant 'Read content' capability.")
    print("  4. Copy the 'Internal Integration Secret' (starts with 'ntn_').")
    print("  5. Share the pages/databases you want to sync with this bot:")
    print("     ••• menu on each top-level page → 'Add connections' → pick it.")
    print("     (Sharing a page implicitly shares its descendants.)")
    print()

    if not args.no_browser:
        try:
            if webbrowser.open(NOTION_INTEGRATIONS_URL):
                print(f"(opened {NOTION_INTEGRATIONS_URL} in your browser)")
                print()
        except Exception:
            pass

    if args.token:
        token = args.token.strip()
    else:
        try:
            from getpass import getpass

            token = getpass("Paste the integration token (input hidden): ").strip()
        except Exception:
            token = input("Paste the integration token: ").strip()
    if not token:
        print("error: empty token", file=sys.stderr)
        return 2
    if not token.startswith("ntn_") and not token.startswith("secret_"):
        print(
            "warning: token does not start with 'ntn_' or 'secret_'; continuing anyway",
            file=sys.stderr,
        )

    print()
    print("[validate] GET /v1/users/me …")
    try:
        me = notion_request("GET", "/users/me", token)
    except Exception as e:
        print(f"error: token validation failed: {e}", file=sys.stderr)
        return 3
    bot_name = me.get("name") or "(unnamed)"
    bot_id = me.get("id", "")
    bot_block = me.get("bot", {}) or {}
    workspace_name = bot_block.get("workspace_name") or ""
    print(f"  bot id:         {bot_id}")
    print(f"  bot name:       {bot_name}")
    print(f"  workspace:      {workspace_name or '(not reported)'}")

    print()
    print("[validate] POST /v1/search (counting accessible pages, capped at 1000) …")
    visible = 0
    cursor: str | None = None
    try:
        while True:
            body: dict = {
                "filter": {"property": "object", "value": "page"},
                "page_size": 100,
            }
            if cursor:
                body["start_cursor"] = cursor
            resp = notion_request("POST", "/search", token, body=body)
            results = resp.get("results", [])
            visible += len(results)
            if visible >= 1000 or not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
    except Exception as e:
        print(f"warning: search failed: {e}", file=sys.stderr)
    cap_note = " (capped — actual count may be higher)" if visible >= 1000 else ""
    print(f"  pages visible:  {visible}{cap_note}")
    if visible == 0:
        print(
            "  WARN: 0 pages visible. Share at least one page with this integration\n"
            "  via '••• → Add connections' on the page, then rerun to re-validate.",
            file=sys.stderr,
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        f"# Notion internal integration token for CDC.\n"
        f"# bot={bot_name!r} bot_id={bot_id} workspace={workspace_name or '?'!r} "
        f"added {datetime.now(timezone.utc).isoformat()}\n"
        f"{token}\n",
        encoding="utf-8",
    )
    print()
    print(f"wrote token to {out_path}")
    print("CDC subcommands (`register`, `listen --fetch-on-event`, `reconcile`)")
    print("will now pick this up automatically. NOTION_TOKEN env still wins if set.")
    return 0


# --------------------------------------------------------------------------
# subcommand: register
# --------------------------------------------------------------------------


def cmd_register(args: argparse.Namespace) -> int:
    token = _load_internal_token()
    if not token:
        print(
            "error: no internal token found. Run `setup-token`, set NOTION_TOKEN, "
            f"or write the token to {_internal_token_path()}.",
            file=sys.stderr,
        )
        return 2
    body: dict = {"url": args.url, "events": args.events}
    if args.database_id:
        body["filter"] = {"database_id": args.database_id}
    print(f"[register] POST /v1/webhooks events={args.events} url={args.url}")
    resp = notion_request("POST", "/webhooks", token, body=body)
    print(json.dumps(resp, indent=2))
    return 0


# --------------------------------------------------------------------------
# subcommand: listen
# --------------------------------------------------------------------------


class _Handler(BaseHTTPRequestHandler):
    output_dir: Path
    token_path: Path
    events_path: Path
    fetch_on_event: bool
    fetch_token: str | None

    def _ok(self, status: int = 200, body: bytes = b"ok") -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:  # noqa: A003 (override)
        sys.stderr.write(
            f"[listen {datetime.now(timezone.utc).isoformat()}] {format % args}\n"
        )

    def do_POST(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler API)
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b""
        signature_hdr = self.headers.get("X-Notion-Signature", "")

        try:
            payload = json.loads(raw.decode("utf-8")) if raw else {}
        except Exception:
            payload = {}

        if isinstance(payload, dict) and "verification_token" in payload:
            tok = payload["verification_token"]
            self.token_path.write_text(tok, encoding="utf-8")
            sys.stderr.write(
                f"[listen] received verification_token; wrote {self.token_path}\n"
                f"[listen] paste this into the Notion Webhooks UI to activate:\n"
                f"  {tok}\n"
            )
            self._ok()
            return

        verified = False
        if self.token_path.is_file():
            stored = self.token_path.read_text(encoding="utf-8").strip().encode("utf-8")
            mac = hmac.new(stored, raw, hashlib.sha256).hexdigest()
            expected = f"sha256={mac}"
            if hmac.compare_digest(expected, signature_hdr):
                verified = True
            else:
                sys.stderr.write(
                    f"[listen] BAD signature: got={signature_hdr!r} expected={expected!r}\n"
                )
        else:
            sys.stderr.write(
                "[listen] no verification_token on disk yet; cannot verify signature\n"
            )

        record = {
            "received_at": datetime.now(timezone.utc).isoformat(),
            "verified": verified,
            "signature": signature_hdr,
            "payload": payload,
        }
        with self.events_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

        if verified and self.fetch_on_event and self.fetch_token:
            try:
                _maybe_refetch(payload, self.fetch_token, self.output_dir)
            except Exception as e:
                sys.stderr.write(f"[listen] refetch failed: {e}\n")

        self._ok()


def _maybe_refetch(payload: dict, token: str, output_dir: Path) -> None:
    entity = payload.get("entity") or {}
    if entity.get("type") != "page":
        return
    page_id = entity.get("id")
    if not page_id:
        return
    page = notion_request("GET", f"/pages/{page_id}", token)
    blocks = _walk_blocks(page_id, token)
    out = output_dir / "cdc-pages" / f"{page_id}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    payload_out = {
        "page": page,
        "blocks": blocks,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "trigger_event_type": payload.get("type"),
        "trigger_event_timestamp": payload.get("timestamp"),
    }
    out.write_text(json.dumps(payload_out, separators=(",", ":")), encoding="utf-8")
    sys.stderr.write(f"[listen] refetched page {page_id} -> {out}\n")


def _walk_blocks(block_id: str, token: str, depth: int = 0, max_depth: int = 25) -> list[dict]:
    children: list[dict] = []
    cursor: str | None = None
    while True:
        path = f"/blocks/{block_id}/children?page_size=100"
        if cursor:
            path += f"&start_cursor={urllib.parse.quote(cursor)}"
        resp = notion_request("GET", path, token)
        children.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    if depth < max_depth:
        for c in children:
            if c.get("has_children"):
                c["children"] = _walk_blocks(c["id"], token, depth + 1, max_depth)
    return children


def cmd_listen(args: argparse.Namespace) -> int:
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    token_path = output / "verification_token.txt"
    events_path = output / "events.jsonl"
    fetch_token = _load_internal_token() or None

    if args.fetch_on_event and not fetch_token:
        sys.stderr.write(
            "[listen] WARN: --fetch-on-event set but no internal token found; "
            "events will be logged but not fetched. Run `setup-token` or set NOTION_TOKEN.\n"
        )

    _Handler.output_dir = output
    _Handler.token_path = token_path
    _Handler.events_path = events_path
    _Handler.fetch_on_event = args.fetch_on_event
    _Handler.fetch_token = fetch_token

    server = ThreadingHTTPServer((args.bind, args.port), _Handler)
    sys.stderr.write(
        f"[listen] HTTP on {args.bind}:{args.port}; output dir {output}\n"
        f"[listen] verification_token will be written to {token_path}\n"
        f"[listen] events will be appended to {events_path}\n"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        sys.stderr.write("[listen] shutting down\n")
        server.server_close()
    return 0


# --------------------------------------------------------------------------
# subcommand: reconcile
# --------------------------------------------------------------------------


def cmd_reconcile(args: argparse.Namespace) -> int:
    token = _load_internal_token()
    if not token:
        print(
            "error: no internal token found. Run `setup-token`, set NOTION_TOKEN, "
            f"or write the token to {_internal_token_path()}.",
            file=sys.stderr,
        )
        return 2
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    state_path = output / "reconcile_state.json"
    cdc_pages = output / "cdc-pages"
    cdc_pages.mkdir(parents=True, exist_ok=True)

    state: dict = {}
    if state_path.is_file():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            state = {}

    data_sources = [s.strip() for s in args.data_sources.split(",") if s.strip()]
    if not data_sources:
        print("error: --data-sources is required (comma-separated data_source ids)", file=sys.stderr)
        return 2

    interval = max(1, int(args.interval_seconds))
    iterations = args.iterations  # 0 = forever

    n = 0
    while iterations == 0 or n < iterations:
        n += 1
        round_started_at = datetime.now(timezone.utc).isoformat()
        sys.stderr.write(f"[reconcile] round {n} @ {round_started_at}\n")
        for ds in data_sources:
            wm_iso = state.get(ds)
            if wm_iso:
                wm_dt = datetime.fromisoformat(wm_iso.replace("Z", "+00:00"))
                # subtract 60s to cover Notion's minute-rounding on last_edited_time
                after = (wm_dt - timedelta(seconds=60)).isoformat().replace("+00:00", "Z")
            else:
                # cold start: pull last 24h
                after_dt = datetime.now(timezone.utc) - timedelta(hours=24)
                after = after_dt.isoformat().replace("+00:00", "Z")

            sys.stderr.write(f"[reconcile] data_source={ds} after={after}\n")
            cursor: str | None = None
            highest_seen = wm_iso
            count = 0
            while True:
                body: dict = {
                    "filter": {
                        "timestamp": "last_edited_time",
                        "last_edited_time": {"after": after},
                    },
                    "sorts": [
                        {"timestamp": "last_edited_time", "direction": "ascending"}
                    ],
                    "page_size": 100,
                }
                if cursor:
                    body["start_cursor"] = cursor
                resp = notion_request(
                    "POST", f"/data_sources/{ds}/query", token, body=body
                )
                results = resp.get("results", [])
                count += len(results)
                for page in results:
                    pid = page["id"]
                    let = page.get("last_edited_time")
                    if let and (highest_seen is None or let > highest_seen):
                        highest_seen = let
                    blocks = _walk_blocks(pid, token)
                    out = cdc_pages / f"{pid}.json"
                    out.write_text(
                        json.dumps(
                            {
                                "page": page,
                                "blocks": blocks,
                                "fetched_at": datetime.now(timezone.utc).isoformat(),
                                "trigger": "reconcile",
                                "data_source_id": ds,
                            },
                            separators=(",", ":"),
                        ),
                        encoding="utf-8",
                    )
                if not resp.get("has_more"):
                    break
                cursor = resp.get("next_cursor")

            if highest_seen:
                state[ds] = highest_seen
            sys.stderr.write(
                f"[reconcile] data_source={ds} updated_pages={count} new_watermark={highest_seen}\n"
            )

        state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        if iterations == 0 or n < iterations:
            time.sleep(interval)
    return 0


# --------------------------------------------------------------------------
# arg parsing
# --------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_setup = sub.add_parser(
        "setup-token",
        help="walk a workspace owner through creating an internal integration",
    )
    ap_setup.add_argument(
        "--tokens-file",
        default=str(_internal_token_path()),
        help="where to write the token (default: foundation_notes/notion_cli/notion-internal-token.txt)",
    )
    ap_setup.add_argument(
        "--no-browser", action="store_true", help="don't auto-open the integrations page"
    )
    ap_setup.add_argument(
        "--token", help="provide the token directly instead of being prompted"
    )
    ap_setup.set_defaults(func=cmd_setup_token)

    ap_reg = sub.add_parser("register", help="register a webhook subscription")
    ap_reg.add_argument("--url", required=True, help="public HTTPS URL of your endpoint")
    ap_reg.add_argument(
        "--events",
        nargs="+",
        default=[
            "page.created",
            "page.properties_updated",
            "page.content_updated",
            "page.deleted",
            "page.moved",
            "database.content_updated",
            "database.schema_updated",
        ],
    )
    ap_reg.add_argument(
        "--database-id",
        help="optional: filter the subscription to a specific database/data source",
    )
    ap_reg.set_defaults(func=cmd_register)

    ap_listen = sub.add_parser("listen", help="run HTTP webhook receiver")
    ap_listen.add_argument("--bind", default="0.0.0.0")
    ap_listen.add_argument("--port", type=int, default=8787)
    ap_listen.add_argument("--output", default="./notion-cdc")
    ap_listen.add_argument(
        "--fetch-on-event",
        action="store_true",
        help="re-fetch page+blocks on every verified page-level event",
    )
    ap_listen.set_defaults(func=cmd_listen)

    ap_recon = sub.add_parser(
        "reconcile",
        help="poll data sources via last_edited_time as a missed-event safety net",
    )
    ap_recon.add_argument(
        "--data-sources", required=True, help="comma-separated data_source ids"
    )
    ap_recon.add_argument("--output", default="./notion-cdc")
    ap_recon.add_argument("--interval-seconds", type=int, default=300)
    ap_recon.add_argument(
        "--iterations", type=int, default=0, help="0 = run forever"
    )
    ap_recon.set_defaults(func=cmd_reconcile)

    return ap.parse_args()


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    notes_dir = script_dir.parent
    load_dotenv(notes_dir / ".env")
    args = parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
