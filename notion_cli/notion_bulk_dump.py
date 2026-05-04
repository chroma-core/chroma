#!/usr/bin/env python3
"""
Notion bulk dump — download every Notion page you have access to.

Target user: a developer at another company who wants their Notion content
on disk, with no admin access required. The script walks them through
creating one Notion OAuth app the first time they run it, then handles the
authorize → discover → fetch flow end to end.

Recommended one-liner:
    python3 notion_bulk_dump.py grab

That's it. On first run `grab` notices there's no OAuth app configured and
launches the `setup-app` walkthrough automatically. The user clicks through
the Notion integrations page, pastes back client_id / client_secret, then
authorizes their own newly-created app in the browser. We capture the token,
discover all accessible pages via POST /v1/search, and write each one to
<output>/pages/<page_id>.json.

Subcommands:

  grab        End-to-end. Triggers `setup-app` if no app is configured yet,
              otherwise loads the existing app(s), runs the OAuth flow,
              discovery, and parallel dump in one go. Default for users.

  setup-app   Interactive walkthrough that creates ONE Notion OAuth app
              entry in foundation_notes/notion_cli/notion-oauth-apps.json.
              Re-runnable to add more apps later (multi-app fan-out is
              supported by the storage format, but the UX assumes one app
              for now).

  oauth-setup Lower-level variant of `grab`'s authorization step that takes
              client_id / client_secret directly via flag/env. Useful when
              scripting against a pre-existing app.

  discover    Phase 1 only: paginate POST /v1/search, write discovery.jsonl,
              print the token-count plan. Reads tokens from --tokens-file.

  dump        Phase 1 + Phase 2: discover + per-page GET /v1/pages/{id}
              + recursive GET /v1/blocks/{id}/children. Use with --resume
              to recover a partially-completed `grab`.

Throughput model:
      tokens_needed = ceil(N_pages * AVG_REQS_PER_PAGE
                           / (TARGET_SECONDS * RPS_PER_TOKEN))
  Notion limits each OAuth connection to ~3 req/s. We default to 2.8 req/s
  per token for safety. With one app you get ~3 req/s; multi-app fan-out
  (later) scales linearly.

Env (loads foundation_notes/.env):
  NOTION_OAUTH_REDIRECT_URI  optional; default http://localhost:8765/callback.
                             Must EXACTLY match the redirect URI registered
                             on the OAuth app in the Notion dashboard.
  NOTION_OAUTH_CLIENT_ID     used only by the lower-level `oauth-setup`.
  NOTION_OAUTH_CLIENT_SECRET used only by the lower-level `oauth-setup`.

Token sources for `discover` / `dump` (any of, in priority order):
  1. --tokens-file (default: foundation_notes/notion_cli/notion-tokens.txt;
     one token per line, # comments allowed)
  2. --tokens "csv,of,tokens"
  3. NOTION_TOKENS env (csv)
  4. NOTION_TOKEN env (single)
"""

from __future__ import annotations

import argparse
import base64
import json
import math
import os
import queue
import secrets
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2026-03-11"
DEFAULT_RPS_PER_TOKEN = 2.8
DEFAULT_AVG_REQS_PER_PAGE = 3.0
DEFAULT_TARGET_SECONDS = 60
DEFAULT_WORKERS_PER_TOKEN = 8

NOTION_AUTHORIZE_URL = "https://api.notion.com/v1/oauth/authorize"
NOTION_TOKEN_URL = "https://api.notion.com/v1/oauth/token"
DEFAULT_REDIRECT_URI = "http://localhost:8765/callback"
DEFAULT_OAUTH_TIMEOUT_SECONDS = 300


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


# --------------------------------------------------------------------------
# HTTP + rate limiter
# --------------------------------------------------------------------------


class TokenBucket:
    """Thread-safe token bucket; one per Notion integration token."""

    def __init__(self, rate: float, burst: int | None = None) -> None:
        self.rate = float(rate)
        self.capacity = float(burst if burst is not None else max(1, int(rate)))
        self.tokens = self.capacity
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(
                    self.capacity, self.tokens + (now - self.last) * self.rate
                )
                self.last = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = (1 - self.tokens) / self.rate
            time.sleep(wait)


def http_request(
    url: str,
    token: str,
    method: str = "GET",
    body: dict | None = None,
    max_retries: int = 8,
    timeout: int = 60,
) -> dict:
    """Notion API call with retry on 429 (Retry-After) and 5xx (exponential backoff)."""
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
            raise RuntimeError(f"HTTP {e.code} on {method} {url}: {body_excerpt}") from e
        except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
            time.sleep(min(60.0, 2.0**attempt))
            last_err = e
            continue
    raise RuntimeError(f"max retries exceeded on {method} {url}: {last_err!r}")


# --------------------------------------------------------------------------
# Notion API wrappers
# --------------------------------------------------------------------------


def discover_pages(token: str, bucket: TokenBucket, max_pages: int = 0):
    cursor: str | None = None
    yielded = 0
    while True:
        bucket.acquire()
        body: dict = {
            "filter": {"property": "object", "value": "page"},
            "page_size": 100,
        }
        if cursor:
            body["start_cursor"] = cursor
        resp = http_request(f"{NOTION_API}/search", token, method="POST", body=body)
        for r in resp.get("results", []):
            yield r
            yielded += 1
            if max_pages and yielded >= max_pages:
                return
        if not resp.get("has_more"):
            return
        cursor = resp.get("next_cursor")


def fetch_page(token: str, bucket: TokenBucket, page_id: str) -> dict:
    bucket.acquire()
    return http_request(f"{NOTION_API}/pages/{page_id}", token)


def fetch_block_children(
    token: str, bucket: TokenBucket, block_id: str
) -> tuple[list[dict], int]:
    children: list[dict] = []
    cursor: str | None = None
    req_count = 0
    while True:
        bucket.acquire()
        url = f"{NOTION_API}/blocks/{block_id}/children?page_size=100"
        if cursor:
            url += f"&start_cursor={urllib.parse.quote(cursor)}"
        resp = http_request(url, token)
        req_count += 1
        children.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return children, req_count


def fetch_block_tree(
    token: str, bucket: TokenBucket, block_id: str, max_depth: int = 25
) -> tuple[list[dict], int]:
    children, total_reqs = fetch_block_children(token, bucket, block_id)
    if max_depth <= 0:
        return children, total_reqs
    for child in children:
        if child.get("has_children"):
            sub, sn = fetch_block_tree(token, bucket, child["id"], max_depth - 1)
            child["children"] = sub
            total_reqs += sn
    return children, total_reqs


def fetch_one_page(
    token: str, bucket: TokenBucket, page_id: str, output_dir: Path, resume: bool
) -> dict:
    out = output_dir / "pages" / f"{page_id}.json"
    if resume and out.exists():
        return {"id": page_id, "skipped": True, "reqs": 0, "blocks": 0}
    out.parent.mkdir(parents=True, exist_ok=True)
    page = fetch_page(token, bucket, page_id)
    blocks, block_reqs = fetch_block_tree(token, bucket, page_id)
    payload = {"page": page, "blocks": blocks}
    tmp = out.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    tmp.rename(out)
    return {"id": page_id, "skipped": False, "reqs": 1 + block_reqs, "blocks": len(blocks)}


# --------------------------------------------------------------------------
# Token loading + setup
# --------------------------------------------------------------------------


def default_tokens_file() -> Path:
    return Path(__file__).resolve().parent / "notion-tokens.txt"


def load_tokens(args: argparse.Namespace) -> list[str]:
    tokens: list[str] = []
    tf = Path(args.tokens_file).resolve() if getattr(args, "tokens_file", None) else default_tokens_file()
    if tf.is_file():
        for line in tf.read_text(encoding="utf-8").splitlines():
            t = line.strip()
            if t and not t.startswith("#"):
                tokens.append(t)
    if getattr(args, "tokens", ""):
        tokens.extend(t.strip() for t in args.tokens.split(",") if t.strip())
    if not tokens:
        env_csv = os.environ.get("NOTION_TOKENS", "").strip()
        if env_csv:
            tokens.extend(t.strip() for t in env_csv.split(",") if t.strip())
    if not tokens:
        single = os.environ.get("NOTION_TOKEN", "").strip()
        if single:
            tokens.append(single)
    seen: set[str] = set()
    deduped: list[str] = []
    for t in tokens:
        if t in seen:
            continue
        seen.add(t)
        deduped.append(t)
    return deduped


def _exchange_code_for_token(
    client_id: str, client_secret: str, code: str, redirect_uri: str
) -> dict:
    auth_b64 = base64.b64encode(
        f"{client_id}:{client_secret}".encode("utf-8")
    ).decode("ascii")
    body = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }
    req = urllib.request.Request(
        NOTION_TOKEN_URL,
        data=json.dumps(body).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Basic {auth_b64}",
            "Content-Type": "application/json",
            "Notion-Version": NOTION_VERSION,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        excerpt = e.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"OAuth token exchange failed: HTTP {e.code}: {excerpt}") from e


def _run_oauth_callback_server(
    redirect_uri: str, expected_state: str, timeout_seconds: int
) -> dict:
    """Block until Notion redirects back with ?code (or ?error). Returns the parsed query."""
    parsed = urllib.parse.urlparse(redirect_uri)
    if parsed.hostname not in ("localhost", "127.0.0.1"):
        raise RuntimeError(
            f"redirect_uri must be on localhost/127.0.0.1 for this script; got {redirect_uri!r}"
        )
    port = parsed.port or 8765
    callback_path = parsed.path or "/callback"
    received: dict = {}

    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, *args, **kwargs) -> None:  # noqa: A003
            return

        def do_GET(self) -> None:  # noqa: N802
            url = urllib.parse.urlparse(self.path)
            if url.path != callback_path:
                self._respond(404, "not found")
                return
            qs = {k: v[0] for k, v in urllib.parse.parse_qs(url.query).items()}
            if "error" in qs:
                received["error"] = qs.get("error_description") or qs["error"]
                self._respond(400, f"Authorization error: {received['error']}. You can close this tab.")
                return
            if qs.get("state") != expected_state:
                received["error"] = "state mismatch (possible CSRF)"
                self._respond(400, "State mismatch. You can close this tab.")
                return
            if "code" not in qs:
                received["error"] = "no code in callback"
                self._respond(400, "No code parameter. You can close this tab.")
                return
            received["code"] = qs["code"]
            self._respond(
                200,
                "Authorization received. You can close this tab and return to the terminal.",
            )

        def _respond(self, status: int, msg: str) -> None:
            payload = msg.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = HTTPServer(("127.0.0.1", port), _Handler)
    server.timeout = 1
    deadline = time.time() + timeout_seconds
    try:
        while time.time() < deadline and "code" not in received and "error" not in received:
            server.handle_request()
    finally:
        server.server_close()
    if "error" in received:
        raise RuntimeError(received["error"])
    if "code" not in received:
        raise RuntimeError(f"timed out after {timeout_seconds}s waiting for OAuth callback")
    return received


def _acquire_oauth_token(
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    *,
    open_browser: bool = True,
    timeout: int = DEFAULT_OAUTH_TIMEOUT_SECONDS,
    label: str = "",
) -> dict:
    """One round-trip OAuth flow. Returns the full Notion token response dict.

    Raises on user cancellation, callback timeout, or token-exchange failure.
    """
    state = secrets.token_urlsafe(24)
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "owner": "user",
        "state": state,
    }
    authorize_url = f"{NOTION_AUTHORIZE_URL}?{urllib.parse.urlencode(params)}"
    prefix = f"[oauth {label}]" if label else "[oauth]"
    print(f"{prefix} opening browser; if it doesn't open, paste this URL:")
    print(f"  {authorize_url}")
    if open_browser:
        try:
            webbrowser.open(authorize_url)
        except Exception:
            pass
    result = _run_oauth_callback_server(redirect_uri, state, timeout)
    return _exchange_code_for_token(
        client_id, client_secret, result["code"], redirect_uri
    )


def cmd_oauth_setup(args: argparse.Namespace) -> int:
    client_id = (args.client_id or os.environ.get("NOTION_OAUTH_CLIENT_ID", "")).strip()
    client_secret = (
        args.client_secret or os.environ.get("NOTION_OAUTH_CLIENT_SECRET", "")
    ).strip()
    redirect_uri = (
        args.redirect_uri
        or os.environ.get("NOTION_OAUTH_REDIRECT_URI", "")
        or DEFAULT_REDIRECT_URI
    ).strip()

    print("--- Notion public-connection OAuth setup ---")
    print()
    if not client_id or not client_secret:
        print("error: NOTION_OAUTH_CLIENT_ID and NOTION_OAUTH_CLIENT_SECRET are required.")
        print()
        print("First-time setup (do this once):")
        print("  1. Go to https://www.notion.so/profile/integrations")
        print("  2. Click 'New integration' → 'Public'.")
        print("  3. Set capabilities: 'Read content'.")
        print(f"  4. Add a redirect URI EXACTLY equal to: {redirect_uri}")
        print("  5. Set installation scope: 'Any workspace'.")
        print("  6. Copy the OAuth client ID and OAuth client secret into")
        print("     foundation_notes/.env as:")
        print("        NOTION_OAUTH_CLIENT_ID=…")
        print("        NOTION_OAUTH_CLIENT_SECRET=…")
        print()
        print("For multi-app fan-out, repeat steps 1–5 to create additional public")
        print("connections, then use `grab` with an apps file (preferred) or pass")
        print("--client-id/--client-secret per run.")
        return 2

    try:
        token_resp = _acquire_oauth_token(
            client_id,
            client_secret,
            redirect_uri,
            open_browser=not args.no_browser,
            timeout=args.timeout,
        )
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 3

    access_token = token_resp.get("access_token") or ""
    refresh_token = token_resp.get("refresh_token")
    bot_id = token_resp.get("bot_id") or ""
    workspace_id = token_resp.get("workspace_id") or ""
    workspace_name = token_resp.get("workspace_name") or "?"
    owner = token_resp.get("owner") or {}
    owner_user = owner.get("user") or {}
    owner_email = (owner_user.get("person") or {}).get("email") or ""
    owner_name = owner_user.get("name") or ""

    print(f"  bot_id:         {bot_id}")
    print(f"  workspace:      {workspace_name} ({workspace_id})")
    print(f"  authorized as:  {owner_name or '?'} <{owner_email or '?'}>")
    print(f"  refresh_token:  {'yes' if refresh_token else 'no'}")

    print()
    print("[validate] GET /v1/users/me …")
    try:
        me = http_request(f"{NOTION_API}/users/me", access_token)
        bot_name = me.get("name") or "(unnamed)"
        print(f"  bot name:       {bot_name}")
    except Exception as e:
        print(f"warning: /users/me failed: {e}", file=sys.stderr)
        bot_name = "(unknown)"

    print()
    print("[validate] POST /v1/search (counting accessible pages, capped at 1000) …")
    bucket = TokenBucket(DEFAULT_RPS_PER_TOKEN)
    visible = 0
    try:
        for _ in discover_pages(access_token, bucket, max_pages=1000):
            visible += 1
    except Exception as e:
        print(f"warning: search failed: {e}", file=sys.stderr)
    cap_note = " (capped — actual count may be higher)" if visible >= 1000 else ""
    print(f"  pages visible:  {visible}{cap_note}")
    if visible == 0:
        print(
            "  WARN: 0 pages visible. The OAuth picker only grants access to the\n"
            "  pages you ticked. Re-run `oauth-setup` and pick at least one root page,\n"
            "  or share more pages by re-authorizing.",
            file=sys.stderr,
        )

    tokens_path = Path(args.tokens_file).resolve()
    tokens_path.parent.mkdir(parents=True, exist_ok=True)
    existing = tokens_path.read_text(encoding="utf-8") if tokens_path.is_file() else ""
    if access_token in existing.splitlines():
        print()
        print(f"token already present in {tokens_path}; not duplicating.")
        print(
            "(Notion typically returns the SAME token when the SAME user re-authorizes\n"
            "the SAME public connection. For fan-out, use a different user OR a different\n"
            "public connection — see --client-id.)"
        )
        return 0

    suffix = "" if (not existing) or existing.endswith("\n") else "\n"
    with tokens_path.open("a", encoding="utf-8") as f:
        f.write(suffix)
        f.write(
            f"# OAuth token: bot={bot_name!r} bot_id={bot_id} "
            f"workspace={workspace_name!r} ({workspace_id}) "
            f"user={owner_email or owner_name or '?'} "
            f"app={client_id} "
            f"added {datetime.now(timezone.utc).isoformat()}\n"
        )
        f.write(f"{access_token}\n")
    nontok_lines = [
        l for l in tokens_path.read_text().splitlines()
        if l.strip() and not l.lstrip().startswith("#")
    ]
    print()
    print(f"appended token to {tokens_path}")
    print(f"total tokens now: {len(nontok_lines)}")
    print()
    print("To get more tokens for fan-out parallelism:")
    print("  - Have additional workspace members each run `oauth-setup` (multi-user).")
    print("  - OR register more public connections in the Notion creator dashboard,")
    print("    set --client-id/--client-secret per app, and re-run (multi-app).")
    return 0


# --------------------------------------------------------------------------
# Discover + dump
# --------------------------------------------------------------------------


def _run_discovery(
    tokens: list[str],
    output: Path,
    max_pages: int,
    rps_per_token: float,
) -> tuple[list[dict], float, Path]:
    print(f"[discovery] using token #1 of {len(tokens)} to walk POST /v1/search …")
    discovery_path = output / "discovery.jsonl"
    page_index: list[dict] = []
    bucket0 = TokenBucket(rps_per_token, burst=int(rps_per_token) + 1)
    t0 = time.time()
    with discovery_path.open("w", encoding="utf-8") as f:
        for page in discover_pages(tokens[0], bucket0, max_pages=max_pages):
            entry = {
                "id": page["id"],
                "object": page.get("object"),
                "last_edited_time": page.get("last_edited_time"),
                "created_time": page.get("created_time"),
                "in_trash": page.get("in_trash", False),
                "archived": page.get("archived", False),
                "url": page.get("url"),
                "parent": page.get("parent"),
            }
            f.write(json.dumps(entry) + "\n")
            page_index.append(entry)
    discovery_secs = time.time() - t0
    print(f"[discovery] found {len(page_index)} pages in {discovery_secs:.1f}s")
    print(f"[discovery] wrote {discovery_path}")
    return page_index, discovery_secs, discovery_path


def _print_plan(
    n_pages: int,
    tokens: list[str],
    rps_per_token: float,
    target_seconds: int,
    avg_reqs_per_page: float,
) -> int:
    reqs_total = n_pages * avg_reqs_per_page
    rps_total = len(tokens) * rps_per_token
    projected_secs = reqs_total / rps_total if rps_total else float("inf")
    if target_seconds > 0:
        tokens_needed = max(
            1, math.ceil(reqs_total / (target_seconds * rps_per_token))
        )
    else:
        tokens_needed = 0
    print()
    print("--- plan ---")
    print(f"pages discovered:        {n_pages}")
    print(f"avg reqs per page:       {avg_reqs_per_page}")
    print(f"rps per token:           {rps_per_token}")
    print(f"target seconds:          {target_seconds}")
    print(f"tokens provided:         {len(tokens)}")
    print(f"tokens needed @ target:  {tokens_needed}")
    print(f"projected fetch time:    {projected_secs:.1f}s with {len(tokens)} token(s)")
    if tokens_needed > len(tokens):
        deficit = tokens_needed - len(tokens)
        print(
            f"NOTE: {deficit} more token(s) needed to hit the {target_seconds}s "
            f"target. Run `setup-token` to register more, or lower the target."
        )
    return tokens_needed


def _run_fetch(
    tokens: list[str],
    page_index: list[dict],
    output: Path,
    rps_per_token: float,
    workers_per_token: int,
    resume: bool,
) -> int:
    n_pages = len(page_index)
    print()
    print(
        f"[fetch] starting with {len(tokens)} token(s) × {workers_per_token} "
        f"workers/token = {len(tokens) * workers_per_token} threads …"
    )
    buckets = [
        TokenBucket(rps_per_token, burst=int(rps_per_token) + 1)
        for _ in tokens
    ]
    work_q: queue.Queue[dict] = queue.Queue()
    for entry in page_index:
        work_q.put(entry)

    n_workers = len(tokens) * workers_per_token
    progress = {"done": 0, "skipped": 0, "failed": 0, "reqs": 0}
    progress_lock = threading.Lock()
    manifest_lock = threading.Lock()
    manifest_path = output / "manifest.jsonl"
    manifest_f = manifest_path.open("a", encoding="utf-8")

    def worker(idx: int) -> None:
        token_idx = idx % len(tokens)
        token = tokens[token_idx]
        bucket = buckets[token_idx]
        while True:
            try:
                entry = work_q.get_nowait()
            except queue.Empty:
                return
            page_id = entry["id"]
            try:
                result = fetch_one_page(token, bucket, page_id, output, resume)
            except Exception as e:
                with progress_lock:
                    progress["failed"] += 1
                with manifest_lock:
                    manifest_f.write(
                        json.dumps(
                            {
                                **entry,
                                "error": str(e)[:500],
                                "fetched_at": datetime.now(timezone.utc).isoformat(),
                                "token_idx": token_idx,
                            }
                        )
                        + "\n"
                    )
                    manifest_f.flush()
                print(f"[fetch] FAIL {page_id}: {e}", flush=True)
                continue

            with manifest_lock:
                manifest_f.write(
                    json.dumps(
                        {
                            **entry,
                            **result,
                            "fetched_at": datetime.now(timezone.utc).isoformat(),
                            "token_idx": token_idx,
                        }
                    )
                    + "\n"
                )
                manifest_f.flush()
            with progress_lock:
                if result.get("skipped"):
                    progress["skipped"] += 1
                else:
                    progress["done"] += 1
                progress["reqs"] += int(result.get("reqs", 0))
                processed = progress["done"] + progress["skipped"] + progress["failed"]
                if processed % 50 == 0 or processed == n_pages:
                    print(
                        f"[fetch] {processed}/{n_pages} "
                        f"(done={progress['done']} skipped={progress['skipped']} "
                        f"failed={progress['failed']} reqs={progress['reqs']})",
                        flush=True,
                    )

    fetch_t0 = time.time()
    with ThreadPoolExecutor(max_workers=n_workers) as exe:
        futures = [exe.submit(worker, i) for i in range(n_workers)]
        for f in as_completed(futures):
            f.result()
    fetch_secs = time.time() - fetch_t0
    manifest_f.close()

    print()
    print("--- fetch summary ---")
    print(
        f"pages: done={progress['done']} skipped={progress['skipped']} "
        f"failed={progress['failed']}"
    )
    print(f"requests issued:    {progress['reqs']}")
    print(f"fetch wall time:    {fetch_secs:.1f}s")
    if fetch_secs > 0:
        print(
            f"throughput:         {progress['reqs']/fetch_secs:.1f} req/s "
            f"({progress['done']/fetch_secs:.1f} pages/s)"
        )
    print(f"output dir:         {output}")
    print(f"manifest:           {manifest_path}")
    return 0 if progress["failed"] == 0 else 4


def _default_apps_file() -> Path:
    return Path(__file__).resolve().parent / "notion-oauth-apps.json"


def _load_apps_file(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    raw = path.read_text(encoding="utf-8")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"failed to parse {path}: {e}") from e
    if isinstance(data, dict) and "apps" in data:
        apps = data["apps"]
    elif isinstance(data, list):
        apps = data
    else:
        raise RuntimeError(
            f"unexpected JSON shape in {path}: expected list or {{'apps': [...]}}"
        )
    out: list[dict] = []
    for i, a in enumerate(apps):
        if not isinstance(a, dict):
            raise RuntimeError(f"{path}: apps[{i}] is not a JSON object")
        out.append(a)
    return out


def cmd_setup_app(args: argparse.Namespace) -> int:
    apps_path = Path(args.apps_file).resolve()
    redirect_uri = (
        args.redirect_uri
        or os.environ.get("NOTION_OAUTH_REDIRECT_URI", "")
        or DEFAULT_REDIRECT_URI
    ).strip()

    if not sys.stdin.isatty():
        print(
            "error: setup-app requires an interactive terminal "
            "(or pre-create the apps file manually).",
            file=sys.stderr,
        )
        return 2

    existing: list[dict] = []
    if apps_path.is_file():
        try:
            existing = _load_apps_file(apps_path)
        except Exception:
            existing = []
        if existing:
            print(f"--- Notion OAuth app setup ---")
            print()
            print(f"You already have {len(existing)} app(s) configured at:")
            print(f"  {apps_path}")
            print()
            ans = input("Add another app? [y/N]: ").strip().lower()
            if ans not in ("y", "yes"):
                print("(no changes)")
                return 0

    print()
    print("--- Notion OAuth app setup ---")
    print()
    print("To download your Notion data, you need to create a Notion")
    print("'public connection' (an OAuth app) in your own Notion account.")
    print("Takes ~3 minutes; only required once.")
    print()
    print("Heads up — Notion permissions:")
    print("  Only a 'Workspace Owner' can create integrations. If you're a")
    print("  regular Member of your work workspace you'll see this error on")
    print("  the form:")
    print('    "You don\'t have permission to create integrations in any')
    print('     workspaces. Please create a new workspace or contact a')
    print('     workspace owner for access."')
    print("  Easiest fix: click the 'create a new workspace' link in that")
    print("  error and make a free personal workspace (you're the owner of")
    print("  any workspace you create). Reload the form, choose your new")
    print("  personal workspace under 'Associated workspace', and set")
    print("  'Installable in' = 'Any workspace'. The integration will live")
    print("  in your personal workspace but the OAuth flow can still grant")
    print("  it access to pages in your work workspace.")
    print()
    print("Step 1: Open the Notion 'New public integration' form.")
    print("        I'll open it in your browser; sign in if needed.")
    input("        [press Enter when ready] ")
    if not args.no_browser:
        try:
            webbrowser.open(
                "https://www.notion.so/profile/integrations/public/form/new-integration"
            )
        except Exception:
            pass

    print()
    print("Step 2: Fill in the form. Use these exact values where called out:")
    print()
    print("          Connection name:      Notion Bulk Download   (or anything)")
    print("          Associated workspace: <pick a workspace where you are owner>")
    print("          Capabilities:         'Read content'")
    print("          Installable in:       'Any workspace'")
    print(f"          Redirect URI:         {redirect_uri}")
    print()
    print("        Then click 'Save'.")
    input("        [press Enter when ready] ")

    print()
    print("Step 3: After saving, the Configuration tab shows your OAuth credentials.")
    print("        Find 'OAuth client ID' and 'OAuth client secret' and paste below.")
    print()
    client_id = input("Client ID: ").strip()
    if not client_id:
        print("error: empty client_id", file=sys.stderr)
        return 2
    try:
        from getpass import getpass

        client_secret = getpass("Client Secret (input hidden): ").strip()
    except Exception:
        client_secret = input("Client Secret: ").strip()
    if not client_secret:
        print("error: empty client_secret", file=sys.stderr)
        return 2
    name = (
        getattr(args, "name", None)
        or input("App name [Notion Bulk Download]: ").strip()
        or "Notion Bulk Download"
    )

    new_app = {
        "name": name,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    existing.append(new_app)
    apps_path.parent.mkdir(parents=True, exist_ok=True)
    apps_path.write_text(
        json.dumps(existing, indent=2) + "\n", encoding="utf-8"
    )
    print()
    print(f"saved {name!r} to {apps_path}")
    print(f"({len(existing)} app(s) total)")
    print()
    print("You're ready to download. Run:")
    print("  ./notion_bulk_dump.sh grab")
    return 0


def cmd_grab(args: argparse.Namespace) -> int:
    apps_path = Path(args.apps_file).resolve()
    redirect_uri = (
        args.redirect_uri
        or os.environ.get("NOTION_OAUTH_REDIRECT_URI", "")
        or DEFAULT_REDIRECT_URI
    ).strip()

    try:
        apps = _load_apps_file(apps_path)
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    if not apps:
        print(f"No OAuth app configured yet at {apps_path}.")
        print("Walking you through creating one now.")
        print()
        rc = cmd_setup_app(args)
        if rc != 0:
            return rc
        try:
            apps = _load_apps_file(apps_path)
        except Exception as e:
            print(f"error: {e}", file=sys.stderr)
            return 2
        if not apps:
            print("error: setup-app produced no apps", file=sys.stderr)
            return 2

    if args.max_tokens > 0:
        apps = apps[: args.max_tokens]

    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tokens_path = Path(args.tokens_file).resolve()
    tokens_path.parent.mkdir(parents=True, exist_ok=True)

    print("--- notion grab ---")
    print(f"apps configured:        {len(apps)}")
    print(f"output dir:             {output}")
    print(f"tokens-file:            {tokens_path}")
    print(f"target seconds:         {args.target_seconds}")
    print(f"redirect uri:           {redirect_uri}")
    print()
    if len(apps) == 1:
        print("You'll be asked to authorize this app in your browser.")
    else:
        print(f"You'll authorize {len(apps)} apps in your browser, one at a time.")
        print("Pick the SAME workspace every time and tick the SAME pages each time.")
        print("(Each app is its own bot; Notion has no API to share grants between bots.)")
    print()
    print("IMPORTANT — what the OAuth page picker will and will NOT show:")
    print("  Notion's picker only displays pages where you have 'Full access'")
    print("  (view + edit + share). Sharing a page with a connection is itself")
    print("  a share operation; only Full access qualifies. See:")
    print("    https://developers.notion.com/docs/authorization")
    print()
    print("  In your personal workspace you have Full access to everything, so")
    print("  the picker shows it all. In a company workspace you usually have")
    print("  Full access ONLY to your own Private pages — teamspace pages are")
    print("  typically 'Can edit' (or less), which Notion HIDES from the picker.")
    print()
    print("  If the picker shows almost nothing for a company workspace, this")
    print("  is the cause. Workarounds (any one):")
    print("    (a) Ask a workspace owner to create an *internal* integration")
    print("        and share the teamspaces with its bot. Use that token here")
    print("        instead — set NOTION_TOKEN=<token> and run `dump`. (See")
    print("        ./notion_cdc.sh setup-token, which walks an owner through it.)")
    print("    (b) Ask a workspace owner to share the teamspaces with THIS")
    print("        public integration via the page's ••• → Connections → Add")
    print("        menu (they have Full access; they can share). Then re-run")
    print("        `grab` and the pages will appear in your picker.")
    print("    (c) Ask a workspace owner to grant YOU Full access on the")
    print("        teamspaces you want. Re-run `grab`; the picker will then")
    print("        list them.")
    print()

    tokens: list[str] = []
    workspace_ids: set[str] = set()
    workspace_names: list[str] = []

    for i, app in enumerate(apps, 1):
        client_id = (app.get("client_id") or "").strip()
        client_secret = (app.get("client_secret") or "").strip()
        name = app.get("name") or f"app#{i}"
        if not client_id or not client_secret:
            print(
                f"[{i}/{len(apps)}] {name}: missing client_id/client_secret; skipping",
                file=sys.stderr,
            )
            continue
        try:
            resp = _acquire_oauth_token(
                client_id,
                client_secret,
                redirect_uri,
                open_browser=not args.no_browser,
                timeout=args.timeout,
                label=f"{i}/{len(apps)} {name}",
            )
        except Exception as e:
            print(f"[{i}/{len(apps)}] {name}: oauth failed: {e}", file=sys.stderr)
            if args.strict_apps:
                return 3
            continue

        access_token = resp.get("access_token") or ""
        bot_id = resp.get("bot_id") or ""
        workspace_id = resp.get("workspace_id") or ""
        workspace_name = resp.get("workspace_name") or "?"
        owner = resp.get("owner") or {}
        owner_user = owner.get("user") or {}
        owner_email = (owner_user.get("person") or {}).get("email") or ""

        print(
            f"  ✓ bot_id={bot_id} workspace={workspace_name!r} ({workspace_id}) "
            f"user={owner_email or '?'}"
        )
        if access_token in tokens:
            print(
                "    (Notion returned a token we already have — same user re-authorized "
                "the same app. Not counted toward fan-out.)"
            )
            continue
        tokens.append(access_token)
        workspace_ids.add(workspace_id)
        workspace_names.append(workspace_name)
        with tokens_path.open("a", encoding="utf-8") as f:
            f.write(
                f"# OAuth token via grab: bot_id={bot_id} "
                f"workspace={workspace_name!r} ({workspace_id}) "
                f"user={owner_email or '?'} app={name} "
                f"added {datetime.now(timezone.utc).isoformat()}\n"
            )
            f.write(f"{access_token}\n")

    if not tokens:
        print("error: no tokens acquired", file=sys.stderr)
        return 3

    if len(workspace_ids) > 1:
        print(
            f"error: tokens span multiple workspaces ({sorted(set(workspace_names))}); "
            "the dump phase can only target one workspace at a time. Either re-run "
            "and pick the same workspace for every app, or split tokens-file by "
            "workspace_id and run `dump --tokens-file ...` per workspace.",
            file=sys.stderr,
        )
        return 4

    print()
    print(
        f"acquired {len(tokens)} token(s) for workspace "
        f"{next(iter(workspace_names), '?')!r}"
    )

    page_index, _, _ = _run_discovery(
        tokens, output, args.max_pages, args.rps_per_token
    )
    if len(page_index) < 20:
        print()
        print(
            f"WARNING: discovery only found {len(page_index)} page(s). In a company"
        )
        print(
            "  workspace this almost always means Notion's OAuth picker filtered"
        )
        print(
            "  out everything you don't have FULL ACCESS to (which is most teamspace"
        )
        print(
            "  content unless you're a teamspace owner). The picker is doing its job;"
        )
        print(
            "  there is no checkbox or scope to override it. See:"
        )
        print("    https://developers.notion.com/docs/authorization")
        print()
        print("  To pull more, you need a workspace owner. Pick one:")
        print("    (a) Owner creates an INTERNAL integration and shares the")
        print("        teamspaces with its bot, then gives you the token. Use it via")
        print("        NOTION_TOKEN=<token> ./notion_bulk_dump.sh dump. The owner")
        print("        flow is in ./notion_cdc.sh setup-token.")
        print("    (b) Owner shares the teamspaces with THIS public integration via")
        print("        each page's ••• → Connections → Add. Re-run `grab` after.")
        print("    (c) Owner grants YOU Full access on the teamspaces you want.")
        print("        Re-run `grab`; they'll show up in your picker.")
        print()
        print("  Continuing with the current grant for now…")
        print()
    tokens_needed = _print_plan(
        len(page_index),
        tokens,
        args.rps_per_token,
        args.target_seconds,
        args.avg_reqs_per_page,
    )
    if args.strict_tokens and tokens_needed > len(tokens):
        print("strict-tokens set; exiting before fetch.", file=sys.stderr)
        return 5
    return _run_fetch(
        tokens,
        page_index,
        output,
        args.rps_per_token,
        args.workers_per_token,
        args.resume,
    )


def cmd_discover(args: argparse.Namespace) -> int:
    tokens = load_tokens(args)
    if not tokens:
        print(
            "error: no tokens found. Run `setup-token` first, or set "
            "NOTION_TOKEN(S) / --tokens / --tokens-file.",
            file=sys.stderr,
        )
        return 2
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    page_index, _, _ = _run_discovery(
        tokens, output, args.max_pages, args.rps_per_token
    )
    tokens_needed = _print_plan(
        len(page_index),
        tokens,
        args.rps_per_token,
        args.target_seconds,
        args.avg_reqs_per_page,
    )
    if args.strict_tokens and tokens_needed > len(tokens):
        return 3
    return 0


def cmd_dump(args: argparse.Namespace) -> int:
    tokens = load_tokens(args)
    if not tokens:
        print(
            "error: no tokens found. Run `setup-token` first, or set "
            "NOTION_TOKEN(S) / --tokens / --tokens-file.",
            file=sys.stderr,
        )
        return 2
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    page_index, _, _ = _run_discovery(
        tokens, output, args.max_pages, args.rps_per_token
    )
    tokens_needed = _print_plan(
        len(page_index),
        tokens,
        args.rps_per_token,
        args.target_seconds,
        args.avg_reqs_per_page,
    )
    if args.strict_tokens and tokens_needed > len(tokens):
        print("strict-tokens set; exiting before fetch.", file=sys.stderr)
        return 3
    return _run_fetch(
        tokens,
        page_index,
        output,
        args.rps_per_token,
        args.workers_per_token,
        args.resume,
    )


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _add_common_dump_flags(p: argparse.ArgumentParser) -> None:
    p.add_argument("--output", default="./notion-dump", help="output directory")
    p.add_argument("--tokens", default="", help="comma-separated tokens (additive to --tokens-file)")
    p.add_argument(
        "--tokens-file",
        default=str(default_tokens_file()),
        help="path to tokens file (one per line, # comments allowed)",
    )
    p.add_argument(
        "--rps-per-token",
        type=float,
        default=DEFAULT_RPS_PER_TOKEN,
        help=f"sustained req/s per token (default {DEFAULT_RPS_PER_TOKEN}; Notion's documented limit is 3)",
    )
    p.add_argument(
        "--target-seconds",
        type=int,
        default=DEFAULT_TARGET_SECONDS,
        help=f"target fetch duration in seconds for token-count planning (default {DEFAULT_TARGET_SECONDS})",
    )
    p.add_argument(
        "--avg-reqs-per-page",
        type=float,
        default=DEFAULT_AVG_REQS_PER_PAGE,
        help=f"estimated avg API calls per page for planning (default {DEFAULT_AVG_REQS_PER_PAGE})",
    )
    p.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="cap discovery for testing (0 = no cap)",
    )
    p.add_argument(
        "--strict-tokens",
        action="store_true",
        help="exit non-zero if provided token count is below tokens_needed",
    )


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_setup_app = sub.add_parser(
        "setup-app",
        help="interactive walkthrough for creating a Notion OAuth app",
    )
    ap_setup_app.add_argument(
        "--apps-file",
        default=str(_default_apps_file()),
        help="where to save app config (default: foundation_notes/notion_cli/notion-oauth-apps.json)",
    )
    ap_setup_app.add_argument(
        "--redirect-uri",
        default="",
        help=f"OAuth redirect URI (default: NOTION_OAUTH_REDIRECT_URI or {DEFAULT_REDIRECT_URI})",
    )
    ap_setup_app.add_argument(
        "--no-browser",
        action="store_true",
        help="don't auto-open the integrations page",
    )
    ap_setup_app.add_argument(
        "--name",
        default=None,
        help="app name (skips the prompt)",
    )
    ap_setup_app.set_defaults(func=cmd_setup_app)

    ap_grab = sub.add_parser(
        "grab",
        help="end-to-end: ensure OAuth app is set up + discover + dump (recommended)",
    )
    ap_grab.add_argument(
        "--apps-file",
        default=str(_default_apps_file()),
        help="JSON file listing OAuth apps (default: foundation_notes/notion_cli/notion-oauth-apps.json)",
    )
    ap_grab.add_argument(
        "--redirect-uri",
        default="",
        help=f"shared OAuth redirect URI (default: NOTION_OAUTH_REDIRECT_URI or {DEFAULT_REDIRECT_URI})",
    )
    ap_grab.add_argument(
        "--no-browser",
        action="store_true",
        help="don't auto-open the authorization URL",
    )
    ap_grab.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_OAUTH_TIMEOUT_SECONDS,
        help=f"per-app oauth callback timeout in seconds (default {DEFAULT_OAUTH_TIMEOUT_SECONDS})",
    )
    ap_grab.add_argument(
        "--max-tokens",
        type=int,
        default=0,
        help="cap apps used (0 = use all listed in apps-file)",
    )
    ap_grab.add_argument(
        "--strict-apps",
        action="store_true",
        help="abort on first OAuth failure instead of skipping",
    )
    _add_common_dump_flags(ap_grab)
    ap_grab.add_argument(
        "--workers-per-token",
        type=int,
        default=DEFAULT_WORKERS_PER_TOKEN,
        help=f"thread workers per token (default {DEFAULT_WORKERS_PER_TOKEN})",
    )
    ap_grab.add_argument("--resume", action="store_true", help="skip pages already on disk")
    ap_grab.add_argument(
        "--max-depth", type=int, default=25, help="max recursion depth on block tree"
    )
    ap_grab.set_defaults(func=cmd_grab)

    ap_oauth = sub.add_parser(
        "oauth-setup",
        help="run the public-connection OAuth flow and save the resulting token",
    )
    ap_oauth.add_argument(
        "--tokens-file",
        default=str(default_tokens_file()),
        help="where to append the token (default: foundation_notes/notion_cli/notion-tokens.txt)",
    )
    ap_oauth.add_argument(
        "--client-id",
        default="",
        help="OAuth client id (overrides NOTION_OAUTH_CLIENT_ID)",
    )
    ap_oauth.add_argument(
        "--client-secret",
        default="",
        help="OAuth client secret (overrides NOTION_OAUTH_CLIENT_SECRET)",
    )
    ap_oauth.add_argument(
        "--redirect-uri",
        default="",
        help=f"OAuth redirect URI (default: NOTION_OAUTH_REDIRECT_URI or {DEFAULT_REDIRECT_URI})",
    )
    ap_oauth.add_argument(
        "--no-browser",
        action="store_true",
        help="don't auto-open the authorization URL",
    )
    ap_oauth.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_OAUTH_TIMEOUT_SECONDS,
        help=f"seconds to wait for the OAuth callback (default {DEFAULT_OAUTH_TIMEOUT_SECONDS})",
    )
    ap_oauth.set_defaults(func=cmd_oauth_setup)

    ap_discover = sub.add_parser(
        "discover",
        help="phase 1 only: enumerate pages and print the token-count plan",
    )
    _add_common_dump_flags(ap_discover)
    ap_discover.set_defaults(func=cmd_discover)

    ap_dump = sub.add_parser(
        "dump", help="phase 1 + phase 2: full bulk dump"
    )
    _add_common_dump_flags(ap_dump)
    ap_dump.add_argument(
        "--workers-per-token",
        type=int,
        default=DEFAULT_WORKERS_PER_TOKEN,
        help=f"thread workers per token (default {DEFAULT_WORKERS_PER_TOKEN}); the bucket is the real throttle",
    )
    ap_dump.add_argument("--resume", action="store_true", help="skip pages already on disk")
    ap_dump.add_argument(
        "--max-depth", type=int, default=25, help="max recursion depth on block tree"
    )
    ap_dump.set_defaults(func=cmd_dump)

    return ap.parse_args()


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    notes_dir = script_dir.parent
    load_dotenv(notes_dir / ".env")
    args = parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
