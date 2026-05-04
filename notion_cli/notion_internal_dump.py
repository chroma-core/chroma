#!/usr/bin/env python3
"""
Notion internal-API dump — bypass the Full-access OAuth picker.

Why this exists:
  Notion's PUBLIC OAuth API (used by notion_bulk_dump.py) only sees pages
  where the authorizing user has FULL access. In a typical company workspace
  a regular Member has Full access only on their Private pages, so the OAuth
  picker hides the entire teamspace tree. This script bypasses that by using
  the same /api/v3 endpoints that Notion's web app uses, authenticated by
  the user's session cookie (token_v2). Those endpoints return everything
  the user can VIEW — Can edit, Can comment, Can view all qualify.

  ToS note: this is undocumented and arguably against Notion's terms of
  service ("scrape", "data mining"). Used responsibly (modest rate, personal
  / internal use), enforcement is rare. Don't republish, don't hammer.

Phases:
  Phase 0 — auth (run this once per session, ~30s):
    login        Guided in-terminal login: opens notion.so, polls your browser
                 cookie store, validates, prompts for workspace, saves token.

  Phase 1 — validate permissions (cheap, run this first):
    discover     /api/v3/search across the chosen space; print page count.

  Phase 2 — bulk export at scale (the actual dump):
    dump         Enqueue exportBlock for top-level containers, poll, download
                 zips, unzip into <output>/exports/<container>/.
    grab         All-in-one: discover → dump.

Auth (token_v2 cookie sources, priority order):
  1. --token-v2 flag
  2. NOTION_TOKEN_V2 env
  3. foundation_notes/notion_cli/notion-token-v2.txt (one line, written
     by `login`)
  4. browser_cookie3 auto-extract from any Chromium-family browser, Firefox,
     or Safari (if `pip install browser_cookie3` succeeded). Triggers a
     Touch ID / Keychain prompt on macOS the first time.

Manual cookie copy (fallback if `login` can't read your browser):
  1. Open notion.so in any browser, sign in.
  2. DevTools (Cmd-Opt-I) → Application → Storage → Cookies → www.notion.so.
  3. Copy the Value of `token_v2` (long opaque blob) into
     foundation_notes/notion_cli/notion-token-v2.txt OR export
     NOTION_TOKEN_V2.

Env (loads foundation_notes/.env):
  NOTION_TOKEN_V2          Notion session cookie (see above).
  NOTION_INTERNAL_SPACE_ID Optional; pin to one workspace if your account
                           is in multiple. Otherwise the script prompts.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import select
import shutil
import sys
import threading
import time
import urllib.error
import urllib.request
import webbrowser
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

API_BASE = "https://www.notion.so/api/v3"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
DEFAULT_TIMEOUT = 30
DEFAULT_RPS = 6.0
DEFAULT_POLL_RPS = 3.0
# Notion's per-user concurrent server-side export task limit appears to be
# around 4-5. Going higher just causes 429s on getTasks; the actual
# throughput is the same or worse because workers waste cycles waiting out
# the rate-limit cooldown. 4 is the sweet spot.
DEFAULT_EXPORT_PARALLEL = 4
DEFAULT_POLL_INTERVAL = 5.0
DEFAULT_TASK_TIMEOUT = 1800
DEFAULT_OUTPUT = "./notion-internal-dump"
# When a 429 response has no Retry-After header, fall back to this and
# exponentially back off from it, capped at MAX_BACKOFF_S.
DEFAULT_BACKOFF_INITIAL_S = 5.0
MAX_BACKOFF_S = 60.0


# ---------------------------------------------------------------------------
# .env loader (same shape as the other foundation_notes scripts)
# ---------------------------------------------------------------------------


def _load_env() -> None:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        os.environ.setdefault(k, v)


_load_env()


def _env_path() -> Path:
    return Path(__file__).resolve().parent.parent / ".env"


def _default_token_path() -> Path:
    return Path(__file__).resolve().parent / "notion-token-v2.txt"


def _persist_env_var(key: str, value: str) -> Path:
    """Upsert `export KEY=value` in foundation_notes/.env. Returns the path
    that was written. Idempotent: leaves other lines untouched.
    """
    path = _env_path()
    lines: list[str] = []
    found = False
    if path.exists():
        for raw in path.read_text(encoding="utf-8").splitlines():
            stripped = raw.strip()
            if (
                stripped.startswith(f"{key}=")
                or stripped.startswith(f"export {key}=")
            ):
                lines.append(f"export {key}={value}")
                found = True
            else:
                lines.append(raw)
    if not found:
        lines.append(f"export {key}={value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.environ[key] = value
    return path


def _all_chrome_cookie_files() -> list[str]:
    """All Chrome/Chromium cookie SQLite files we know how to look at."""
    import glob
    roots = [
        os.path.expanduser("~/Library/Application Support/Google/Chrome"),
        os.path.expanduser("~/Library/Application Support/Google/Chrome Beta"),
        os.path.expanduser("~/Library/Application Support/Google/Chrome Canary"),
        os.path.expanduser("~/Library/Application Support/Arc/User Data"),
        os.path.expanduser("~/Library/Application Support/BraveSoftware/Brave-Browser"),
        os.path.expanduser("~/Library/Application Support/Microsoft Edge"),
        os.path.expanduser("~/Library/Application Support/Vivaldi"),
    ]
    out: list[str] = []
    for root in roots:
        if not os.path.isdir(root):
            continue
        for path in glob.glob(f"{root}/*/Cookies") + glob.glob(f"{root}/*/Network/Cookies"):
            out.append(path)
    return out


CookieKey = tuple[str, str, str]


def _patch_browser_cookie3_for_wal() -> None:
    """Make browser_cookie3 copy `Cookies-wal`, `Cookies-shm`, and
    `Cookies-journal` alongside the main `Cookies` file when it falls back
    to its legacy "copy to temp" connection. Without this, fresh writes
    that are still in the WAL/journal can be invisible until SQLite
    checkpoints. No-op if browser_cookie3's internals don't match what we
    expect.
    """
    try:
        import browser_cookie3  # type: ignore
    except Exception:
        return
    cls = getattr(browser_cookie3, "_DatabaseConnetion", None)
    if cls is None or getattr(cls, "_wal_patched", False):
        return
    legacy_attr = "_DatabaseConnetion__get_connection_legacy"
    orig = getattr(cls, legacy_attr, None)
    if orig is None:
        return

    def _patched_legacy(self):  # type: ignore[no-redef]
        import tempfile  # noqa: PLC0415
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as tf:
            tmp = tf.name
        src = self._DatabaseConnetion__database_file  # type: ignore[attr-defined]
        try:
            shutil.copyfile(src, tmp)
            for ext in ("-wal", "-shm", "-journal"):
                sib = str(src) + ext
                if os.path.exists(sib):
                    try:
                        shutil.copyfile(sib, tmp + ext)
                    except Exception:
                        pass
        except PermissionError:
            return None
        import sqlite3  # noqa: PLC0415
        con = sqlite3.connect(tmp)
        self._DatabaseConnetion__temp_cookie_file = tmp  # type: ignore[attr-defined]
        try:
            con.cursor().execute("select 1 from sqlite_master")
        except sqlite3.OperationalError:
            return None
        return con

    setattr(cls, legacy_attr, _patched_legacy)
    cls._wal_patched = True


_patch_browser_cookie3_for_wal()


def _vlog(verbose: bool, msg: str) -> None:
    if verbose:
        print(f"  [verbose] {msg}", file=sys.stderr)


def _extract_notion_cookies_from(
    cookie_file: str, *, verbose: bool = False
) -> dict[CookieKey, dict]:
    """Returns {(name,domain,path): {value, domain, path, httponly}} for
    notion.so / notion.com cookies in `cookie_file`. Decrypts via
    browser_cookie3 (may trigger a Touch ID / Keychain prompt on macOS).
    """
    try:
        import browser_cookie3  # type: ignore
    except Exception:
        _vlog(verbose, "browser_cookie3 not installed")
        return {}
    label = _short_chrome_label(cookie_file)
    if verbose:
        try:
            mt = os.path.getmtime(cookie_file)
            when = datetime.fromtimestamp(mt).strftime("%H:%M:%S")
            wal = cookie_file + "-wal"
            wal_size = os.path.getsize(wal) if os.path.exists(wal) else 0
            _vlog(verbose, f"{label}: mtime={when} wal_bytes={wal_size}")
        except OSError as e:
            _vlog(verbose, f"{label}: stat failed: {e}")
    try:
        cj = browser_cookie3.chrome(cookie_file=cookie_file, domain_name="notion")
    except Exception as e:
        _vlog(verbose, f"{label}: extraction failed: {e}")
        return {}
    out: dict[CookieKey, dict] = {}
    for c in cj:
        if "notion" not in c.domain:
            continue
        out[(c.name, c.domain, c.path)] = {
            "name": c.name,
            "value": c.value or "",
            "domain": c.domain,
            "path": c.path,
            "httponly": getattr(c, "_rest", {}).get("HttpOnly", False),
        }
    if verbose:
        tokens = [(k, v) for k, v in out.items() if k[0] == "token_v2"]
        if tokens:
            _vlog(verbose, f"{label}: {len(tokens)} token_v2 row(s):")
            for k, v in tokens:
                _vlog(
                    verbose,
                    f"  - domain={k[1]} path={k[2]} val_len={len(v['value'])} "
                    f"prefix={(v['value'] or '')[:24]}",
                )
            other_names = sorted({k[0] for k in out.keys() if k[0] != "token_v2"})
            if other_names:
                _vlog(verbose, f"{label}: other notion cookies: {other_names}")
        else:
            names = sorted({k[0] for k in out.keys()})
            _vlog(
                verbose,
                f"{label}: token_v2 MISSING. notion cookies present: {names or '[]'}",
            )
    return out


def _extract_notion_cookies_via(
    name: str, *, verbose: bool = False
) -> dict[CookieKey, dict]:
    """Use one of browser_cookie3's named extractors (firefox, safari, etc.).
    Returns the same shape as `_extract_notion_cookies_from`.
    """
    try:
        import browser_cookie3  # type: ignore
    except Exception:
        return {}
    fn = getattr(browser_cookie3, name, None)
    if fn is None:
        _vlog(verbose, f"{name}: extractor not in browser_cookie3")
        return {}
    try:
        cj = fn(domain_name="notion")
    except Exception as e:
        _vlog(verbose, f"{name}: extraction failed: {e}")
        return {}
    out: dict[CookieKey, dict] = {}
    for c in cj:
        if "notion" not in c.domain:
            continue
        out[(c.name, c.domain, c.path)] = {
            "name": c.name,
            "value": c.value or "",
            "domain": c.domain,
            "path": c.path,
            "httponly": getattr(c, "_rest", {}).get("HttpOnly", False),
        }
    if verbose:
        tok = next((v for k, v in out.items() if k[0] == "token_v2"), None)
        if tok:
            _vlog(verbose, f"{name}: token_v2 present (len={len(tok['value'])})")
        else:
            _vlog(verbose, f"{name}: no token_v2 in cookie jar")
    return out


def all_browser_sessions(
    *, verbose: bool = False
) -> list[tuple[str, dict[CookieKey, dict]]]:
    """Enumerate every browser/profile that currently has a Notion `token_v2`
    cookie. Returns [(label, cookies)] where label is human-readable
    ("chrome:Default", "firefox", ...) and cookies is the full notion.so /
    notion.com cookie set keyed by (name, domain, path).
    """
    out: list[tuple[str, dict[CookieKey, dict]]] = []
    for cf in _all_chrome_cookie_files():
        cookies = _extract_notion_cookies_from(cf, verbose=verbose)
        if any(c["name"] == "token_v2" for c in cookies.values()):
            out.append((_short_chrome_label(cf), cookies))
    for name in ("firefox", "safari"):
        cookies = _extract_notion_cookies_via(name, verbose=verbose)
        if any(c["name"] == "token_v2" for c in cookies.values()):
            out.append((name, cookies))
    return out


def _short_chrome_label(cookie_file: str) -> str:
    """Turn a long Cookies path into something like 'chrome:Profile 2'."""
    parts = Path(cookie_file).parts
    profile = "?"
    family = "chrome"
    for i, p in enumerate(parts):
        if p == "Application Support" and i + 1 < len(parts):
            family = parts[i + 1].lower().replace(" ", "-")
        if p in ("Cookies",) and i >= 2:
            profile = parts[i - 2] if parts[i - 1] == "Network" else parts[i - 1]
    return f"{family}:{profile}"


def load_browser_cookies() -> dict[CookieKey, dict]:
    """Return the cookie set of the first browser session that has a Notion
    `token_v2`. Used by the download path to talk to file.notion.so. For
    'this is a working session' validation, prefer `find_working_session()`.

    Returns {} if no session is found.
    """
    sessions = all_browser_sessions()
    if not sessions:
        return {}
    sessions.sort(key=lambda s: sum(
        1 for c in s[1].values() if c["name"] in ("token_v2", "file_token", "p_sync_session")
    ), reverse=True)
    return sessions[0][1]


def _api_host() -> str:
    from urllib.parse import urlparse
    return urlparse(API_BASE).hostname or "www.notion.so"


def _cookie_domain_matches_host(cookie_domain: str, host: str) -> bool:
    """Standard browser cookie domain matching: a cookie on `.foo.com`
    matches host `bar.foo.com`, but a cookie on `.app.foo.com` does NOT
    match `www.foo.com`.
    """
    d = (cookie_domain or "").lstrip(".")
    if not d:
        return False
    return host == d or host.endswith("." + d)


def find_all_working_sessions(
    *, verbose: bool = False
) -> list[dict]:
    """Iterate every detected browser session, find one valid token_v2 per
    session, and dedupe by Notion user_id. Returns a list of dicts:

        [
          {
            "label": "google:Profile 2",
            "cookies": {(name,domain,path): {...}, ...},
            "token": "<v03:eyJ...>",
            "uc": <loadUserContent payload>,
            "user_id": "33dd...",
            "user_email": "lyon@trychroma.com",
            "spaces": {space_id: {...}, ...},
          },
          ...
        ]

    Notion sets a token_v2 cookie on multiple domains (e.g. .www.notion.so
    AND .app.notion.com). Only the one whose domain matches the API host
    will validate, so we try those first.
    """
    api_host = _api_host()
    seen_users: set[str] = set()
    out: list[dict] = []
    for label, cookies in all_browser_sessions(verbose=verbose):
        candidates: list[tuple[CookieKey, str]] = [
            (k, c["value"])
            for k, c in cookies.items()
            if c["name"] == "token_v2" and c["value"]
        ]
        if not candidates:
            continue
        candidates.sort(
            key=lambda kv: not _cookie_domain_matches_host(kv[0][1], api_host)
        )
        for key, token in candidates:
            _vlog(
                verbose,
                f"{label}: trying token_v2 from domain={key[1]} path={key[2]} "
                f"(len={len(token)})",
            )
            try:
                uc = NotionInternal(token).load_user_content()
            except Exception as e:
                _vlog(verbose, f"{label}: validation FAILED ({key[1]}): {e}")
                continue
            _vlog(verbose, f"{label}: validation OK ({key[1]})")
            me, spaces = _summarize_uc(uc)
            user_id = me.get("id") or ""
            if user_id and user_id in seen_users:
                _vlog(
                    verbose,
                    f"{label}: same identity ({me.get('email','?')}) already seen; skipping",
                )
                break
            if user_id:
                seen_users.add(user_id)
            out.append(
                {
                    "label": label,
                    "cookies": cookies,
                    "token": token,
                    "uc": uc,
                    "user_id": user_id,
                    "user_email": me.get("email", "?"),
                    "spaces": spaces,
                }
            )
            break
    return out


def find_working_session(
    *, verbose: bool = False
) -> Optional[tuple[str, dict[CookieKey, dict], dict]]:
    """Backward-compat single-session shim around find_all_working_sessions.
    Returns the first working session (label, cookies, uc), or None if
    nothing works. Doesn't prompt.
    """
    sessions = find_all_working_sessions(verbose=verbose)
    if not sessions:
        return None
    s = sessions[0]
    return (s["label"], s["cookies"], s["uc"])


def cookie_header_for(cookies: dict[CookieKey, dict], target_url: str) -> str:
    """Build a Cookie header that the browser would send to `target_url`,
    respecting domain + path matching.
    """
    from urllib.parse import urlparse
    p = urlparse(target_url)
    host = p.hostname or ""
    path = p.path or "/"
    parts: list[str] = []
    seen_names: set[str] = set()
    for c in cookies.values():
        d = c["domain"].lstrip(".")
        if not (host == d or host.endswith("." + d)):
            continue
        cpath = c.get("path") or "/"
        if not path.startswith(cpath):
            continue
        if c["name"] in seen_names:
            continue
        seen_names.add(c["name"])
        parts.append(f'{c["name"]}={c["value"]}')
    return "; ".join(parts)


def load_token_v2(arg: Optional[str]) -> Optional[str]:
    if arg:
        return arg.strip()
    env = os.environ.get("NOTION_TOKEN_V2", "").strip()
    if env:
        return env
    p = _default_token_path()
    if p.exists():
        for raw in p.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if line and not line.startswith("#"):
                return line
    cookies = load_browser_cookies()
    for c in cookies.values():
        if c["name"] == "token_v2":
            return c["value"]
    return None


# ---------------------------------------------------------------------------
# Rate limit
# ---------------------------------------------------------------------------


class TokenBucket:
    def __init__(self, rate: float, capacity: Optional[float] = None) -> None:
        self.rate = float(rate)
        self.capacity = float(capacity if capacity is not None else max(rate, 1.0))
        self.tokens = self.capacity
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def take(self, n: float = 1.0) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= n:
                    self.tokens -= n
                    return
                wait = (n - self.tokens) / self.rate
            time.sleep(max(wait, 0.001))


class RateLimitGate:
    """Shared cooldown across worker threads. When any worker observes a
    429, it calls .trip(retry_after) to push out a global "don't send any
    more requests until X" deadline. Every request first calls .wait(),
    which blocks until the deadline (if any) has passed.

    This kills the thundering herd: with N workers, if one hits a 429 and
    sets a 30s cooldown, the other N-1 won't immediately fire their own
    requests and incur their own 429s.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cool_until = 0.0
        self.trips = 0
        self.total_wait_s = 0.0

    def wait(self) -> None:
        with self._lock:
            wait_s = max(0.0, self._cool_until - time.time())
        if wait_s > 0:
            time.sleep(wait_s)
            with self._lock:
                self.total_wait_s += wait_s

    def trip(self, retry_after_s: float) -> None:
        with self._lock:
            new_until = time.time() + max(0.0, retry_after_s)
            if new_until > self._cool_until:
                self._cool_until = new_until
                self.trips += 1


class RateLimitedError(RuntimeError):
    """Raised by NotionInternal._post when the server returns 429.
    `retry_after` is the cooldown in seconds (parsed from Retry-After
    header, or DEFAULT_BACKOFF_INITIAL_S if absent). The gate has already
    been tripped by the time this is raised.
    """

    def __init__(self, message: str, *, retry_after: float) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def _parse_retry_after(headers: Any, default_s: float) -> float:
    raw = headers.get("Retry-After") if headers else None
    if not raw:
        return default_s
    try:
        return max(0.0, float(raw))
    except ValueError:
        # HTTP also allows an HTTP-date here; not worth parsing for our
        # purposes -- just fall back.
        return default_s


# ---------------------------------------------------------------------------
# Shared task poller
# ---------------------------------------------------------------------------


class TaskPool:
    """One poller thread shared across all worker threads. Periodically
    calls client.get_tasks([all in-flight task ids]) in a single batched
    request, then signals the corresponding worker via threading.Event.

    Replaces N independent polling loops with one, dropping polling RPS
    from N/poll_interval to 1/poll_interval regardless of worker
    parallelism. That largely eliminates getTasks 429s, because Notion's
    per-user rate limit on task-related calls is the dominant constraint.
    """

    def __init__(
        self,
        client: "NotionInternal",
        *,
        poll_interval: float,
        poll_bucket: TokenBucket,
    ) -> None:
        self.client = client
        self.poll_interval = float(poll_interval)
        self.poll_bucket = poll_bucket
        self._lock = threading.Lock()
        self._pending: dict[str, threading.Event] = {}
        self._results: dict[str, dict] = {}
        self._shutdown = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.poll_count = 0
        self.batched_count = 0  # sum of per-call task-id list sizes

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._poll_loop,
            name="notion-task-poller",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._shutdown.set()
        if self._thread is not None:
            self._thread.join(timeout=max(self.poll_interval * 2, 5.0))

    def register(self, task_id: str) -> threading.Event:
        ev = threading.Event()
        with self._lock:
            self._pending[task_id] = ev
        return ev

    def status(self, task_id: str) -> Optional[dict]:
        with self._lock:
            return self._results.get(task_id)

    def _poll_loop(self) -> None:
        backoff = self.poll_interval
        while not self._shutdown.is_set():
            if self._shutdown.wait(backoff):
                return
            with self._lock:
                pending = list(self._pending.keys())
            if not pending:
                backoff = self.poll_interval
                continue
            self.poll_bucket.take()
            try:
                resp = self.client.get_tasks(pending)
            except RateLimitedError as e:
                # gate.trip() already happened in _post; sleep our own
                # backoff so we don't fire again the instant the gate
                # cooldown expires (which would be a thundering herd of
                # one).
                backoff = min(MAX_BACKOFF_S, max(backoff * 2, e.retry_after))
                continue
            except Exception:
                backoff = self.poll_interval
                continue
            self.poll_count += 1
            self.batched_count += len(pending)
            backoff = self.poll_interval
            with self._lock:
                for t in resp.get("results") or []:
                    tid = t.get("id")
                    if not tid or tid not in self._pending:
                        continue
                    state = t.get("state") or "in_progress"
                    self._results[tid] = t
                    if state in ("success", "failure"):
                        ev = self._pending.pop(tid, None)
                        if ev is not None:
                            ev.set()


# ---------------------------------------------------------------------------
# /api/v3 client
# ---------------------------------------------------------------------------


class NotionInternal:
    def __init__(
        self,
        token_v2: str,
        *,
        rps: float = DEFAULT_RPS,
        gate: Optional[RateLimitGate] = None,
    ) -> None:
        self.token_v2 = token_v2
        self.bucket = TokenBucket(rps)
        self.gate = gate or RateLimitGate()

    def _post(
        self,
        path: str,
        body: Optional[dict] = None,
        *,
        space_id: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT,
        rate_limit: bool = True,
    ) -> dict:
        if rate_limit:
            self.gate.wait()
            self.bucket.take()
        url = f"{API_BASE}/{path.lstrip('/')}"
        data = json.dumps(body or {}).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
            "Cookie": f"token_v2={self.token_v2}",
            "Accept": "application/json",
            "Notion-Client-Version": "23.13.0.4444",
        }
        if space_id:
            headers["x-notion-space-id"] = space_id
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8") or "{}")
        except urllib.error.HTTPError as e:
            body_text = ""
            try:
                body_text = e.read().decode("utf-8", errors="replace")
            except Exception:
                pass
            if e.code == 429:
                retry_after = _parse_retry_after(e.headers, DEFAULT_BACKOFF_INITIAL_S)
                self.gate.trip(retry_after)
                raise RateLimitedError(
                    f"HTTP 429 on {path} (retry_after={retry_after:.1f}s): "
                    f"{body_text[:200]}",
                    retry_after=retry_after,
                ) from e
            raise RuntimeError(
                f"HTTP {e.code} on {path}: {body_text[:500]}"
            ) from e

    # ------- discovery helpers -------

    def load_user_content(self) -> dict:
        return self._post("loadUserContent", {})

    def get_spaces(self) -> dict:
        return self._post("getSpaces", {})

    def sync_record_values(self, requests: list[dict]) -> dict:
        return self._post("syncRecordValues", {"requests": requests})

    def get_record_values(self, requests: list[dict]) -> dict:
        return self._post("getRecordValues", {"requests": requests})

    def load_page_chunk(
        self,
        page_id: str,
        *,
        chunk_number: int = 0,
        limit: int = 100,
        cursor: Optional[dict] = None,
    ) -> dict:
        body = {
            "pageId": page_id,
            "limit": limit,
            "cursor": cursor or {"stack": []},
            "chunkNumber": chunk_number,
            "verticalColumns": False,
        }
        return self._post("loadPageChunk", body)

    def search(
        self,
        space_id: str,
        *,
        query: str = "",
        limit: int = 100,
        variant: str = "minimal",
    ) -> dict:
        """Try multiple known body shapes; first one that returns 200 wins."""
        variants = []
        if variant in ("minimal", "auto"):
            variants.append({
                "type": "BlocksInSpace",
                "query": query,
                "spaceId": space_id,
                "limit": limit,
                "filters": {
                    "isDeletedOnly": False,
                    "excludeTemplates": True,
                    "isNavigableOnly": True,
                    "requireEditPermissions": False,
                    "ancestors": [],
                    "createdBy": [],
                    "editedBy": [],
                    "lastEditedTime": {},
                    "createdTime": {},
                    "inTeams": [],
                    "navigableBlockContentOnly": False,
                },
                "sort": {"field": "relevance"},
                "source": "quick_find",
            })
        if variant in ("legacy", "auto"):
            variants.append({
                "type": "BlocksInSpace",
                "query": query,
                "filters": {
                    "isDeletedOnly": False,
                    "isNavigableOnly": True,
                    "excludeTemplates": True,
                    "requireEditPermissions": False,
                    "ancestors": [],
                    "createdBy": [],
                    "editedBy": [],
                    "lastEditedTime": {},
                    "createdTime": {},
                },
                "sort": "Relevance",
                "limit": limit,
                "spaceId": space_id,
                "source": "quick_find",
            })
        last_err: Optional[Exception] = None
        for body in variants:
            try:
                return self._post("search", body, space_id=space_id)
            except Exception as e:
                last_err = e
                continue
        raise last_err or RuntimeError("search: no variants succeeded")

    # ------- export task helpers -------

    def enqueue_export_block(
        self,
        block_id: str,
        space_id: str,
        *,
        export_type: str = "markdown",
        recursive: bool = True,
        include_files: str = "everything",
    ) -> str:
        body = {
            "task": {
                "eventName": "exportBlock",
                "request": {
                    "block": {"id": block_id, "spaceId": space_id},
                    "recursive": recursive,
                    "exportOptions": {
                        "exportType": export_type,
                        "timeZone": "America/Los_Angeles",
                        "locale": "en",
                        "collectionViewExportType": "currentView",
                        "flattenExportFiletree": False,
                        "includeContents": include_files,
                    },
                },
            }
        }
        resp = self._post("enqueueTask", body, space_id=space_id)
        task_id = resp.get("taskId") or resp.get("task_id") or ""
        if not task_id:
            raise RuntimeError(f"enqueueTask returned no taskId: {resp}")
        return task_id

    def get_tasks(self, task_ids: list[str]) -> dict:
        return self._post("getTasks", {"taskIds": task_ids})


# ---------------------------------------------------------------------------
# Helpers to dig records out of recordMap blobs
# ---------------------------------------------------------------------------


def _walk_record_map(record_map: dict, kind: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    table = record_map.get(kind)
    if not isinstance(table, dict):
        return out
    for rid, wrapper in table.items():
        if not isinstance(wrapper, dict):
            continue
        v = wrapper.get("value")
        while isinstance(v, dict) and "value" in v and isinstance(v.get("value"), dict):
            v = v["value"]
        if isinstance(v, dict) and v.get("id"):
            out[rid] = v
    return out


def _block_title(block: dict) -> str:
    props = (block or {}).get("properties") or {}
    title = props.get("title") or []
    chunks: list[str] = []
    for piece in title:
        if isinstance(piece, list) and piece and isinstance(piece[0], str):
            chunks.append(piece[0])
    return ("".join(chunks)).strip() or "(untitled)"


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def _resolve_space(client: NotionInternal, hint: Optional[str]) -> tuple[str, str]:
    """Return (space_id, space_name). Prompts if multiple and no hint."""
    user_content = client.load_user_content()
    record_map = (user_content.get("recordMap") or {})
    spaces = _walk_record_map(record_map, "space")
    if not spaces:
        raise RuntimeError("loadUserContent returned no spaces; bad cookie?")
    if hint:
        for sid, s in spaces.items():
            if sid == hint or s.get("name") == hint:
                return sid, s.get("name") or "?"
        raise RuntimeError(f"no space matched hint {hint!r}; available: "
                           + ", ".join(f"{s.get('name','?')}={sid}" for sid, s in spaces.items()))
    if len(spaces) == 1:
        sid, s = next(iter(spaces.items()))
        return sid, s.get("name") or "?"
    print("multiple spaces found; pick one:", file=sys.stderr)
    items = list(spaces.items())
    for i, (sid, s) in enumerate(items, 1):
        print(f"  [{i}] {s.get('name','?')}  ({sid})", file=sys.stderr)
    while True:
        choice = input("choice: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(items):
            sid, s = items[int(choice) - 1]
            return sid, s.get("name") or "?"


NOTION_LOGIN_URL = "https://www.notion.so/login"


def _summarize_uc(uc: dict) -> tuple[dict, dict[str, dict]]:
    rmap = uc.get("recordMap") or {}
    spaces = _walk_record_map(rmap, "space")
    users = _walk_record_map(rmap, "notion_user")
    me = next(iter(users.values()), {}) if users else {}
    return me, spaces


def _print_session_summary(source: str, uc: dict) -> None:
    me, spaces = _summarize_uc(uc)
    print()
    print(f"  signed in as: {me.get('email','?')} ({me.get('id','?')})")
    print(f"  cookie source: {source}")
    print(f"  workspaces:    {len(spaces)}")
    for sid, s in spaces.items():
        name = s.get("name") or "(untitled)"
        print(f"    - {name!r:30s}  {sid}")


def _save_token(token: str, source_label: str) -> Path:
    path = _default_token_path()
    path.write_text(token + "\n", encoding="utf-8")
    print(f"  saved token_v2 -> {path}  (from {source_label})")
    return path


def _maybe_pick_and_save_space(
    uc: dict,
    *,
    no_pick: bool,
    pinned: Optional[str] = None,
) -> Optional[str]:
    _, spaces = _summarize_uc(uc)
    if not spaces:
        return None
    if pinned:
        if pinned in spaces:
            _persist_env_var("NOTION_INTERNAL_SPACE_ID", pinned)
            print(
                f"  saved NOTION_INTERNAL_SPACE_ID={pinned} "
                f"({spaces[pinned].get('name','?')}) -> {_env_path()}"
            )
            return pinned
        print(f"  warning: --space-id {pinned} not in your accessible spaces")
        return None
    if no_pick:
        return None
    if len(spaces) == 1:
        sid = next(iter(spaces.keys()))
        _persist_env_var("NOTION_INTERNAL_SPACE_ID", sid)
        print(
            f"  saved NOTION_INTERNAL_SPACE_ID={sid} "
            f"({spaces[sid].get('name','?')}) -> {_env_path()}"
        )
        return sid
    print()
    print("Multiple workspaces available. Which one will the dump target?")
    items = list(spaces.items())
    for i, (sid, s) in enumerate(items, 1):
        print(f"  [{i}] {s.get('name','(untitled)')}  ({sid})")
    while True:
        ans = input(f"choice (1-{len(items)}, or empty to skip): ").strip()
        if not ans:
            print("  skipped; you'll be prompted at dump time")
            return None
        if ans.isdigit() and 1 <= int(ans) <= len(items):
            sid, s = items[int(ans) - 1]
            _persist_env_var("NOTION_INTERNAL_SPACE_ID", sid)
            print(
                f"  saved NOTION_INTERNAL_SPACE_ID={sid} "
                f"({s.get('name','?')}) -> {_env_path()}"
            )
            return sid
        print("  invalid choice, try again")


def _accept_session(
    session: dict, args: argparse.Namespace, pinned: Optional[str]
) -> int:
    """Save the session's token, print summary, persist the space pin."""
    _save_token(session["token"], session["label"])
    _print_session_summary(session["label"], session["uc"])
    _maybe_pick_and_save_space(session["uc"], no_pick=args.no_pick, pinned=pinned)
    print()
    print("Done. Run `./notion_internal_dump.sh dump` (or grab) next.")
    return 0


def _pick_session_and_workspace(
    sessions: list[dict], args: argparse.Namespace, pinned: Optional[str]
) -> int:
    """Present a flattened (workspace, session) chooser when multiple Notion
    identities are signed in across the user's browsers. Saves the chosen
    session's token and pins the chosen workspace.
    """
    if len(sessions) == 1:
        return _accept_session(sessions[0], args, pinned)

    print()
    print(f"Found {len(sessions)} active Notion sessions across your browsers:")
    for s in sessions:
        nspaces = len(s["spaces"])
        print(
            f"  - {s['user_email']:30s}  ({s['label']})  ->  "
            f"{nspaces} workspace(s)"
        )

    if pinned:
        for s in sessions:
            if pinned in s["spaces"]:
                name = s["spaces"][pinned].get("name", "?")
                print()
                print(
                    f"  Pinned NOTION_INTERNAL_SPACE_ID={pinned} ({name}) "
                    f"matched session {s['user_email']}; using it."
                )
                return _accept_session(s, args, pinned)
        print(
            f"  warning: pinned NOTION_INTERNAL_SPACE_ID={pinned} not found in "
            f"any of these sessions"
        )

    if args.no_pick or not sys.stdin.isatty():
        s = sessions[0]
        print(f"  --no-pick: defaulting to {s['user_email']} ({s['label']})")
        return _accept_session(s, args, pinned)

    flat: list[tuple[dict, str, str]] = []
    for s in sessions:
        for sid, sp in s["spaces"].items():
            flat.append((s, sid, sp.get("name") or "(untitled)"))
    name_w = max(len(name) for _, _, name in flat)
    print()
    print("Pick the workspace to dump (token + space will be saved together):")
    for i, (s, sid, name) in enumerate(flat, 1):
        print(
            f"  [{i:2d}] {name:<{name_w}}  ({s['user_email']}, {s['label']})"
        )
    while True:
        try:
            ans = input(f"choice (1-{len(flat)}): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\naborted.")
            return 130
        if ans.isdigit() and 1 <= int(ans) <= len(flat):
            chosen, chosen_sid, _name = flat[int(ans) - 1]
            return _accept_session(chosen, args, chosen_sid)
        print("  invalid choice, try again")


def cmd_login_paste(args: argparse.Namespace) -> int:
    """Manual fallback: prompt the user to paste a token_v2 from DevTools,
    validate it, save it. Use this when the cookie-store auto-detection
    can't find a working session (e.g. Chrome holds the live token in
    memory and the on-disk SQLite version is stale).
    """
    pinned = (args.space_id or os.environ.get("NOTION_INTERNAL_SPACE_ID") or "").strip() or None
    print()
    print("=== Manual token paste ===")
    print()
    print("The auto-detect couldn't find a working Notion session. Let's paste")
    print("your live token_v2 from DevTools instead. This always works.")
    print()
    print("Steps:")
    print("  1. Open https://www.notion.so in Chrome (any tab where you're")
    print("     signed in).")
    print("  2. Open DevTools: Cmd-Opt-I (or right-click -> Inspect).")
    print("  3. Application tab -> Storage -> Cookies -> https://www.notion.so")
    print("  4. Find the row whose Name is exactly: token_v2")
    print("  5. Double-click the Value column and Cmd-C to copy it.")
    print()
    if not args.no_browser:
        try:
            webbrowser.open("https://www.notion.so")
        except Exception:
            pass
    while True:
        try:
            token = input("Paste token_v2 here (or empty to abort): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\naborted.")
            return 130
        if not token:
            print("aborted.")
            return 130
        if token.startswith('"') and token.endswith('"'):
            token = token[1:-1]
        if not token.startswith("v0") and "%3A" not in token and ":" not in token:
            print("  doesn't look like a token_v2 (should start with v02:/v03:");
            print("  if you copied a different cookie value, try again.")
            continue
        print("  validating against /api/v3/loadUserContent...")
        try:
            uc = NotionInternal(token).load_user_content()
        except Exception as e:
            print(f"  validation FAILED: {e}")
            print("  try again, or Ctrl-C to abort.")
            continue
        _save_token(token, "manual paste")
        _print_session_summary("manual paste", uc)
        _maybe_pick_and_save_space(uc, no_pick=args.no_pick, pinned=pinned)
        print()
        print("Done. Run `./notion_internal_dump.sh dump` (or grab) next.")
        return 0


def _print_login_intro() -> None:
    print()
    print("=== Notion login ===")
    print()
    print("To dump your Notion data we use the same /api/v3 endpoints the web")
    print("app uses, authenticated by your browser session cookie. That cookie")
    print("(`token_v2`) is HttpOnly so we have to read it from your browser's")
    print("cookie store rather than via JavaScript.")
    print()
    print("Here's what's about to happen:")
    print(f"  1. I'll open {NOTION_LOGIN_URL} in your default browser.")
    print("  2. You sign in (or just confirm you're already signed in).")
    print("  3. I'll detect your session from your browser's cookies and")
    print("     validate it against /api/v3/loadUserContent.")
    print("  4. If you have access to multiple workspaces, I'll ask which one.")
    print()
    print("Supported browsers (whichever Notion is signed in to):")
    print("  Chrome, Chrome Beta/Canary, Arc, Brave, Edge, Vivaldi, Firefox,")
    print("  Safari. macOS may pop a Touch ID / Keychain prompt the first time")
    print("  we read the cookie file -- that's normal.")
    print()


def _snapshot_cookie_mtimes() -> dict[str, float]:
    """Return {cookie_file: mtime} for every Chrome-like cookie file we know
    about. mtime is wall time seconds.
    """
    out: dict[str, float] = {}
    for cf in _all_chrome_cookie_files():
        try:
            out[cf] = os.path.getmtime(cf)
        except OSError:
            pass
    return out


def _wait_for_input_or_timeout(seconds: float) -> Optional[str]:
    """Sleep up to `seconds`, but return early if the user presses Enter.
    Returns the trimmed line (which may be empty) if the user pressed
    Enter, or None if the timeout fired. Falls back to plain time.sleep
    if stdin isn't a tty (e.g. piped).
    """
    try:
        if not sys.stdin.isatty():
            time.sleep(seconds)
            return None
        ready, _, _ = select.select([sys.stdin], [], [], seconds)
    except (ValueError, OSError):
        time.sleep(seconds)
        return None
    if ready:
        try:
            return sys.stdin.readline().strip()
        except Exception:
            return ""
    return None


def cmd_login(args: argparse.Namespace) -> int:
    """Smart guided login. Tries existing creds, then existing browser
    sessions, then walks the user through opening Notion and signing in.
    """
    pinned = (args.space_id or os.environ.get("NOTION_INTERNAL_SPACE_ID") or "").strip() or None
    verbose = bool(getattr(args, "verbose", False))

    if not args.force:
        existing = load_token_v2(args.token_v2)
        if existing:
            try:
                uc = NotionInternal(existing).load_user_content()
                _print_session_summary("saved/env token", uc)
                _maybe_pick_and_save_space(uc, no_pick=args.no_pick, pinned=pinned)
                print()
                print("Already authenticated. Use --force to re-run the login flow.")
                return 0
            except Exception as e:
                print(f"  saved/env token didn't validate ({e}); checking browser...")

    print("Looking for active Notion sessions in your browsers...")
    sessions = find_all_working_sessions(verbose=verbose)
    if sessions:
        return _pick_session_and_workspace(sessions, args, pinned)

    _print_login_intro()
    if args.no_browser:
        print(f"Open this in your browser, then sign in:\n  {NOTION_LOGIN_URL}")
    else:
        try:
            webbrowser.open(NOTION_LOGIN_URL)
            print(f"Opened {NOTION_LOGIN_URL} in your default browser.")
        except Exception:
            print(
                f"Couldn't auto-open the browser. Open this URL manually, then "
                f"sign in:\n  {NOTION_LOGIN_URL}"
            )
    print()
    print(
        f"Waiting for sign-in (timeout {args.timeout}s, polling every "
        f"{args.poll_interval:g}s)."
    )
    print("  Press Enter to re-check immediately.")
    print("  Type 'paste' + Enter to switch to manual token paste.")
    print("  Ctrl-C to abort.")

    mtime_cache = _snapshot_cookie_mtimes()
    if mtime_cache:
        print()
        print("  Watching browser cookie files for writes:")
        for cf, mt in mtime_cache.items():
            when = datetime.fromtimestamp(mt).strftime("%H:%M:%S")
            print(f"    - {_short_chrome_label(cf):24s}  last mtime {when}")
    print()

    deadline = time.time() + args.timeout
    started = time.time()
    last_status = started
    cookie_writes_seen = 0
    nudged_close_tab = False

    while time.time() < deadline:
        # Only re-extract cookies (which can trigger a Keychain prompt) when
        # something on disk actually changed -- or when the user pressed
        # Enter to force a re-check.
        recheck_reason: Optional[str] = None
        for cf in _all_chrome_cookie_files():
            try:
                mt = os.path.getmtime(cf)
            except OSError:
                continue
            prev = mtime_cache.get(cf)
            if prev is None:
                mtime_cache[cf] = mt
                when = datetime.fromtimestamp(mt).strftime("%H:%M:%S")
                print(f"  [discovered] {_short_chrome_label(cf)} cookie file (mtime {when})")
                recheck_reason = recheck_reason or "new cookie file"
            elif mt != prev:
                when = datetime.fromtimestamp(mt).strftime("%H:%M:%S")
                print(f"  [chrome flushed cookies] {_short_chrome_label(cf)} at {when}")
                mtime_cache[cf] = mt
                cookie_writes_seen += 1
                recheck_reason = recheck_reason or "cookie file flushed"

        if recheck_reason:
            print(f"  rechecking ({recheck_reason})...")
            sessions = find_all_working_sessions(verbose=verbose)
            if sessions:
                print()
                return _pick_session_and_workspace(sessions, args, pinned)
            print("  no working session yet; will keep watching.")

        now = time.time()
        elapsed = int(now - started)
        if now - last_status >= 15:
            remaining = int(deadline - now)
            print(
                f"  ...still waiting ({elapsed}s elapsed, {remaining}s remaining, "
                f"chrome flushes seen: {cookie_writes_seen})"
            )
            last_status = now

        # If we haven't seen Chrome flush any cookies after 60s, the user is
        # almost certainly signed in but Chrome is buffering. Nudge them.
        if not nudged_close_tab and elapsed >= 60 and cookie_writes_seen == 0:
            print()
            print("  Heads up: no cookie file writes detected yet. Chrome buffers")
            print("  cookie writes, sometimes for minutes. To force a flush:")
            print("    - Close the Notion tab you signed into, OR")
            print("    - Quit Chrome (Cmd-Q) and reopen it (cookies flush on quit).")
            print("  Then press Enter here to re-check immediately.")
            print()
            nudged_close_tab = True

        try:
            line = _wait_for_input_or_timeout(args.poll_interval)
        except KeyboardInterrupt:
            print("\naborted.")
            return 130
        if line is not None:
            if line.lower() in ("paste", "p"):
                return cmd_login_paste(args)
            print("  forcing immediate re-check (you pressed Enter)...")
            sessions = find_all_working_sessions(verbose=verbose)
            if sessions:
                print()
                return _pick_session_and_workspace(sessions, args, pinned)
            print("  still no working session.")

    print()
    print("Timed out waiting for sign-in.")
    print("Workarounds:")
    print( "  1. Force Chrome to flush its cookies: close the Notion tab or")
    print( "     quit Chrome (Cmd-Q), then re-run `login --force`.")
    print(f"  2. Re-run with a longer timeout, e.g. --timeout 600.")
    print( "  3. Run with --verbose to see per-poll extraction details.")
    print( "  4. Manually copy `token_v2` from DevTools (Application -> Cookies)")
    print(f"     and paste it into {_default_token_path()}.")
    return 4


def _search_all(
    client: NotionInternal,
    space_id: str,
    *,
    page_size: int = 1000,
    max_pages: int = 100000,
    progress: bool = True,
) -> tuple[dict[str, dict], dict[str, dict]]:
    """Paginate /api/v3/search until exhausted or max_pages hit.

    Returns (blocks_by_id, teams_by_id). Each value is the inner record dict
    (already unwrapped from its wrapper). De-duplicates across pages.
    """
    blocks: dict[str, dict] = {}
    teams: dict[str, dict] = {}
    spaces: dict[str, dict] = {}

    seen_ids: set[str] = set()
    total_seen = 0
    last_batch_new = page_size
    batch = 0
    while last_batch_new > 0 and total_seen < max_pages:
        batch += 1
        resp = client.search(space_id, query="", limit=page_size, variant="minimal")
        results = resp.get("results") or []
        if not isinstance(results, list):
            break
        rmap = resp.get("recordMap") or {}
        blocks.update(_walk_record_map(rmap, "block"))
        teams.update(_walk_record_map(rmap, "team"))
        spaces.update(_walk_record_map(rmap, "space"))
        new_count = 0
        for r in results:
            bid = r.get("id")
            if bid and bid not in seen_ids:
                seen_ids.add(bid)
                new_count += 1
        last_batch_new = new_count
        total_seen += new_count
        if progress:
            print(
                f"  [search batch {batch}] returned={len(results)} new={new_count} "
                f"unique_pages={total_seen} blocks_in_map={len(blocks)} "
                f"teams_in_map={len(teams)}"
            )
        if len(results) < page_size:
            break
    return blocks, teams


def _walk_sidebar_top_level(client: NotionInternal, space_id: str) -> list[dict]:
    """Top-level container pages = pages with parent_table in {space, team}.

    We get them by paginating /api/v3/search across the space and filtering
    the recordMap.block table. This is the only path that surfaces teamspace
    pages reliably; getSpaces / syncRecordValues only return the user's
    Private pages and the bare space metadata.
    """
    blocks, teams = _search_all(client, space_id, progress=True)
    out: list[dict] = []
    for bid, b in blocks.items():
        if b.get("parent_table") == "space" and b.get("parent_id") == space_id:
            out.append({
                "id": bid,
                "title": _block_title(b),
                "kind": "space_page",
                "type": b.get("type"),
                "last_edited_time": b.get("last_edited_time"),
            })
        elif b.get("parent_table") == "team":
            tid = b.get("parent_id") or ""
            t = teams.get(tid) or {}
            if t and t.get("space_id") and t["space_id"] != space_id:
                continue
            out.append({
                "id": bid,
                "title": _block_title(b),
                "kind": "teamspace_page",
                "type": b.get("type"),
                "teamspace_id": tid,
                "teamspace_name": t.get("name"),
                "last_edited_time": b.get("last_edited_time"),
            })
    out.sort(key=lambda r: (r.get("kind") or "", r.get("teamspace_name") or "", r.get("title") or ""))
    return out


def cmd_discover(args: argparse.Namespace) -> int:
    token = load_token_v2(args.token_v2)
    if not token:
        print(
            "error: no token_v2; run "
            "`./notion_internal_dump.sh login` first",
            file=sys.stderr,
        )
        return 2
    client = NotionInternal(token, rps=args.rps)
    space_id, space_name = _resolve_space(
        client, args.space_id or os.environ.get("NOTION_INTERNAL_SPACE_ID")
    )
    print(f"space:  {space_name!r} ({space_id})")

    print(f"--- /api/v3/search (page_size {args.page_size}, max_pages {args.max_pages}) ---")
    t0 = time.time()
    blocks, teams = _search_all(
        client, space_id, page_size=args.page_size, max_pages=args.max_pages
    )
    dt = time.time() - t0
    print(f"blocks discovered: {len(blocks)}")
    print(f"teams discovered:  {len(teams)}")
    print(f"discovery wall:    {dt:.1f}s")

    sidebar: list[dict] = []
    for bid, b in blocks.items():
        pt = b.get("parent_table")
        pid = b.get("parent_id")
        if pt == "space" and pid == space_id:
            sidebar.append({
                "id": bid,
                "title": _block_title(b),
                "kind": "space_page",
                "type": b.get("type"),
                "last_edited_time": b.get("last_edited_time"),
            })
        elif pt == "team":
            t = teams.get(pid or "") or {}
            if t.get("space_id") and t["space_id"] != space_id:
                continue
            sidebar.append({
                "id": bid,
                "title": _block_title(b),
                "kind": "teamspace_page",
                "type": b.get("type"),
                "teamspace_id": pid,
                "teamspace_name": t.get("name"),
                "last_edited_time": b.get("last_edited_time"),
            })
    sidebar.sort(key=lambda r: (r.get("kind") or "", r.get("teamspace_name") or "", r.get("title") or ""))

    n_private = sum(1 for s in sidebar if s.get("kind") == "space_page")
    n_team = sum(1 for s in sidebar if s.get("kind") == "teamspace_page")
    teamspace_names = sorted({s["teamspace_name"] for s in sidebar if s.get("teamspace_name")})
    print()
    print(f"sidebar (top-level containers): {len(sidebar)}  "
          f"(space_page={n_private}  teamspace_page={n_team})")
    if teamspace_names:
        print(f"teamspaces represented: {', '.join(teamspace_names)}")
    for s in sidebar:
        ts = f" [{s['teamspace_name']}]" if s.get("teamspace_name") else ""
        print(f"  {s.get('kind','?'):14s}  {s['id']}  {s['title']!r}{ts}")

    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    sidebar_path = output / "sidebar.jsonl"
    with sidebar_path.open("w", encoding="utf-8") as f:
        for s in sidebar:
            s2 = dict(s)
            s2["space_id"] = space_id
            f.write(json.dumps(s2) + "\n")
    discovery_path = output / "discovery.jsonl"
    with discovery_path.open("w", encoding="utf-8") as f:
        for bid, b in blocks.items():
            f.write(json.dumps({
                "id": bid,
                "title": _block_title(b),
                "type": b.get("type"),
                "parent_table": b.get("parent_table"),
                "parent_id": b.get("parent_id"),
                "last_edited_time": b.get("last_edited_time"),
                "space_id": space_id,
            }) + "\n")
    print(f"wrote: {sidebar_path}")
    print(f"wrote: {discovery_path}")
    return 0


def cmd_probe(args: argparse.Namespace) -> int:
    """Hit each /api/v3 endpoint we care about, dump shape + sample data."""
    token = load_token_v2(args.token_v2)
    if not token:
        print(
            "error: no token_v2; run "
            "`./notion_internal_dump.sh login` first",
            file=sys.stderr,
        )
        return 2
    client = NotionInternal(token, rps=args.rps)
    space_id = args.space_id or os.environ.get("NOTION_INTERNAL_SPACE_ID") or ""
    if not space_id:
        space_id, _ = _resolve_space(client, None)

    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)

    def _save(name: str, obj: Any) -> None:
        p = output / f"probe.{name}.json"
        p.write_text(json.dumps(obj, indent=2), encoding="utf-8")
        print(f"  wrote {p}")

    print("=== loadUserContent ===")
    try:
        uc = client.load_user_content()
        _save("loadUserContent", uc)
        rmap = uc.get("recordMap") or {}
        for table in sorted(rmap.keys()):
            ids = list(rmap[table].keys())
            print(f"  recordMap.{table}: {len(ids)} record(s)  e.g. {ids[:2]}")
    except Exception as e:
        print(f"  ERROR: {e}")

    print("\n=== getSpaces ===")
    try:
        gs = client.get_spaces()
        _save("getSpaces", gs)
        if isinstance(gs, dict):
            for k, v in gs.items():
                if isinstance(v, dict) and "recordMap" in v:
                    print(f"  spaces[{k}].recordMap tables: {sorted((v.get('recordMap') or {}).keys())}")
                elif k == "recordMap":
                    print(f"  recordMap tables: {sorted(v.keys())}")
    except Exception as e:
        print(f"  ERROR: {e}")

    print(f"\n=== syncRecordValues space:{space_id} ===")
    try:
        sr = client.sync_record_values([
            {"pointer": {"table": "space", "id": space_id}, "version": -1}
        ])
        _save("syncRecordValues_space", sr)
        rmap = sr.get("recordMap") or {}
        for table in sorted(rmap.keys()):
            ids = list(rmap[table].keys())
            print(f"  recordMap.{table}: {len(ids)} record(s)  e.g. {ids[:2]}")
        spaces = _walk_record_map(rmap, "space")
        sr_space = spaces.get(space_id) or {}
        print(f"  space.name = {sr_space.get('name')!r}")
        print(f"  space.pages = {len(sr_space.get('pages') or [])}  e.g. {(sr_space.get('pages') or [])[:3]}")
        print(f"  space.teams = {len(sr_space.get('teams') or [])}  e.g. {(sr_space.get('teams') or [])[:3]}")
        print(f"  space keys: {sorted(sr_space.keys())}")
    except Exception as e:
        print(f"  ERROR: {e}")

    print(f"\n=== search variants ===")
    for variant in ("minimal", "legacy"):
        print(f"  variant={variant}")
        try:
            r = client.search(space_id, query="", limit=5, variant=variant)
            _save(f"search_{variant}", r)
            print(f"    OK results={len(r.get('results') or [])}  recordMap.block={len((r.get('recordMap') or {}).get('block') or {})}")
        except Exception as e:
            print(f"    ERROR: {e}")
    return 0


# ---------------------------------------------------------------------------
# Phase 2: dump via exportBlock
# ---------------------------------------------------------------------------


def _download(
    url: str,
    dest: Path,
    *,
    timeout: int = 600,
    browser_cookies: Optional[dict[CookieKey, dict]] = None,
) -> int:
    headers: dict[str, str] = {"User-Agent": USER_AGENT}
    if browser_cookies:
        cookie_hdr = cookie_header_for(browser_cookies, url)
        if cookie_hdr:
            headers["Cookie"] = cookie_hdr
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp, dest.open("wb") as f:
        total = 0
        while True:
            chunk = resp.read(1 << 20)
            if not chunk:
                break
            f.write(chunk)
            total += len(chunk)
    return total


def _export_one(
    client: NotionInternal,
    container: dict,
    space_id: str,
    output_root: Path,
    *,
    task_pool: TaskPool,
    task_timeout: float,
    include_files: str,
    unzip: bool,
    browser_cookies: Optional[dict[CookieKey, dict]] = None,
) -> dict:
    label = (container.get("title") or container["id"])[:40]
    started = time.time()
    enqueue_backoff = DEFAULT_BACKOFF_INITIAL_S
    enqueue_deadline = started + task_timeout
    while True:
        try:
            task_id = client.enqueue_export_block(
                container["id"],
                space_id,
                include_files=include_files,
            )
            break
        except RateLimitedError as e:
            if time.time() >= enqueue_deadline:
                return {
                    "container": container,
                    "ok": False,
                    "phase": "enqueue",
                    "error": f"timed out under rate limiting: {e}",
                    "elapsed_s": time.time() - started,
                }
            sleep_s = min(MAX_BACKOFF_S, max(enqueue_backoff, e.retry_after))
            time.sleep(sleep_s)
            enqueue_backoff = min(MAX_BACKOFF_S, enqueue_backoff * 2)
            continue
        except Exception as e:
            return {
                "container": container,
                "ok": False,
                "phase": "enqueue",
                "error": str(e),
                "elapsed_s": time.time() - started,
            }
    completion = task_pool.register(task_id)
    remaining = max(0.0, started + task_timeout - time.time())
    completed = completion.wait(timeout=remaining)
    final = task_pool.status(task_id) or {}
    state = final.get("state") or "in_progress"
    status = final.get("status") or {}
    pages_exported = status.get("pagesExported") or 0
    if not completed:
        return {
            "container": container,
            "ok": False,
            "phase": "timeout",
            "task_id": task_id,
            "error": f"task did not finish in {task_timeout}s (state={state})",
            "elapsed_s": time.time() - started,
        }
    if state == "failure":
        return {
            "container": container,
            "ok": False,
            "phase": "task",
            "task_id": task_id,
            "error": json.dumps(status)[:500],
            "elapsed_s": time.time() - started,
        }
    export_url = status.get("exportURL") or ""
    if not export_url:
        return {
            "container": container,
            "ok": False,
            "phase": "no_url",
            "task_id": task_id,
            "error": f"task succeeded but no exportURL (status={status})",
            "elapsed_s": time.time() - started,
        }
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in label) or container["id"]
    dest_dir = output_root / "exports" / f"{safe}__{container['id']}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    zip_path = dest_dir / "export.zip"
    try:
        size = _download(export_url, zip_path, browser_cookies=browser_cookies)
    except Exception as e:
        return {
            "container": container,
            "ok": False,
            "phase": "download",
            "task_id": task_id,
            "error": str(e),
            "elapsed_s": time.time() - started,
        }
    if unzip:
        try:
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(dest_dir / "unzipped")
        except Exception as e:
            return {
                "container": container,
                "ok": True,
                "phase": "unzip_failed",
                "task_id": task_id,
                "zip": str(zip_path),
                "bytes": size,
                "error": str(e),
                "elapsed_s": time.time() - started,
                "pages_exported": pages_exported,
            }
    return {
        "container": container,
        "ok": True,
        "task_id": task_id,
        "zip": str(zip_path),
        "bytes": size,
        "pages_exported": pages_exported,
        "elapsed_s": time.time() - started,
    }


def cmd_dump(args: argparse.Namespace) -> int:
    token = load_token_v2(args.token_v2)
    if not token:
        print(
            "error: no token_v2; run "
            "`./notion_internal_dump.sh login` first",
            file=sys.stderr,
        )
        return 2
    client = NotionInternal(token, rps=args.rps)
    space_id, space_name = _resolve_space(
        client, args.space_id or os.environ.get("NOTION_INTERNAL_SPACE_ID")
    )
    print(f"space:  {space_name!r} ({space_id})")

    browser_cookies = load_browser_cookies()
    has_file_token = any(c["name"] == "file_token" for c in browser_cookies.values())
    if not has_file_token:
        print(
            "WARNING: no `file_token` cookie found in any browser profile. "
            "exportBlock tasks will succeed but downloading the resulting zip "
            "from file.notion.so will return HTTP 403 — the file proxy auths "
            "via `file_token` (HttpOnly, .notion.so, path /f) which only "
            "exists in your logged-in Chrome session.",
            file=sys.stderr,
        )
        print(
            "Quickest fix: open notion.so in Google Chrome, sign in fully, then "
            "re-run. The script auto-discovers cookies across all Chrome / Arc / "
            "Brave profiles and uses whichever profile holds the live session.",
            file=sys.stderr,
        )
    else:
        n_profiles_relevant = sum(1 for c in browser_cookies.values() if c["name"] in ("token_v2", "file_token", "p_sync_session"))
        print(f"browser cookies loaded: {len(browser_cookies)} total ({n_profiles_relevant} session-bearing)")

    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)

    sidebar = _walk_sidebar_top_level(client, space_id)
    if args.only:
        wanted = set(args.only.split(","))
        sidebar = [s for s in sidebar if s["id"] in wanted or (s.get("title") in wanted)]
    if args.skip:
        skip = set(args.skip.split(","))
        sidebar = [s for s in sidebar if s["id"] not in skip and (s.get("title") not in skip)]
    if not sidebar:
        print("nothing to export (sidebar empty after filters)", file=sys.stderr)
        return 3

    print(
        f"exporting {len(sidebar)} container(s) with {args.parallel} worker(s) "
        f"+ 1 shared poller @ {args.poll_interval:.1f}s"
    )
    poll_bucket = TokenBucket(args.poll_rps)
    task_pool = TaskPool(
        client, poll_interval=args.poll_interval, poll_bucket=poll_bucket
    )
    task_pool.start()
    results: list[dict] = []
    started = time.time()
    summary_path = output / "dump.summary.jsonl"
    summary_path.unlink(missing_ok=True)
    try:
        with ThreadPoolExecutor(max_workers=args.parallel) as ex:
            futs = {
                ex.submit(
                    _export_one,
                    client,
                    c,
                    space_id,
                    output,
                    task_pool=task_pool,
                    task_timeout=args.task_timeout,
                    include_files=args.include_files,
                    unzip=not args.no_unzip,
                    browser_cookies=browser_cookies,
                ): c
                for c in sidebar
            }
            done = 0
            counter_w = len(str(len(sidebar)))
            for f in as_completed(futs):
                res = f.result()
                results.append(res)
                done += 1
                with summary_path.open("a", encoding="utf-8") as sf:
                    sf.write(json.dumps(res) + "\n")
                label = (res["container"].get("title") or res["container"]["id"])[:40]
                tag = "OK  " if res.get("ok") else "FAIL"
                title_field = f"{label:<40}"
                if res.get("ok"):
                    n = int(res.get("pages_exported", 0) or 0)
                    pages_str = f"{n:>3} {'page ' if n == 1 else 'pages'}"
                    size_mb = res.get("bytes", 0) / 1e6
                    extra = (
                        f"  {pages_str}"
                        f"  {size_mb:>5.1f} MB"
                        f"  {res['elapsed_s']:>5.1f}s"
                    )
                else:
                    err = (res.get("error") or "")[:100]
                    extra = f"  phase={res.get('phase')} err={err}"
                print(
                    f"[{done:>{counter_w}}/{len(sidebar)}] {tag}  "
                    f"{title_field}{extra}"
                )
    finally:
        task_pool.stop()

    elapsed = time.time() - started
    ok = sum(1 for r in results if r.get("ok"))
    fail = len(results) - ok
    total_bytes = sum(r.get("bytes", 0) for r in results if r.get("ok"))
    pages = sum(r.get("pages_exported", 0) for r in results if r.get("ok"))
    print()
    print("--- dump summary ---")
    print(f"containers:  ok={ok}  fail={fail}")
    print(f"pages:       {pages} (server-reported)")
    print(f"zips total:  {total_bytes/1e6:.2f} MB")
    print(f"wall time:   {elapsed:.1f}s")
    if elapsed > 0 and total_bytes > 0:
        print(
            f"throughput:  {(total_bytes/1e6)/elapsed:.2f} MB/s "
            f"({pages/elapsed:.1f} pages/s)"
        )
    cooldown_pct = 100.0 * client.gate.total_wait_s / max(elapsed, 1e-6)
    print(
        f"rate-limit:  {client.gate.trips} trip(s), "
        f"{client.gate.total_wait_s:.1f}s total cooldown "
        f"({cooldown_pct:.1f}% of wall)"
    )
    avg_batch = (
        task_pool.batched_count / task_pool.poll_count
        if task_pool.poll_count else 0.0
    )
    naive_polls = int(elapsed / max(args.poll_interval, 1e-6)) * args.parallel
    print(
        f"poller:      {task_pool.poll_count} batched call(s), "
        f"avg {avg_batch:.1f} task ids/call "
        f"(vs ~{naive_polls} with per-worker polling)"
    )
    if client.gate.trips > 0 and args.parallel > 2:
        print(
            f"  hint: try --parallel {max(2, args.parallel // 2)} next time "
            f"to reduce enqueue rate-limit pressure further."
        )
    print(f"summary:     {summary_path}")
    return 0 if fail == 0 else 4


def cmd_grab(args: argparse.Namespace) -> int:
    print()
    print("=== Phase 1/2: discover ===")
    rc = cmd_discover(args)
    if rc != 0:
        return rc
    print()
    print("=== Phase 2/2: dump ===")
    return cmd_dump(args)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _add_common_token(p: argparse.ArgumentParser) -> None:
    p.add_argument("--token-v2", default="", help="Notion session cookie (overrides env / file)")
    p.add_argument("--space-id", default="", help="space id to target (skip prompt)")


def _add_dump_flags(p: argparse.ArgumentParser) -> None:
    p.add_argument("--output", default=DEFAULT_OUTPUT, help="output directory")
    p.add_argument("--rps", type=float, default=DEFAULT_RPS, help=f"req/s for /api/v3 (default {DEFAULT_RPS})")
    p.add_argument("--poll-rps", type=float, default=DEFAULT_POLL_RPS, help=f"req/s for getTasks polling (default {DEFAULT_POLL_RPS})")
    p.add_argument("--parallel", type=int, default=DEFAULT_EXPORT_PARALLEL, help=f"concurrent export tasks (default {DEFAULT_EXPORT_PARALLEL}). Notion's per-user concurrent task limit is around 4-5; going higher just causes 429s and isn't actually faster.")
    p.add_argument("--poll-interval", type=float, default=DEFAULT_POLL_INTERVAL, help=f"seconds between getTasks polls per task (default {DEFAULT_POLL_INTERVAL})")
    p.add_argument("--task-timeout", type=float, default=DEFAULT_TASK_TIMEOUT, help=f"max seconds to wait for a single export task (default {DEFAULT_TASK_TIMEOUT})")
    p.add_argument("--include-files", default="everything", choices=["everything", "no_files"], help="include file attachments in the zip")
    p.add_argument("--no-unzip", action="store_true", help="don't auto-unzip downloaded zips")
    p.add_argument("--only", default="", help="comma-separated container ids/titles to include")
    p.add_argument("--skip", default="", help="comma-separated container ids/titles to skip")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Notion internal-API dump (token_v2 cookie)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_login = sub.add_parser(
        "login",
        help="guided browser login: open notion.so, detect session, validate, save",
    )
    _add_common_token(p_login)
    p_login.add_argument(
        "--force",
        action="store_true",
        help="re-run the login flow even if a saved token already validates",
    )
    p_login.add_argument(
        "--no-browser",
        action="store_true",
        help="don't auto-open the browser; just print the URL and poll",
    )
    p_login.add_argument(
        "--no-pick",
        action="store_true",
        help="don't prompt for which workspace to pin (skips writing NOTION_INTERNAL_SPACE_ID)",
    )
    p_login.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="seconds to wait for sign-in before giving up (default 300)",
    )
    p_login.add_argument(
        "--poll-interval",
        type=float,
        default=2.0,
        help="seconds between cookie-store polls (default 2.0)",
    )
    p_login.add_argument(
        "--verbose",
        action="store_true",
        help="print per-poll diagnostics (cookie file mtimes, extraction details, validation errors)",
    )
    p_login.set_defaults(func=cmd_login)

    p_paste = sub.add_parser(
        "login-paste",
        help="manual fallback: paste token_v2 from DevTools, validate, save",
    )
    _add_common_token(p_paste)
    p_paste.add_argument(
        "--no-browser",
        action="store_true",
        help="don't auto-open notion.so in your browser",
    )
    p_paste.add_argument(
        "--no-pick",
        action="store_true",
        help="don't prompt for which workspace to pin",
    )
    p_paste.set_defaults(func=cmd_login_paste)

    p_disc = sub.add_parser("discover", help="walk sidebar containers + (optional) /api/v3/search")
    _add_common_token(p_disc)
    p_disc.add_argument("--output", default=DEFAULT_OUTPUT, help="output directory")
    p_disc.add_argument("--rps", type=float, default=DEFAULT_RPS, help="req/s")
    p_disc.add_argument("--page-size", type=int, default=300, help="search batch size (Notion may cap at 1000)")
    p_disc.add_argument("--max-pages", type=int, default=100000, help="cap on pages discovered via search")
    p_disc.set_defaults(func=cmd_discover)

    p_probe = sub.add_parser("probe", help="diagnostic: dump raw responses from key endpoints")
    _add_common_token(p_probe)
    p_probe.add_argument("--output", default=DEFAULT_OUTPUT, help="where to write probe.*.json")
    p_probe.add_argument("--rps", type=float, default=DEFAULT_RPS, help="req/s")
    p_probe.set_defaults(func=cmd_probe)

    p_dump = sub.add_parser("dump", help="exportBlock per top-level container, download zips")
    _add_common_token(p_dump)
    _add_dump_flags(p_dump)
    p_dump.set_defaults(func=cmd_dump)

    p_grab = sub.add_parser("grab", help="discover + dump (the all-in-one)")
    _add_common_token(p_grab)
    p_grab.add_argument("--page-size", type=int, default=300)
    p_grab.add_argument("--max-pages", type=int, default=100000)
    _add_dump_flags(p_grab)
    p_grab.set_defaults(func=cmd_grab)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
