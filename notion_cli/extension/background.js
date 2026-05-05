// Notion CLI helper — background service worker.
//
// Receives `handoff` requests from content.js (which only fires when a
// notion.so URL contains ?cli-handoff=PORT&nonce=NONCE), reads
// token_v2 + file_token via the privileged chrome.cookies API, and POSTs
// them to http://127.0.0.1:PORT/handoff. The CLI half of this dance is in
// notion_cli/notion_auth.py (cmd_login_extension).

const NOTION_DOMAINS = ["notion.so", "notion.com"];

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (!msg || msg.type !== "handoff") {
    return false;
  }
  const senderUrl =
    (sender && sender.url) ||
    (sender && sender.tab && sender.tab.url) ||
    "";
  if (!/^https:\/\/[^/]*\.notion\.(so|com)\//.test(senderUrl)) {
    sendResponse({ ok: false, error: "sender not on notion.so" });
    return true;
  }
  doHandoff(msg.port, msg.nonce)
    .then(sendResponse)
    .catch((e) => sendResponse({ ok: false, error: String(e) }));
  return true; // keep sendResponse channel alive for async resolution
});

async function doHandoff(port, nonce) {
  const portNum = parseInt(port, 10);
  if (!Number.isFinite(portNum) || portNum < 1024 || portNum > 65535) {
    return { ok: false, error: `invalid port: ${port}` };
  }
  if (!/^[a-f0-9]{16,128}$/.test(nonce)) {
    return { ok: false, error: "invalid nonce" };
  }

  const cookies = await getNotionCookies();
  const token_v2 = pickCookie(cookies, "token_v2");
  const file_token = pickCookie(cookies, "file_token");

  const body = {
    nonce: nonce,
    token_v2: token_v2 ? token_v2.value : null,
    file_token: file_token ? file_token.value : null,
    token_v2_domain: token_v2 ? token_v2.domain : null,
    file_token_domain: file_token ? file_token.domain : null,
    extension_version:
      (chrome.runtime.getManifest && chrome.runtime.getManifest().version) ||
      "?",
  };

  let resp;
  try {
    resp = await fetch(`http://127.0.0.1:${portNum}/handoff`, {
      method: "POST",
      // text/plain is a CORS-safelisted Content-Type so the browser
      // skips preflight; the local server json-parses the body anyway.
      headers: { "Content-Type": "text/plain;charset=utf-8" },
      body: JSON.stringify(body),
    });
  } catch (e) {
    return { ok: false, error: `could not reach CLI on 127.0.0.1:${portNum}: ${e}` };
  }
  if (!resp.ok) {
    return {
      ok: false,
      error: `CLI returned HTTP ${resp.status}`,
      had_token_v2: Boolean(body.token_v2),
      had_file_token: Boolean(body.file_token),
    };
  }
  return {
    ok: true,
    had_token_v2: Boolean(body.token_v2),
    had_file_token: Boolean(body.file_token),
  };
}

async function getNotionCookies() {
  const all = [];
  for (const dom of NOTION_DOMAINS) {
    try {
      const part = await chrome.cookies.getAll({ domain: dom });
      all.push(...part);
    } catch (e) {
      // Permissions issue or transient; just keep going.
    }
  }
  return all;
}

// Prefer cookies whose domain matches www.notion.so / .notion.so over
// .app.notion.com or other variants. Notion mints token_v2 on multiple
// domains and only the www-side one validates against /api/v3 endpoints.
function pickCookie(cookies, name) {
  const matches = cookies.filter((c) => c.name === name && c.value);
  if (matches.length === 0) return null;
  matches.sort((a, b) => domainPriority(b.domain) - domainPriority(a.domain));
  return matches[0];
}

function domainPriority(domain) {
  const d = (domain || "").toLowerCase();
  if (d === ".notion.so" || d === "www.notion.so" || d === ".www.notion.so") return 3;
  if (d.endsWith(".notion.so")) return 2;
  if (d.endsWith(".notion.com")) return 1;
  return 0;
}
