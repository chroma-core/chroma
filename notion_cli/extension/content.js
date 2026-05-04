// Notion CLI helper — content script.
//
// Runs on every notion.so / notion.com page at document_start. If the URL
// includes a ?cli-handoff=PORT&nonce=NONCE pair, asks the background
// service worker to read the session cookies and POST them to a local
// CLI listener on 127.0.0.1:PORT. Otherwise no-op.
//
// All the privileged work (reading HttpOnly cookies, fetching localhost)
// happens in background.js. This script just plumbs the request and
// shows a banner.

(function () {
  const params = new URLSearchParams(location.search);
  const port = params.get("cli-handoff");
  const nonce = params.get("nonce");
  if (!port || !nonce) return;
  if (!/^\d+$/.test(port)) return;
  if (!/^[a-f0-9]{16,128}$/.test(nonce)) return;

  showBanner(
    "Notion CLI helper: handing off your session to localhost:" +
      port +
      "...",
    "info",
  );

  chrome.runtime.sendMessage(
    { type: "handoff", port: port, nonce: nonce },
    (resp) => {
      if (chrome.runtime.lastError) {
        showBanner(
          "Notion CLI helper: extension background unreachable — " +
            chrome.runtime.lastError.message,
          "error",
        );
        return;
      }
      if (!resp) {
        showBanner(
          "Notion CLI helper: no response from background worker",
          "error",
        );
        return;
      }
      if (resp.ok) {
        const bits = [];
        if (resp.had_token_v2) bits.push("token_v2");
        if (resp.had_file_token) bits.push("file_token");
        showBanner(
          "Notion CLI helper: sent " +
            (bits.join(" + ") || "(no cookies?)") +
            " to localhost:" +
            port +
            ". You can close this tab.",
          "ok",
        );
      } else {
        showBanner(
          "Notion CLI helper: " + (resp.error || "unknown error"),
          "error",
        );
      }
    },
  );
})();

function showBanner(msg, kind) {
  const root = document.documentElement;
  if (!root) return;
  const id = "notion-cli-helper-banner";
  document.getElementById(id)?.remove();
  const div = document.createElement("div");
  div.id = id;
  div.textContent = msg;
  const bg = kind === "ok" ? "#1f7a3a" : kind === "error" ? "#a1262a" : "#1f3f7a";
  div.style.cssText = [
    "position:fixed",
    "top:16px",
    "left:50%",
    "transform:translateX(-50%)",
    "background:" + bg,
    "color:white",
    "padding:12px 24px",
    "border-radius:8px",
    "font:14px -apple-system,BlinkMacSystemFont,Segoe UI,sans-serif",
    "z-index:2147483647",
    "box-shadow:0 4px 12px rgba(0,0,0,0.3)",
    "max-width:80vw",
  ].join(";");
  // body may not exist yet at document_start; attach to documentElement
  // and let it ride.
  root.appendChild(div);
  if (kind !== "error") {
    setTimeout(() => div.remove(), 8000);
  }
}
