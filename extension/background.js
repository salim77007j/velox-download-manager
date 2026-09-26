// Velox browser extension — sends downloads to the local Velox daemon
// (127.0.0.1, token-authenticated). No data leaves the machine.

const DEFAULTS = { port: 7654, token: "", enabled: true, captureMinSizeMB: 0 };

async function cfg() {
  return { ...DEFAULTS, ...(await chrome.storage.local.get(Object.keys(DEFAULTS))) };
}

async function addToVelox(url, referrer) {
  const c = await cfg();
  if (!c.enabled || !c.token) return { ok: false, error: "Velox: extension not configured (set the API token in options)" };
  try {
    const res = await fetch(`http://127.0.0.1:${c.port}/api/add`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Velox-Token": c.token },
      body: JSON.stringify({ url, headers: referrer ? [["Referer", referrer]] : [] }),
    });
    if (res.status === 401) return { ok: false, error: "Velox: bad API token" };
    if (!res.ok) return { ok: false, error: `Velox: HTTP ${res.status}` };
    const j = await res.json();
    return { ok: true, id: j.id };
  } catch (e) {
    return { ok: false, error: "Velox daemon not reachable — run `velox serve`" };
  }
}

function notify(msg) {
  try {
    chrome.notifications.create({ type: "basic", iconUrl: "icon.png", title: "Velox", message: msg });
  } catch (_) {}
}

// Context menu
chrome.runtime.onInstalled.addListener(() => {
  chrome.contextMenus.create({ id: "velox-download", title: "Download with Velox", contexts: ["link", "audio", "video"] });
});

chrome.contextMenus.onClicked.addListener(async (info, tab) => {
  const r = await addToVelox(info.linkUrl || info.srcUrl || info.frameUrl, tab?.url);
  notify(r.ok ? `Added to Velox: ${info.linkUrl || info.srcUrl}` : r.error);
});

// Intercept regular downloads and route them to Velox instead

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg?.type === "add") {
    addToVelox(msg.url, msg.referrer).then(sendResponse);
    return true;
  }
  if (msg?.type === "ping") {
    cfg().then(async (c) => {
      try {
        const res = await fetch(`http://127.0.0.1:${c.port}/api/ping`, {
          headers: { "X-Velox-Token": c.token },
        });
        sendResponse({ ok: res.ok, status: res.status });
      } catch (_) {
        sendResponse({ ok: false });
      }
    });
    return true;
  }
});
