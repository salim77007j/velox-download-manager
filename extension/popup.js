const status = document.getElementById("status");
document.getElementById("go").onclick = async () => {
  const url = document.getElementById("url").value.trim();
  if (!url) { status.textContent = "Enter a URL first."; return; }
  status.textContent = "Sending…";
  const r = await chrome.runtime.sendMessage({ type: "add", url });
  status.textContent = r?.ok ? "Added to Velox ✔" : r?.error || "Failed";
};

(async () => {
  const p = await chrome.runtime.sendMessage({ type: "ping" });
  status.textContent = p?.ok
    ? "Daemon: connected ✔"
    : "Daemon: not reachable — run `velox serve` and set the token in options.";
})();
