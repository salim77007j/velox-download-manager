const $ = (id) => document.getElementById(id);
chrome.storage.local.get(["port", "token"]).then(({ port, token }) => {
  $("port").value = port ?? 7654;
  $("token").value = token ?? "";
});
$("save").onclick = async () => {
  await chrome.storage.local.set({
    port: Number($("port").value) || 7654,
    token: $("token").value.trim(),
  });
  $("saved").textContent = "Saved ✔";
  setTimeout(() => ($("saved").textContent = ""), 1500);
};
