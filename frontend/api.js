// Shared fetch helper for both admin.html and user.html.
// Change API_BASE if you run server.py on a different host/port.
const API_BASE = window.localStorage.getItem("armor_api_base") || "http://localhost:8000";

async function api(path, options = {}) {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  let body = null;
  try { body = await res.json(); } catch (_) { /* no body */ }
  if (!res.ok) {
    const detail = (body && body.detail) ? body.detail : res.statusText;
    throw new Error(detail);
  }
  return body;
}

function showToast(msg, isError = false) {
  const el = document.getElementById("toast");
  if (!el) return;
  el.textContent = msg;
  el.classList.toggle("error", isError);
  el.classList.add("show");
  clearTimeout(el._t);
  el._t = setTimeout(() => el.classList.remove("show"), 3200);
}

function fmtBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(2)} MB`;
}

function fmtTime(ts) {
  const d = new Date(ts * 1000);
  return d.toLocaleString();
}

function b64FromBytes(bytes) {
  let binary = "";
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
  }
  return btoa(binary);
}

function bytesFromB64(b64) {
  const binary = atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

// Polls /api/health so pages can show a clear "backend not running" banner
// instead of failing silently.
async function checkConnection() {
  const banner = document.getElementById("connBanner");
  try {
    await api("/api/health");
    if (banner) banner.classList.remove("show");
    return true;
  } catch (e) {
    if (banner) {
      banner.classList.add("show");
      banner.textContent = `Can't reach the Vault API at ${API_BASE}. Start it with: python server.py`;
    }
    return false;
  }
}
