let bucketCache = [];

async function init() {
  const ok = await checkConnection();
  if (!ok) return;
  await loadBuckets();
  await loadObjects();
}

async function loadBuckets() {
  try {
    bucketCache = await api("/api/buckets");
    const opts = bucketCache.map(b => `<option value="${b.bucket}">${b.bucket} (${b.mode})</option>`).join("");
    document.getElementById("bucketSelect").innerHTML = opts || `<option value="">No buckets yet</option>`;
    document.getElementById("filterBucket").innerHTML =
      `<option value="">All buckets</option>` + bucketCache.map(b => `<option value="${b.bucket}">${b.bucket}</option>`).join("");
  } catch (e) { showToast(e.message, true); }
}

async function loadObjects() {
  const bucket = document.getElementById("filterBucket").value;
  try {
    const path = bucket ? `/api/objects?bucket=${encodeURIComponent(bucket)}` : "/api/objects";
    const objs = await api(path);
    const body = document.getElementById("objectsBody");
    if (!objs.length) {
      body.innerHTML = `<tr><td colspan="6"><div class="empty">No objects yet — upload one above.</div></td></tr>`;
      return;
    }
    body.innerHTML = objs.map(o => `
      <tr>
        <td class="mono">${o.bucket}/${o.key}</td>
        <td class="mono">${o.content_type}</td>
        <td class="mono">${fmtBytes(o.size)}</td>
        <td><span class="badge neutral">${o.mode === "replication" ? `N${o.n}/W${o.w}/R${o.r}` : `K${o.k}+M${o.m}`}</span></td>
        <td class="mono">${fmtTime(o.created_at)}</td>
        <td>
          <button class="btn btn-sm" data-dl="${o.bucket}|${o.key}">Download</button>
          <button class="btn btn-sm danger" data-del="${o.bucket}|${o.key}">Delete</button>
        </td>
      </tr>`).join("");

    body.querySelectorAll("[data-dl]").forEach(btn => {
      btn.addEventListener("click", () => downloadObject(...btn.getAttribute("data-dl").split("|")));
    });
    body.querySelectorAll("[data-del]").forEach(btn => {
      btn.addEventListener("click", () => deleteObject(...btn.getAttribute("data-del").split("|")));
    });
  } catch (e) { showToast(e.message, true); }
}

async function downloadObject(bucket, key) {
  try {
    const res = await api(`/api/objects/${encodeURIComponent(bucket)}/${encodeURIComponent(key)}`);
    const bytes = bytesFromB64(res.data_base64);
    const blob = new Blob([bytes], { type: res.content_type || "application/octet-stream" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = res.filename || key.split("/").pop() || "object";
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
    showToast(`Downloaded ${bucket}/${key}`);
  } catch (e) { showToast(e.message, true); }
}

async function deleteObject(bucket, key) {
  if (!confirm(`Delete ${bucket}/${key}? This can't be undone.`)) return;
  try {
    await api(`/api/objects/${encodeURIComponent(bucket)}/${encodeURIComponent(key)}`, { method: "DELETE" });
    showToast(`Deleted ${bucket}/${key}`);
    loadObjects();
  } catch (e) { showToast(e.message, true); }
}

document.getElementById("filterBucket").addEventListener("change", loadObjects);
document.getElementById("refreshBtn").addEventListener("click", loadObjects);

document.getElementById("uploadForm").addEventListener("submit", async (e) => {
  e.preventDefault();
  const bucket = document.getElementById("bucketSelect").value;
  const key = document.getElementById("objKey").value.trim();
  const text = document.getElementById("objText").value;
  const fileInput = document.getElementById("objFile");
  if (!bucket) { showToast("No bucket available — ask an admin to create one first.", true); return; }

  let data_base64, content_type, filename;
  if (fileInput.files && fileInput.files[0]) {
    const file = fileInput.files[0];
    const buf = new Uint8Array(await file.arrayBuffer());
    data_base64 = b64FromBytes(buf);
    content_type = file.type || "application/octet-stream";
    filename = file.name;
  } else {
    const enc = new TextEncoder().encode(text);
    data_base64 = b64FromBytes(enc);
    content_type = "text/plain";
    filename = null;
  }

  try {
    await api("/api/objects", {
      method: "POST",
      body: JSON.stringify({ bucket, key, data_base64, content_type, filename }),
    });
    showToast(`Wrote ${bucket}/${key}`);
    e.target.reset();
    loadObjects();
  } catch (err) { showToast(err.message, true); }
});

init();
setInterval(loadObjects, 10000);
