async function refreshAll() {
  const ok = await checkConnection();
  if (!ok) return;
  await Promise.all([refreshOverview(), refreshNodes(), refreshRaft(), refreshBuckets(), refreshObjects()]);
}

async function refreshOverview() {
  try {
    const o = await api("/api/admin/overview");
    const stats = [
      ["Raft leader", o.leader || "—"],
      ["Term", o.raft_term ?? "—"],
      ["Nodes alive", `${o.alive_count}/${o.node_count}`],
      ["Buckets", o.bucket_count],
      ["Objects", o.object_count],
      ["Bytes stored", fmtBytes(o.total_bytes)],
      ["Repairs run", o.total_repairs],
    ];
    document.getElementById("statRow").innerHTML = stats
      .map(([label, num]) => `<div class="stat"><div class="num">${num}</div><div class="label">${label}</div></div>`)
      .join("");
  } catch (e) { showToast(e.message, true); }
}

async function refreshNodes() {
  try {
    const nodes = await api("/api/admin/nodes");
    const body = document.getElementById("nodesBody");
    if (!nodes.length) {
      body.innerHTML = `<tr><td colspan="5"><div class="empty">No storage nodes in the ring.</div></td></tr>`;
      return;
    }
    body.innerHTML = nodes.map(n => `
      <tr>
        <td class="mono">${n.node_id}</td>
        <td class="mono">${n.failure_domain}</td>
        <td>${n.alive ? `<span class="badge ok"><span class="dot"></span>alive</span>` : `<span class="badge down"><span class="dot"></span>down</span>`}</td>
        <td class="mono">${n.chunk_count}</td>
        <td>
          <button class="btn btn-sm" data-toggle="${n.node_id}" data-alive="${n.alive}">${n.alive ? "Mark down" : "Bring up"}</button>
          <button class="btn btn-sm danger" data-remove="${n.node_id}">Remove</button>
        </td>
      </tr>`).join("");

    body.querySelectorAll("[data-toggle]").forEach(btn => {
      btn.addEventListener("click", async () => {
        const nodeId = btn.getAttribute("data-toggle");
        const currentlyAlive = btn.getAttribute("data-alive") === "true";
        try {
          await api(`/api/admin/nodes/${encodeURIComponent(nodeId)}/toggle`, {
            method: "POST", body: JSON.stringify({ alive: !currentlyAlive }),
          });
          showToast(`${nodeId} is now ${!currentlyAlive ? "alive" : "down"}`);
          refreshAll();
        } catch (e) { showToast(e.message, true); }
      });
    });
    body.querySelectorAll("[data-remove]").forEach(btn => {
      btn.addEventListener("click", async () => {
        const nodeId = btn.getAttribute("data-remove");
        if (!confirm(`Remove ${nodeId} from the ring? Objects placed only on it become unreadable.`)) return;
        try {
          await api(`/api/admin/nodes/${encodeURIComponent(nodeId)}`, { method: "DELETE" });
          showToast(`${nodeId} removed`);
          refreshAll();
        } catch (e) { showToast(e.message, true); }
      });
    });
  } catch (e) { showToast(e.message, true); }
}

async function refreshRaft() {
  try {
    const nodes = await api("/api/admin/raft");
    document.getElementById("raftBody").innerHTML = nodes.map(n => `
      <tr>
        <td class="mono">${n.node_id}</td>
        <td>${n.role === "leader" ? `<span class="badge ok">leader</span>` : `<span class="badge neutral">${n.role}</span>`}</td>
        <td class="mono">${n.term}</td>
        <td class="mono">${n.log_length}</td>
        <td class="mono">${n.commit_index}</td>
      </tr>`).join("");
  } catch (e) { showToast(e.message, true); }
}

async function refreshBuckets() {
  try {
    const buckets = await api("/api/admin/buckets");
    const body = document.getElementById("bucketsBody");
    if (!buckets.length) {
      body.innerHTML = `<tr><td colspan="5"><div class="empty">No buckets configured.</div></td></tr>`;
      return;
    }
    body.innerHTML = buckets.map(b => {
      const cfg = b.mode === "replication" ? `N=${b.n} W=${b.w} R=${b.r}` : `K=${b.k} M=${b.m}`;
      return `
      <tr>
        <td class="mono">${b.bucket}</td>
        <td><span class="badge neutral">${b.mode}</span></td>
        <td class="mono">${cfg}</td>
        <td class="mono">${b.shard_count}</td>
        <td class="mono">${(b.efficiency * 100).toFixed(1)}%</td>
      </tr>`;
    }).join("");
  } catch (e) { showToast(e.message, true); }
}

async function refreshObjects() {
  try {
    const objs = await api("/api/admin/objects");
    const body = document.getElementById("objectsBody");
    if (!objs.length) {
      body.innerHTML = `<tr><td colspan="5"><div class="empty">No objects written yet.</div></td></tr>`;
      return;
    }
    body.innerHTML = objs.map(o => `
      <tr>
        <td class="mono">${o.bucket}/${o.key}</td>
        <td class="mono">${fmtBytes(o.size)}</td>
        <td><span class="badge neutral">${o.mode}</span></td>
        <td class="mono" style="max-width:260px; overflow-wrap:anywhere;">${o.placement.join(", ")}</td>
        <td class="mono">${fmtTime(o.created_at)}</td>
      </tr>`).join("");
  } catch (e) { showToast(e.message, true); }
}

document.querySelectorAll('input[name="mode"]').forEach(r => {
  r.addEventListener("change", () => {
    const isRepl = document.querySelector('input[name="mode"]:checked').value === "replication";
    document.getElementById("replFields").style.display = isRepl ? "" : "none";
    document.getElementById("ecFields").style.display = isRepl ? "none" : "";
  });
});

document.getElementById("addNodeForm").addEventListener("submit", async (e) => {
  e.preventDefault();
  const node_id = document.getElementById("nodeId").value.trim();
  const failure_domain = document.getElementById("failureDomain").value.trim() || "default";
  const weight = parseInt(document.getElementById("weight").value, 10) || 1;
  try {
    await api("/api/admin/nodes", { method: "POST", body: JSON.stringify({ node_id, failure_domain, weight }) });
    showToast(`${node_id} added to ring`);
    e.target.reset();
    document.getElementById("failureDomain").value = "rack-0";
    document.getElementById("weight").value = "1";
    refreshAll();
  } catch (err) { showToast(err.message, true); }
});

document.getElementById("bucketForm").addEventListener("submit", async (e) => {
  e.preventDefault();
  const mode = document.querySelector('input[name="mode"]:checked').value;
  const payload = {
    bucket: document.getElementById("bucketName").value.trim(),
    mode,
    n: parseInt(document.getElementById("fN").value, 10),
    w: parseInt(document.getElementById("fW").value, 10),
    r: parseInt(document.getElementById("fR").value, 10),
    k: parseInt(document.getElementById("fK").value, 10),
    m: parseInt(document.getElementById("fM").value, 10),
  };
  try {
    await api("/api/admin/buckets", { method: "POST", body: JSON.stringify(payload) });
    showToast(`Bucket "${payload.bucket}" saved`);
    refreshBuckets();
  } catch (err) { showToast(err.message, true); }
});

document.getElementById("repairBtn").addEventListener("click", async () => {
  const btn = document.getElementById("repairBtn");
  btn.disabled = true;
  try {
    const res = await api("/api/admin/repair", { method: "POST" });
    document.getElementById("repairResult").textContent =
      `Scanned ${res.objects_scanned} object(s), repaired ${res.chunks_repaired} chunk(s).`;
    showToast("Repair pass complete");
    refreshOverview();
  } catch (err) { showToast(err.message, true); }
  btn.disabled = false;
});

refreshAll();
setInterval(refreshAll, 8000);
