# A.R.M.O.R Vault — what was added

Your backend (`consistent_hashing.py`, `raft_lite.py`, `metadata_service.py`,
`storage_node.py`, `quorum.py`, `erasure_coding.py`, `scrubber.py`,
`gossip.py`, `phi_accrual.py`, `merkle_tree.py`, `vector_clock.py`) was not
touched — every original file is unchanged.

## Backend check

`python demo.py` and `python -m unittest test_vault.py` both run clean (6/6
tests pass). No bugs found. One thing worth knowing: `DESIGN.md` describes a
`Chunker` that splits large objects into multi-MB blocks, but there's no
`chunker.py` in the codebase — objects are written as a single chunk. That's
not a bug, just an unfinished piece of the design doc; the new API below
follows the code as it actually is (one object = one chunk), the same way
`demo.py` and `test_vault.py` already do.

## What's new

- **`vault_core.py`** — a plain-Python orchestration layer that wires the
  existing modules into one addressable cluster (nodes, Raft leader election,
  bucket policies, object read/write/delete, repair). It only calls the
  public methods your modules already expose.
- **`server.py`** — a FastAPI REST API over `vault_core.py`.
- **`frontend/`** — the admin and user web UIs (plain HTML/CSS/JS, white
  theme, no build step).

## Run it

```bash
pip install -r requirements.txt
python server.py
```

This starts the API at `http://localhost:8000` (interactive docs at
`/docs`). It boots a 6-node cluster with two buckets already configured:
`hot` (3x replication) and `cold` (4+2 erasure coding).

Then open `frontend/index.html` directly in a browser (double-click it, or
`python -m http.server 5500` from the `frontend/` folder and visit
`http://localhost:5500`). Pick **Admin** or **User**.

If your API runs somewhere other than `localhost:8000`, open the browser
console on either page and run:
```js
localStorage.setItem("armor_api_base", "http://your-host:port")
```

## Admin console (`admin.html`)

- Cluster overview: Raft leader, term, alive/total nodes, object & byte counts
- Storage nodes: add to the ring, mark a node down/up, remove it
- Raft cluster: role/term/log/commit-index per metadata node
- Bucket policies: view and create replication (`N/W/R`) or erasure (`K/M`)
  buckets
- All objects: bucket, key, size, mode, placement, write time
- Repair: runs a Merkle-diff scrub across every replicated object and patches
  stale/missing replicas

## User portal (`user.html`)

- Upload text or a file into any bucket/key
- Browse objects (optionally filtered by bucket)
- Download an object (reconstructed via quorum read or erasure decode)
- Delete an object

## Notes / limitations carried over from the backend design

- Everything is in-memory and single-process — restarting `server.py` clears
  all nodes, buckets, and objects back to the two-bucket, six-node defaults.
- There's no authentication in the backend, so "admin" vs "user" here is just
  two different UIs against the same open API, not an access-control
  boundary. If you need real auth, that has to be added to `server.py`.
- Gossip membership dissemination and the Phi Accrual failure detector
  (`gossip.py`, `phi_accrual.py`) exist in your backend but aren't wired into
  the API yet — node up/down in the admin console is a direct toggle, not
  detected via heartbeats. Happy to wire that in next if you want live
  failure detection in the UI.
