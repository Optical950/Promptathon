"""
REST API for the A.R.M.O.R / Vault backend.

This is the only new file that turns the existing simulation modules into
something a browser frontend can talk to. It does not modify any of the
original backend files -- it just imports and calls them (via vault_core.py)
the same way demo.py and test_vault.py already do.

Run:
    pip install -r requirements.txt
    python server.py
    # -> http://localhost:8000  (interactive docs at /docs)
"""

from __future__ import annotations

import base64
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from vault_core import (
    VaultCluster,
    NodeNotFoundError,
    BucketNotFoundError,
    ObjectNotFoundError,
    WriteFailedError,
    ReadFailedError,
    VaultError,
    ObjectRecord,
)

app = FastAPI(title="A.R.M.O.R Vault API", version="1.0.0")

# Wide-open CORS so admin.html / user.html can be opened directly from disk
# (file://) or served from any local port during development.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

vault = VaultCluster()


def _http_error(e: Exception) -> HTTPException:
    if isinstance(e, (NodeNotFoundError, BucketNotFoundError, ObjectNotFoundError)):
        return HTTPException(status_code=404, detail=str(e))
    if isinstance(e, (WriteFailedError, ReadFailedError)):
        return HTTPException(status_code=503, detail=str(e))
    if isinstance(e, VaultError):
        return HTTPException(status_code=400, detail=str(e))
    return HTTPException(status_code=500, detail=str(e))


def _record_to_dict(r: ObjectRecord) -> dict:
    d = {
        "bucket": r.bucket,
        "key": r.key,
        "size": r.size,
        "mode": r.mode,
        "placement": r.placement,
        "content_type": r.content_type,
        "filename": r.filename,
        "created_at": r.created_at,
    }
    if r.mode == "replication":
        d.update(n=r.n, w=r.w, r=r.r)
    else:
        d.update(k=r.k, m=r.m)
    return d


# --------------------------------------------------------------------------- models

class AddNodeRequest(BaseModel):
    node_id: str = Field(..., min_length=1)
    failure_domain: str = "default"
    weight: int = Field(1, ge=1)


class ToggleNodeRequest(BaseModel):
    alive: bool


class SetBucketPolicyRequest(BaseModel):
    bucket: str = Field(..., min_length=1)
    mode: Literal["replication", "erasure"]
    n: int = 3
    w: int = 2
    r: int = 2
    k: int = 4
    m: int = 2


class WriteObjectRequest(BaseModel):
    bucket: str
    key: str
    data_base64: str
    content_type: str = "application/octet-stream"
    filename: Optional[str] = None


# --------------------------------------------------------------------------- health

@app.get("/api/health")
def health():
    return {"status": "ok"}


# --------------------------------------------------------------------------- admin: overview / raft

@app.get("/api/admin/overview")
def admin_overview():
    return vault.overview()


@app.get("/api/admin/raft")
def admin_raft():
    return vault.raft_status()


# --------------------------------------------------------------------------- admin: nodes

@app.get("/api/admin/nodes")
def admin_list_nodes():
    return vault.list_nodes()


@app.post("/api/admin/nodes", status_code=201)
def admin_add_node(req: AddNodeRequest):
    try:
        return vault.add_node(req.node_id, req.failure_domain, req.weight)
    except VaultError as e:
        raise _http_error(e)


@app.post("/api/admin/nodes/{node_id}/toggle")
def admin_toggle_node(node_id: str, req: ToggleNodeRequest):
    try:
        vault.set_node_alive(node_id, req.alive)
        return {"node_id": node_id, "alive": req.alive}
    except VaultError as e:
        raise _http_error(e)


@app.delete("/api/admin/nodes/{node_id}")
def admin_remove_node(node_id: str):
    try:
        vault.remove_node(node_id)
        return {"removed": node_id}
    except VaultError as e:
        raise _http_error(e)


# --------------------------------------------------------------------------- admin: buckets

@app.get("/api/admin/buckets")
def admin_list_buckets():
    return vault.list_buckets()


@app.post("/api/admin/buckets", status_code=201)
def admin_set_bucket_policy(req: SetBucketPolicyRequest):
    try:
        return vault.set_bucket_policy(req.bucket, req.mode, req.n, req.w, req.r, req.k, req.m)
    except (ValueError, VaultError) as e:
        raise HTTPException(status_code=400, detail=str(e))


# --------------------------------------------------------------------------- admin: objects / repair

@app.get("/api/admin/objects")
def admin_list_objects():
    return [_record_to_dict(r) for r in vault.list_objects()]


@app.post("/api/admin/repair")
def admin_repair():
    return vault.run_scrub()


# --------------------------------------------------------------------------- user-facing: buckets

@app.get("/api/buckets")
def list_buckets():
    """Lightweight bucket listing for populating the user portal's dropdown."""
    return [{"bucket": b["bucket"], "mode": b["mode"]} for b in vault.list_buckets()]


# --------------------------------------------------------------------------- user-facing: objects

@app.get("/api/objects")
def list_objects(bucket: Optional[str] = None):
    return [_record_to_dict(r) for r in vault.list_objects(bucket)]


@app.post("/api/objects", status_code=201)
def write_object(req: WriteObjectRequest):
    try:
        data = base64.b64decode(req.data_base64)
    except Exception:
        raise HTTPException(status_code=400, detail="data_base64 is not valid base64")

    try:
        record = vault.write_object(req.bucket, req.key, data, req.content_type, req.filename)
    except VaultError as e:
        raise _http_error(e)
    return _record_to_dict(record)


@app.get("/api/objects/{bucket}/{key}")
def read_object(bucket: str, key: str):
    try:
        data, record = vault.read_object(bucket, key)
    except VaultError as e:
        raise _http_error(e)
    payload = _record_to_dict(record)
    payload["data_base64"] = base64.b64encode(data).decode("ascii")
    return payload


@app.delete("/api/objects/{bucket}/{key}")
def delete_object(bucket: str, key: str):
    try:
        vault.delete_object(bucket, key)
    except VaultError as e:
        raise _http_error(e)
    return {"deleted": f"{bucket}/{key}"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
