#!/usr/bin/env python3
"""
Lab 4 Starter — Participant (2PC/3PC) (HTTP, standard library only)
===================================================================

2PC endpoints:
- POST /prepare   {"txid":"TX1","op":{...}} -> {"vote":"YES"/"NO"}
- POST /commit    {"txid":"TX1"}
- POST /abort     {"txid":"TX1"}

3PC endpoints (bonus scaffold):
- POST /can_commit {"txid":"TX1","op":{...}} -> {"vote":"YES"/"NO"}
- POST /precommit  {"txid":"TX1"}

GET:
- /status
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import argparse
import json
import os
import threading
import time
from typing import Dict, Any, Optional

lock = threading.Lock()

NODE_ID: str = ""
PORT: int = 8001

kv: Dict[str, str] = {}
TX: Dict[str, Dict[str, Any]] = {}

WAL_PATH: Optional[str] = None

def jdump(obj: Any) -> bytes:
    return json.dumps(obj).encode("utf-8")

def jload(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))

def wal_append(line: str) -> None:
    if not WAL_PATH:
        return
    data = line.rstrip("\n") + "\n"
    with open(WAL_PATH, "a", encoding="utf-8") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())

def wal_replay() -> None:
    """Rebuild TX table and kv store from the WAL if it exists."""
    if not WAL_PATH or not os.path.exists(WAL_PATH):
        return
    try:
        with open(WAL_PATH, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line:
                    continue
                parts = line.split(" ", 3)
                if len(parts) < 2:
                    continue
                txid, action = parts[0], parts[1]
                if action in ("PREPARE", "CAN_COMMIT"):
                    if len(parts) < 4:
                        continue
                    vote = parts[2]
                    try:
                        op = json.loads(parts[3])
                    except json.JSONDecodeError:
                        continue
                    state = "READY" if vote == "YES" else "ABORTED"
                    TX[txid] = {"state": state, "op": op, "ts": time.time()}
                elif action == "PRECOMMIT":
                    rec = TX.setdefault(txid, {"state": "PRECOMMIT", "op": None, "ts": time.time()})
                    rec["state"] = "PRECOMMIT"
                elif action == "COMMIT":
                    rec = TX.get(txid)
                    if rec and rec.get("op") and rec.get("state") != "COMMITTED":
                        apply_op(rec["op"])
                    if not rec:
                        TX[txid] = {"state": "COMMITTED", "op": None, "ts": time.time()}
                    else:
                        rec["state"] = "COMMITTED"
                elif action == "ABORT":
                    rec = TX.get(txid)
                    if not rec:
                        TX[txid] = {"state": "ABORTED", "op": None, "ts": time.time()}
                    else:
                        rec["state"] = "ABORTED"
    except FileNotFoundError:
        return

def validate_op(op: dict) -> bool:
    t = str(op.get("type", "")).upper()
    if t != "SET":
        return False
    if not str(op.get("key", "")).strip():
        return False
    return True

def apply_op(op: dict) -> None:
    t = str(op.get("type", "")).upper()
    if t == "SET":
        k = str(op["key"])
        v = str(op.get("value", ""))
        kv[k] = v
        return
    # YOUR CODE HERE (optional): add DEL/INCR/TRANSFER etc.

class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, obj: dict):
        data = jdump(obj)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/status"):
            with lock:
                self._send(200, {"ok": True, "node": NODE_ID, "port": PORT, "kv": kv, "tx": TX, "wal": WAL_PATH})
            return
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = jload(raw)
        except Exception:
            self._send(400, {"ok": False, "error": "invalid json"})
            return

        if self.path == "/prepare":
            txid = str(body.get("txid", "")).strip()
            op = body.get("op", None)
            if not txid or not isinstance(op, dict):
                self._send(400, {"ok": False, "error": "txid and op required"})
                return

            vote = "YES" if validate_op(op) else "NO"
            with lock:
                TX[txid] = {"state": "READY" if vote == "YES" else "ABORTED", "op": op, "ts": time.time()}
            wal_append(f"{txid} PREPARE {vote} {json.dumps(op)}")

            self._send(200, {"ok": True, "vote": vote, "state": TX[txid]["state"]})
            return

        if self.path == "/commit":
            txid = str(body.get("txid", "")).strip()
            if not txid:
                self._send(400, {"ok": False, "error": "txid required"})
                return

            with lock:
                rec = TX.get(txid)
                if not rec:
                    self._send(409, {"ok": False, "error": "unknown txid"})
                    return
                if rec["state"] not in ("READY", "PRECOMMIT"):
                    self._send(409, {"ok": False, "error": f"cannot commit from state={rec['state']}"})
                    return
                apply_op(rec["op"])
                rec["state"] = "COMMITTED"
            wal_append(f"{txid} COMMIT")

            self._send(200, {"ok": True, "txid": txid, "state": "COMMITTED"})
            return

        if self.path == "/abort":
            txid = str(body.get("txid", "")).strip()
            if not txid:
                self._send(400, {"ok": False, "error": "txid required"})
                return
            with lock:
                rec = TX.get(txid)
                if rec:
                    rec["state"] = "ABORTED"
                else:
                    TX[txid] = {"state": "ABORTED", "op": None, "ts": time.time()}
            wal_append(f"{txid} ABORT")

            self._send(200, {"ok": True, "txid": txid, "state": "ABORTED"})
            return

        if self.path == "/can_commit":
            txid = str(body.get("txid", "")).strip()
            op = body.get("op", None)
            if not txid or not isinstance(op, dict):
                self._send(400, {"ok": False, "error": "txid and op required"})
                return
            vote = "YES" if validate_op(op) else "NO"
            with lock:
                TX[txid] = {"state": "READY" if vote == "YES" else "ABORTED", "op": op, "ts": time.time()}
            wal_append(f"{txid} CAN_COMMIT {vote} {json.dumps(op)}")
            self._send(200, {"ok": True, "vote": vote, "state": TX[txid]["state"]})
            return

        if self.path == "/precommit":
            txid = str(body.get("txid", "")).strip()
            if not txid:
                self._send(400, {"ok": False, "error": "txid required"})
                return
            with lock:
                rec = TX.get(txid)
                if not rec or rec["state"] != "READY":
                    self._send(409, {"ok": False, "error": "precommit requires READY state"})
                    return
                rec["state"] = "PRECOMMIT"
            wal_append(f"{txid} PRECOMMIT")
            self._send(200, {"ok": True, "txid": txid, "state": "PRECOMMIT"})
            return

        self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, fmt, *args):
        return

def main():
    global NODE_ID, PORT, WAL_PATH
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8001)
    ap.add_argument("--wal", default="", help="Optional WAL path (/tmp/participant_B.wal)")
    args = ap.parse_args()

    NODE_ID = args.id
    PORT = args.port
    WAL_PATH = args.wal.strip() or None

    wal_replay()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"[{NODE_ID}] Participant listening on {args.host}:{args.port} wal={WAL_PATH}")
    server.serve_forever()

if __name__ == "__main__":
    main()
