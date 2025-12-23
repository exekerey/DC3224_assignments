import json
import socket
import threading
import time
import uuid
from datetime import datetime, timezone

HOST = "0.0.0.0"
PORT = 5000

def log(msg):
    ts = datetime.now(timezone.utc).isoformat()
    print(f"[{ts}] {msg}", flush=True)

def handle_add(params):
    a = params.get("a")
    b = params.get("b")
    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        raise ValueError("add expects numeric params a and b")
    return a + b

def handle_get_time(params):
    return datetime.now(timezone.utc).isoformat()

def handle_reverse_string(params):
    s = params.get("s")
    if not isinstance(s, str):
        raise ValueError("reverse_string expects string param s")
    return s[::-1]

METHODS = {
    "add": handle_add,
    "get_time": handle_get_time,
    "reverse_string": handle_reverse_string,
}

def make_response(request_id, status, result=None, error=None):
    resp = {
        "request_id": request_id,
        "status": status,
        "result": result,
        "error": error,
        "server_time": datetime.now(timezone.utc).isoformat(),
    }
    return (json.dumps(resp) + "\n").encode("utf-8")

def process_request(line_bytes):
    try:
        req = json.loads(line_bytes.decode("utf-8"))
    except Exception:
        request_id = str(uuid.uuid4())
        return make_response(request_id, "error", error="invalid_json")

    request_id = req.get("request_id") or str(uuid.uuid4())
    method = req.get("method")
    params = req.get("params") or {}

    log(f"REQ id={request_id} method={method} params={params}")

    if method not in METHODS:
        resp = make_response(request_id, "error", error="unknown_method")
        log(f"RES id={request_id} status=error error=unknown_method")
        return resp

    try:
        result = METHODS[method](params)
        resp = make_response(request_id, "ok", result=result)
        log(f"RES id={request_id} status=ok result={result}")
        return resp
    except Exception as e:
        resp = make_response(request_id, "error", error=str(e))
        log(f"RES id={request_id} status=error error={str(e)}")
        return resp

def client_thread(conn, addr):
    log(f"CONNECT from {addr}")
    buf = b""
    try:
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line.strip():
                    continue
                resp = process_request(line)
                conn.sendall(resp)
    except Exception as e:
        log(f"CONN_ERROR from {addr}: {e}")
    finally:
        conn.close()
        log(f"DISCONNECT from {addr}")

def main():
    log(f"Starting RPC server on {HOST}:{PORT}")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen(100)
    while True:
        conn, addr = s.accept()
        t = threading.Thread(target=client_thread, args=(conn, addr), daemon=True)
        t.start()

if __name__ == "__main__":
    main()
