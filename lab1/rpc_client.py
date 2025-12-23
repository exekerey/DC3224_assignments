import json
import os
import socket
import sys
import time
import uuid
from datetime import datetime, timezone

DEFAULT_HOST = "ip-172-31-28-54.ec2.internal"
DEFAULT_PORT = 5000

def rpc_call(host, port, method, params, timeout_s=2.0, retries=3, backoff_s=0.25):
    request_id = str(uuid.uuid4())
    req = {
        "request_id": request_id,
        "method": method,
        "params": params,
        "client_time": datetime.now(timezone.utc).isoformat(),
    }
    payload = (json.dumps(req) + "\n").encode("utf-8")

    last_err = None

    for attempt in range(1, retries + 1):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout_s)
            s.connect((host, port))
            s.sendall(payload)

            data = b""
            while b"\n" not in data:
                chunk = s.recv(4096)
                if not chunk:
                    raise RuntimeError("connection_closed")
                data += chunk

            line = data.split(b"\n", 1)[0]
            resp = json.loads(line.decode("utf-8"))

            if resp.get("request_id") != request_id:
                raise RuntimeError("mismatched_request_id")

            return resp
        except Exception as e:
            last_err = str(e)
            time.sleep(backoff_s * attempt)
        finally:
            try:
                s.close()
            except Exception:
                pass

    return {
        "request_id": request_id,
        "status": "error",
        "result": None,
        "error": f"failed_after_retries: {last_err}",
    }

def main():
    host = os.environ.get("RPC_HOST", DEFAULT_HOST)
    port = int(os.environ.get("RPC_PORT", str(DEFAULT_PORT)))

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 rpc_client.py <method> [json_params]")
        print("Examples:")
        print('  python3 rpc_client.py add \'{"a":5,"b":7}\'')
        print('  python3 rpc_client.py get_time \'{}\'')
        print('  python3 rpc_client.py reverse_string \'{"s":"takagi"}\'')
        sys.exit(1)

    method = sys.argv[1]
    params = json.loads(sys.argv[2]) if len(sys.argv) >= 3 else {}

    resp = rpc_call(host, port, method, params, timeout_s=2.0, retries=3)

    print(json.dumps(resp, indent=2))

    if resp.get("status") != "ok":
        sys.exit(2)

if __name__ == "__main__":
    main()
