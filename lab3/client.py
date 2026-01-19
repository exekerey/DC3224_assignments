import argparse
import json
from urllib import request as urllib_request


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--command", required=True)
    args = parser.parse_args()
    url = f"http://{args.host}:{args.port}/client_command"
    payload = {"command": args.command}
    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    req = urllib_request.Request(url, data=data, headers=headers, method="POST")
    with urllib_request.urlopen(req) as resp:
        body = resp.read().decode("utf-8")
        print(body)


if __name__ == "__main__":
    main()
