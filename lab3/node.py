import argparse
import json
import threading
import time
import random
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib import request as urllib_request
from urllib.error import URLError


NODE_INSTANCE = None


class RaftNode:
    def __init__(self, node_id, host, port, peers):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        self.state = "Follower"
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = -1
        self.last_applied = -1
        self.state_machine = {}
        self.leader_id = None
        self.lock = threading.Lock()
        self.election_deadline = self._new_election_deadline()
        self.heartbeat_interval = 1.0
        self.next_heartbeat_time = time.time() + self.heartbeat_interval

    def _new_election_deadline(self):
        timeout = random.uniform(1.5, 3.0)
        return time.time() + timeout

    def _http_post(self, peer, path, payload):
        url = f"http://{peer}{path}"
        data = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        req = urllib_request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib_request.urlopen(req, timeout=1.0) as resp:
                resp_data = resp.read().decode("utf-8")
                if resp_data:
                    return json.loads(resp_data)
                return {}
        except URLError:
            return None

    def handle_request_vote(self, data):
        with self.lock:
            term = data.get("term", 0)
            candidate_id = data.get("candidateId")
            cand_last_index = data.get("lastLogIndex", -1)
            cand_last_term = data.get("lastLogTerm", 0)

            if term < self.current_term:
                return {"term": self.current_term, "voteGranted": False}

            if term > self.current_term:
                self.current_term = term
                self.state = "Follower"
                self.voted_for = None
                self.leader_id = None

            own_last_index = len(self.log) - 1
            own_last_term = self.log[own_last_index]["term"] if own_last_index >= 0 else 0

            log_ok = cand_last_term > own_last_term or (
                cand_last_term == own_last_term and cand_last_index >= own_last_index
            )

            vote_granted = False
            if log_ok and (self.voted_for is None or self.voted_for == candidate_id):
                self.voted_for = candidate_id
                self.election_deadline = self._new_election_deadline()
                vote_granted = True

            return {"term": self.current_term, "voteGranted": vote_granted}

    def handle_append_entries(self, data):
        with self.lock:
            term = data.get("term", 0)
            leader_id = data.get("leaderId")
            entries = data.get("entries")
            leader_commit = data.get("leaderCommit", -1)
            is_heartbeat = data.get("isHeartbeat", False)

            if term < self.current_term:
                return {"term": self.current_term, "success": False}

            if term > self.current_term or self.state != "Follower":
                self.current_term = term
                self.state = "Follower"
                self.voted_for = None

            self.leader_id = leader_id
            self.election_deadline = self._new_election_deadline()

            if is_heartbeat:
                print(f"[Node {self.node_id}] heartbeat sync from {leader_id} (term {self.current_term})")
            else:
                prev_len = len(self.log)
                if entries is not None:
                    self.log = entries
                    new_len = len(self.log)
                    print(f"[Node {self.node_id}] replicated log from leader {leader_id} (len={new_len})")
                    if new_len > prev_len:
                        start = prev_len
                        end = new_len - 1
                        print(f"[Node {self.node_id}] catch-up applied entries {start}-{end}")

            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log) - 1)
                self._apply_committed_entries()

            return {"term": self.current_term, "success": True}

    def handle_client_command(self, data):
        command = data.get("command")
        if command is None:
            return {"error": "missing command"}

        upper = command.strip().upper()
        if upper.startswith("GET"):
            with self.lock:
                return {
                    "state": self.state_machine,
                    "role": self.state,
                    "term": self.current_term,
                    "logLength": len(self.log),
                }

        with self.lock:
            if self.state != "Leader":
                if self.leader_id is None:
                    return {"error": "no leader"}
                return {"error": "not leader", "leaderId": self.leader_id}

            entry = {"term": self.current_term, "command": command}
            self.log.append(entry)
            index = len(self.log) - 1
            print(f"[Leader {self.node_id}] received client command '{command}', appended at index {index}")

        success_count = 1
        for peer in self.peers:
            payload = {
                "term": self.current_term,
                "leaderId": self.node_id,
                "entries": self.log,
                "leaderCommit": self.commit_index,
                "isHeartbeat": False,
            }
            resp = self._http_post(peer, "/append_entries", payload)
            if resp and resp.get("success"):
                success_count += 1
                print(f"[Leader {self.node_id}] follower {peer} acked index {index}")

        with self.lock:
            cluster_size = len(self.peers) + 1
            majority = cluster_size // 2 + 1
            if success_count >= majority:
                self.commit_index = index
                self._apply_committed_entries()
                print(f"[Leader {self.node_id}] entry committed (index={self.commit_index}, majority={success_count}/{cluster_size})")
                return {"status": "committed", "index": index}
            print(f"[Leader {self.node_id}] entry pending (index={index}, acks={success_count}/{cluster_size})")
            return {"status": "pending", "index": index}

    def _apply_committed_entries(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            if self.last_applied < 0 or self.last_applied >= len(self.log):
                break
            entry = self.log[self.last_applied]
            cmd = entry["command"]
            parts = cmd.strip().split()
            if len(parts) == 4 and parts[0].upper() == "SET" and parts[2] == "=":
                key = parts[1]
                try:
                    value = int(parts[3])
                except ValueError:
                    value = parts[3]
                self.state_machine[key] = value
                print(f"[Node {self.node_id}] apply index={self.last_applied} cmd='{cmd}' -> {key}={value}")

    def start_election_if_needed(self):
        with self.lock:
            now = time.time()
            if self.state == "Leader":
                return
            if now < self.election_deadline:
                return

            self.state = "Candidate"
            self.current_term += 1
            self.voted_for = self.node_id
            term = self.current_term
            print(f"[Node {self.node_id}] timeout -> Candidate (term {term})")

            cand_last_index = len(self.log) - 1
            cand_last_term = self.log[cand_last_index]["term"] if cand_last_index >= 0 else 0
            peers = list(self.peers)

        votes = 1
        for peer in peers:
            payload = {
                "term": term,
                "candidateId": self.node_id,
                "lastLogIndex": cand_last_index,
                "lastLogTerm": cand_last_term,
            }
            resp = self._http_post(peer, "/request_vote", payload)
            if not resp:
                continue
            peer_term = resp.get("term", 0)
            if peer_term > term:
                with self.lock:
                    if peer_term > self.current_term:
                        self.current_term = peer_term
                        self.state = "Follower"
                        self.voted_for = None
                        self.leader_id = None
                        self.election_deadline = self._new_election_deadline()
                return
            if resp.get("voteGranted"):
                votes += 1

        with self.lock:
            cluster_size = len(self.peers) + 1
            majority = cluster_size // 2 + 1
            if self.state == "Candidate" and votes >= majority:
                self.state = "Leader"
                self.leader_id = self.node_id
                self.next_heartbeat_time = time.time()
                print(f"[Node {self.node_id}] received majority votes -> Leader (term {self.current_term})")
            else:
                self.election_deadline = self._new_election_deadline()

    def send_heartbeats_if_leader(self):
        with self.lock:
            if self.state != "Leader":
                return
            now = time.time()
            if now < self.next_heartbeat_time:
                return
            self.next_heartbeat_time = now + self.heartbeat_interval
            term = self.current_term
            peers = list(self.peers)
            leader_commit = self.commit_index

        for peer in peers:
            payload = {
                "term": term,
                "leaderId": self.node_id,
                "entries": None,
                "leaderCommit": leader_commit,
                "isHeartbeat": True,
            }
            self._http_post(peer, "/append_entries", payload)

    def run(self):
        while True:
            self.start_election_if_needed()
            self.send_heartbeats_if_leader()
            time.sleep(0.1)


class RaftRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            data = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            data = {}

        if self.path == "/request_vote":
            resp = NODE_INSTANCE.handle_request_vote(data)
        elif self.path == "/append_entries":
            resp = NODE_INSTANCE.handle_append_entries(data)
        elif self.path == "/client_command":
            resp = NODE_INSTANCE.handle_client_command(data)
        else:
            self.send_response(404)
            self.end_headers()
            return

        resp_bytes = json.dumps(resp).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(resp_bytes)))
        self.end_headers()
        self.wfile.write(resp_bytes)

    def log_message(self, format, *args):
        return


def start_http_server(host, port):
    server = HTTPServer((host, port), RaftRequestHandler)
    server.serve_forever()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peers", default="")
    args = parser.parse_args()
    peers = [p for p in args.peers.split(",") if p]

    global NODE_INSTANCE
    NODE_INSTANCE = RaftNode(args.id, args.host, args.port, peers)

    server_thread = threading.Thread(target=start_http_server, args=(args.host, args.port), daemon=True)
    server_thread.start()

    NODE_INSTANCE.run()


if __name__ == "__main__":
    main()
