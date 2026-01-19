# lab 3 cluster demo

minimal raft-style cluster with three nodes and a lightweight client.

## requirements
- python 3.9 or newer
- all nodes on the same network (ec2 private ips or localhost)
- no extra python dependencies

## start the cluster
run each node in its own terminal; they elect a leader automatically.

```bash
# terminal 1
python3 node.py --id A --host 0.0.0.0 --port 8000 --peers <node B private address>:8001,<node C private address>:8002

# terminal 2
python3 node.py --id B --host 0.0.0.0 --port 8001 --peers <node A private address>:8000,<node C private address>:8002

# terminal 3
python3 node.py --id C --host 0.0.0.0 --port 8002 --peers <node A private address>:8000,<node B private address>:8001
```

## client commands
```bash
# write via the leader
python3 client.py --host <leader ip> --port <leader port> --command "SET x = 5"

# read from any node
python3 client.py --host <node ip> --port <port> --command "GET"
```

## failure demos
### follower failure + recovery
```bash
pkill -f "node.py --id C"
python3 client.py --host <leader ip> --port <leader port> --command "SET z = 7"
python3 node.py --id C --host 0.0.0.0 --port 8002 --peers <node A private address>:8000,<node B private address>:8001
```
restarted follower catches up from the leader, and all nodes return the same state on `GET`.

### leader failure
```bash
pkill -f "node.py --id B"
python3 client.py --host <new leader ip> --port <port> --command "SET y = 10"
python3 client.py --host <node ip> --port <port> --command "GET"
```
remaining nodes elect a new leader automatically, and committed entries survive leader changes.
