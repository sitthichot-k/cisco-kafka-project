# Cisco Kafka Project

Send commands to Cisco routers in GNS3 from a web page. Every request passes
through Kafka, and a worker applies it to the routers over SSH with paramiko.

```
                          cisco-commands                         SSH (paramiko)
 ┌──────────┐  HTTP  ┌─────────┐  ─────►  ┌───────┐  ─────►  ┌────────┐  ─────►  ┌─────── GNS3 ───────┐
 │ Frontend │ ─────► │ Backend │          │ Kafka │          │ Worker │          │       R1    R2       │
 │ (nginx)  │ ◄───── │ (Flask) │  ◄─────  │       │  ◄─────  │        │  ◄─────  │                    │
 └──────────┘  poll  └─────────┘          └───────┘          └────────┘          └────────────────────┘
                          cisco-results
```

The forward path (top row) follows the lab diagram. The return path through
the `cisco-results` topic is an addition that lets the web page show command
output. The worker never calls the backend directly; Kafka is the only link
between them.

## Folder structure

```
.
├── docker-compose.yml     # whole stack
├── config/
│   └── routers.json       # router names, IPs, credentials (shared by backend + worker)
├── frontend/              # static HTML/JS served by nginx; nginx proxies /api to backend
│   ├── Dockerfile
│   ├── nginx.conf
│   └── html/              # index.html, app.js, style.css
├── backend/               # Flask API: frontend -> Kafka, tracks job status from results
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py
└── worker/                # Kafka consumer -> paramiko SSH -> routers
    ├── Dockerfile
    ├── requirements.txt
    ├── worker.py          # Kafka loop, publishes status/output
    └── cisco_ssh.py       # SSH session to IOS (enable, terminal length 0, config mode)
```

## Services and ports

| Service    | URL on the Ubuntu VM         | Purpose                                   |
|------------|------------------------------|-------------------------------------------|
| frontend   | http://<vm-ip>:7000          | Web UI                                    |
| backend    | http://<vm-ip>:7001/api/...  | REST API (also reachable via frontend)    |
| kafka-ui   | http://<vm-ip>:7080          | Browse topics and messages                |
| kafka      | localhost:7092 (VM only)     | Broker listener for tools on the VM       |
| worker     | (none)                       | Runs jobs; scale with `--scale worker=N`  |
| kafka-init | (runs once, then exits)      | Creates the two topics with 3 partitions  |

## Message flow

1. The user picks a router, a mode, and commands in the web page.
2. `POST /api/jobs` → the backend publishes a job to `cisco-commands`, keyed by
   router name, and marks it `queued`.
3. A worker in the consumer group `cisco-workers` picks up the job and publishes
   `running` to `cisco-results`.
4. The worker SSHes to the router, enters `enable` if needed, sets
   `terminal length 0`, and runs the commands. In `config` mode it wraps them in
   `configure terminal` … `end`.
5. The worker publishes `success` with the transcript, or `failed` with the
   error, to `cisco-results`.
6. The backend consumes `cisco-results` and updates the job. The page polls
   `GET /api/jobs` every 2 seconds.

Keying by router puts all jobs for one router on the same partition, so they
run in order even when several workers are running.

Job message (`cisco-commands`):

```json
{"job_id": "a1b2c3d4e5f6", "router": "R1", "mode": "exec",
 "commands": ["show ip interface brief"], "submitted_at": "2026-09-30T10:00:00+00:00"}
```

Result message (`cisco-results`): the same fields, plus `status`
(`running` / `success` / `failed`), `output`, `error`, `worker`,
`started_at`, and `finished_at`.

## Running on the Ubuntu server (VMware)

Only Docker and git are installed on the server. Python, paramiko,
kafka-python, and Kafka itself all run inside containers built by
`docker compose`. Do not install them on the host.

Suggested VM size: 2 vCPU, 4 GB RAM, 20 GB disk. Kafka alone uses about 1 GB.

### 1. Base packages

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y ca-certificates curl git
```

### 2. Docker Engine and the compose plugin

From the official Docker apt repository
(https://docs.docker.com/engine/install/ubuntu/):

```bash
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
  https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Run docker without sudo, then log out and back in (or run: newgrp docker)
sudo usermod -aG docker $USER
```

Check:

```bash
docker run --rm hello-world
docker compose version
```

### 3. Get the project

```bash
git clone https://github.com/sitthichot-k/cisco-kafka-project.git
cd cisco-kafka-project
```

### 4. Check the VM can reach the routers

Get each router's address with `show ip interface brief` in GNS3, then from the VM:

```bash
ping <R1-ip>
ssh admin@<R1-ip>        # password: cisco
```

If `ping` fails, fix the GNS3 / VMware network first. The containers reach the
routers through the VM's own routing, so if the VM cannot reach them, the
worker cannot either.

If `ping` works but `ssh` fails with `no matching key exchange method` or
`no matching host key type`, the router only offers old algorithms. For a
manual test, allow them explicitly:

```bash
ssh -o KexAlgorithms=+diffie-hellman-group1-sha1,diffie-hellman-group14-sha1 \
    -o HostKeyAlgorithms=+ssh-rsa -o PubkeyAcceptedAlgorithms=+ssh-rsa \
    admin@<R1-ip>
```

### 5. Point the config at the routers

Edit `config/routers.json` and set `host` for R1 and R2 to the addresses from
step 4. The file is read on every job, so later edits do not need a restart.

```bash
nano config/routers.json
```

### 6. Start the stack

```bash
docker compose up -d --build
docker compose ps          # kafka should be "healthy", kafka-init "Exited (0)"
```

The first build downloads images and Python packages, which takes a few minutes.

### 7. Test SSH from the worker, then open the web page

```bash
docker compose exec worker python cisco_ssh.py R1 "show ip interface brief"
```

Find the VM's IP with `ip -4 addr` and open `http://<vm-ip>:7000` from the
Windows host. If `ufw` is active, open the ports first:

```bash
sudo ufw allow 7000,7001,7080/tcp
```

### Updating and stopping

```bash
git pull && docker compose up -d --build   # after pulling new code
docker compose down                         # stop, keep Kafka data
docker compose down -v                      # stop and delete Kafka data / job history
```

### Router side (IOS) config used in this lab

```
enable
conf t
int fa0/0
 no shut
 ip add dhcp
hostname R1
ip domain-name test.com
username admin privilege 15
username admin password cisco
crypto key generate rsa modulus 1024
line vty 0 4
 login local
 transport input all
end
wr
```

Do the same on R2 with `hostname R2`. Then:

- Run `show ip interface brief` on each router and put the Fa0/0 address into
  `host` in `config/routers.json`. The IPs in the repo are placeholders.
- Fa0/0 gets its address from DHCP, so it can change after a router or GNS3
  restart. If a job suddenly times out, check the IP again first.
- `privilege 15` makes the SSH session start at `R1#`, so no enable secret is
  needed. If a router ever lands at `R1>`, add `"enable_secret"` to that
  router's entry in `routers.json`.

## Testing and troubleshooting

Test SSH to a router without Kafka, from inside the worker container:

```bash
docker compose exec worker python cisco_ssh.py R1 "show ip interface brief"
```

Send a job with curl, without the web page:

```bash
curl -X POST http://localhost:7001/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"router": "R1", "mode": "exec", "commands": ["show ip interface brief"]}'
curl http://localhost:7001/api/jobs
```

Logs:

```bash
docker compose logs -f worker backend
```

- **Job stays `queued`:** no worker is consuming. Check `docker compose logs worker`.
- **`SSH connect ... failed`:** the container cannot reach the router, or the
  credentials are wrong. Run the `cisco_ssh.py` test above, and `ping` from the VM.
- **SSH algorithm negotiation errors** (`Incompatible ssh peer`, `no acceptable
  kex/cipher`): old IOS images only offer legacy algorithms. The error message
  names the algorithms that did not match. Try `ip ssh version 2` and a
  2048-bit key on the router.
- **`router rejected a command`:** IOS answered `% Invalid input` (or similar).
  The transcript in the error shows which line failed.

Scale workers:

```bash
docker compose up -d --scale worker=3
```

Job history lives in the backend's memory. It is rebuilt from `cisco-results`
when the backend restarts, as long as the `kafka-data` volume exists.
`docker compose down -v` deletes that volume.
