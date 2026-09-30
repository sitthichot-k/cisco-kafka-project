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

Router IPs are **not configured anywhere**. The routers get their addresses
from DHCP, so every machine running this lab ends up with different IPs. The
worker finds the routers itself (see [Router discovery](#router-discovery)),
so the project runs after `git clone` without editing any file.

## Folder structure

```
.
├── docker-compose.yml     # whole stack
├── config/
│   └── routers.json       # SSH credentials, discovery settings, optional manual IPs
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
    ├── discovery.py       # finds routers by hostname on the VM's subnets
    └── cisco_ssh.py       # SSH session to IOS (enable, terminal length 0, config mode)
```

## Services and ports

| Service    | URL on the Ubuntu VM         | Purpose                                   |
|------------|------------------------------|-------------------------------------------|
| frontend   | http://<vm-ip>:7000          | Web UI                                    |
| backend    | http://<vm-ip>:7001/api/...  | REST API (also reachable via frontend)    |
| kafka-ui   | http://<vm-ip>:7080          | Browse topics and messages                |
| kafka      | localhost:7092 (VM only)     | Broker listener for tools on the VM       |
| worker     | (none, host network)         | Runs jobs; scale with `--scale worker=N`  |
| kafka-init | (runs once, then exits)      | Creates the two topics with 3 partitions  |

## Router discovery

1. A few seconds after the backend starts, then every `interval_seconds`
   (default 300), or when the **Scan network** button is pressed, the backend
   publishes a `discover` job to `cisco-commands`.
2. The worker scans the subnets of the VM's own network interfaces (Docker
   bridges and loopback are skipped) for hosts with port 22 open.
3. It logs into each one with the credentials in `config/routers.json`
   (`admin` / `cisco`) and reads the IOS prompt: `R1#` means that IP is R1.
4. The list of routers found is published to `cisco-results`. The backend keeps
   the latest list and shows it in the router drop-down.

The worker runs with `network_mode: host` so it can see the VM's real
interfaces, and so it reaches the routers the same way the VM does.

Before running any job, the worker checks that the prompt shows the expected
hostname. If DHCP has moved R1's old IP to R2, the job fails with
"IPs changed, scan again" instead of running on the wrong router.

Discovery only works when the routers are on a subnet the VM is directly
attached to (for example a GNS3 Cloud node on the same VMware network). If
they are elsewhere but routable, list the subnets to scan in
`config/routers.json`:

```json
"discovery": { "subnets": ["192.168.50.0/24"], "interval_seconds": 300 }
```

Or skip discovery for a router by giving its IP directly. Entries in
`"routers"` win over discovered ones with the same name:

```json
"routers": [ { "name": "R1", "host": "192.168.50.10" } ]
```

Discovery tries the lab credentials on every host with port 22 open in the
scanned subnets. That is fine on a lab network; do not point it at a real one.

## Message flow

1. The user picks a router, a mode, and commands in the web page.
2. `POST /api/jobs` → the backend looks up the router's IP, publishes a job to
   `cisco-commands`, keyed by router name, and marks it `queued`.
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
{"job_id": "a1b2c3d4e5f6", "router": "R1", "host": "192.168.50.10", "mode": "exec",
 "commands": ["show ip interface brief"], "submitted_at": "2026-09-30T10:00:00+00:00"}
```

Result message (`cisco-results`): the same fields, plus `status`
(`running` / `success` / `failed`), `output`, `error`, `worker`,
`started_at`, and `finished_at`. A finished `discover` job also carries
`routers`: `[{"name": "R1", "host": "..."}, ...]`.

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

`apt upgrade` may open blue "Package configuration" screens, for example
`keyboard-configuration` asking for a keyboard layout, or a list of services
to restart. Press Enter to accept the defaults. The keyboard layout only
affects typing on the VMware console, not SSH sessions.

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

You do not need the router IPs in any file, but check the network once. Get a
router's current address with `show ip interface brief` in GNS3, then from the VM:

```bash
ping <R1-ip>
ssh admin@<R1-ip>        # password: cisco
```

If `ping` fails, fix the GNS3 / VMware network first. The containers reach the
routers through the VM's own routing, so if the VM cannot reach them, the
worker cannot either.

If `ssh` fails with `no matching key exchange method found`, the network is
fine: the router answered, but it only offers SHA1 key exchange
(`diffie-hellman-group-exchange-sha1`, `group14-sha1`, `group1-sha1`), which
current OpenSSH on Ubuntu disables. For a manual test, allow them explicitly:

```bash
ssh -o KexAlgorithms=+diffie-hellman-group14-sha1 -o HostKeyAlgorithms=+ssh-rsa admin@<R1-ip>
# if it then says "no matching cipher found", also add: -o Ciphers=+aes128-cbc
```

The worker does not need this. paramiko 3.5.x, pinned by `paramiko<4`, enables
all three SHA1 key exchanges, `ssh-rsa`, and the CBC ciphers by default.

### 5. Start the stack

```bash
docker compose up -d --build
docker compose ps          # kafka should be "healthy", kafka-init "Exited (0)"
```

The first build downloads images and Python packages, which takes a few minutes.

### 6. Check discovery, then open the web page

```bash
docker compose exec worker python discovery.py
```

This prints the subnets scanned, the hosts with SSH open, and which router was
found at which IP. It should end with something like
`found 2 router(s): R1=..., R2=...`.

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

- The hostname is how discovery tells the routers apart, so every router needs
  a unique one. The username and password must match `config/routers.json`.
- Fa0/0 gets its address from DHCP, so it can change after a router or GNS3
  restart. Press **Scan network** (or wait for the periodic scan) afterwards.
- `privilege 15` makes the SSH session start at `R1#`, so no enable secret is
  needed. If a router ever lands at `R1>`, add `"enable_secret"` to that
  router's entry in `routers.json`.

## Testing and troubleshooting

Test discovery and SSH without Kafka, from inside the worker container:

```bash
docker compose exec worker python discovery.py                  # subnets from routers.json
docker compose exec worker python discovery.py 192.168.50.0/24  # a specific subnet
docker compose exec worker python cisco_ssh.py <router-ip> "show ip interface brief"
```

Send a job with curl, without the web page:

```bash
curl -X POST http://localhost:7001/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"router": "R1", "mode": "exec", "commands": ["show ip interface brief"]}'
curl http://localhost:7001/api/jobs
curl -X POST http://localhost:7001/api/discover   # rescan
curl http://localhost:7001/api/routers
```

Logs:

```bash
docker compose logs -f worker backend
```

- **Job stays `queued`:** no worker is consuming. Check `docker compose logs worker`.
- **Router drop-down is empty:** open the "Network scan" job in the job list.
  Its output shows which subnets were scanned and what each SSH host answered.
  If the routers' subnet is missing, set `discovery.subnets` (see above).
- **`IPs changed, scan again`:** DHCP moved the addresses. Press **Scan network**.
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
