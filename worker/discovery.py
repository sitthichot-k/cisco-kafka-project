"""Find Cisco routers by hostname, so router IPs from DHCP need no config.

Scans subnets for open SSH ports, logs in with the default credentials, and
reads the IOS prompt ("R1#") to learn which router answers at which IP.

Runnable on its own:
    python discovery.py                  # scan the subnets from routers.json
    python discovery.py 192.168.1.0/24   # scan a given subnet
"""

import ipaddress
import socket
import sys
from concurrent.futures import ThreadPoolExecutor

import psutil

from cisco_ssh import RouterError, load_config, read_hostname

PORT_TIMEOUT = 0.5
MAX_HOSTS = 1024
# Docker's own bridges and loopback never hold GNS3 routers.
SKIP_IFACE_PREFIXES = ("lo", "docker", "br-", "veth")


def local_subnets():
    """IPv4 subnets of this machine's interfaces (the VM's, when the worker
    runs with network_mode: host), and the machine's own addresses."""
    subnets, own = set(), set()
    for iface, addrs in psutil.net_if_addrs().items():
        if iface.startswith(SKIP_IFACE_PREFIXES):
            continue
        for addr in addrs:
            if addr.family == socket.AF_INET and addr.netmask:
                own.add(addr.address)
                subnets.add(ipaddress.ip_network(f"{addr.address}/{addr.netmask}", strict=False))
    return sorted(subnets, key=str), own


def resolve_subnets(setting):
    if setting in (None, "", "auto"):
        return local_subnets()
    if isinstance(setting, str):
        setting = [setting]
    return [ipaddress.ip_network(s, strict=False) for s in setting], set()


def port_open(host, port):
    try:
        with socket.create_connection((host, port), timeout=PORT_TIMEOUT):
            return True
    except OSError:
        return False


def discover(subnet_setting=None):
    """Return (routers, report). routers is a list of {name, host}."""
    config = load_config()
    defaults = config.get("defaults", {})
    port = int(defaults.get("port", 22))
    if subnet_setting is None:
        subnet_setting = config.get("discovery", {}).get("subnets", "auto")
    subnets, own = resolve_subnets(subnet_setting)

    report = []
    candidates = []
    for net in subnets:
        if net.num_addresses > MAX_HOSTS:
            report.append(f"skip {net}: larger than {MAX_HOSTS} addresses, list a smaller subnet in routers.json")
            continue
        hosts = [str(h) for h in (net.hosts() if net.prefixlen < 31 else net)]
        candidates += [h for h in hosts if h not in own]
        report.append(f"scan {net}")

    with ThreadPoolExecutor(max_workers=128) as pool:
        open_hosts = [h for h, ok in zip(candidates, pool.map(lambda h: port_open(h, port), candidates)) if ok]
    report.append(f"SSH open on: {', '.join(open_hosts) or 'none'}")

    def identify(host):
        try:
            return host, read_hostname({**defaults, "host": host}), None
        except RouterError as exc:
            return host, None, str(exc).splitlines()[0]

    routers, seen = [], {}
    with ThreadPoolExecutor(max_workers=16) as pool:
        for host, name, error in pool.map(identify, open_hosts):
            if name is None:
                report.append(f"{host}: not a router we can log into ({error})")
            elif name in seen:
                report.append(f"{host}: hostname {name} already found at {seen[name]}, skipped")
            else:
                seen[name] = host
                routers.append({"name": name, "host": host})
                report.append(f"{host}: {name}")

    routers.sort(key=lambda r: r["name"])
    return routers, report


if __name__ == "__main__":
    found, lines = discover(sys.argv[1:] or None)
    print("\n".join(lines))
    print(f"\nfound {len(found)} router(s): " + ", ".join(f"{r['name']}={r['host']}" for r in found))
