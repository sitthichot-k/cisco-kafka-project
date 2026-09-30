"""SSH into a Cisco IOS router with paramiko and run exec or config commands.

Runnable on its own to test SSH without Kafka:
    python cisco_ssh.py Router1 "show ip interface brief"
"""

import json
import os
import re
import sys
import time

import paramiko

ROUTERS_FILE = os.environ.get("ROUTERS_FILE", "/config/routers.json")
CONNECT_TIMEOUT = 10
COMMAND_TIMEOUT = 30

# Matches "R1>", "R1#", "R1(config)#", "R1(config-if)#" at the very end of the
# buffer, so a prompt-like line in the middle of output does not end the read.
PROMPT_RE = re.compile(r"(?:^|\n)[\w.\-]+(\([\w.\-]+\))?[>#] ?\Z")
PASSWORD_RE = re.compile(r"(?i)password: ?\Z")
IOS_ERROR_RE = re.compile(r"(?m)^% (Invalid input|Incomplete command|Ambiguous command|Unknown command)")


class RouterError(Exception):
    pass


def find_router(name):
    with open(ROUTERS_FILE, encoding="utf-8") as f:
        config = json.load(f)
    for router in config["routers"]:
        if router["name"] == name:
            return {**config.get("defaults", {}), **router}
    raise RouterError(f"router {name!r} is not in {ROUTERS_FILE}")


def _read_until(shell, pattern, timeout):
    buf = ""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if shell.recv_ready():
            buf += shell.recv(65535).decode("utf-8", errors="replace")
            if pattern.search(buf):
                return buf
        else:
            time.sleep(0.1)
    raise RouterError(f"timed out after {timeout}s waiting for router; last output:\n{buf[-500:]}")


def _send(shell, line, pattern=PROMPT_RE, timeout=COMMAND_TIMEOUT):
    shell.send(line + "\n")
    return _read_until(shell, pattern, timeout)


def run_commands(router, commands, mode="exec"):
    """Return the session transcript. Raises RouterError on connection
    problems, timeouts, or when IOS rejects a command."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(
            hostname=router["host"],
            port=int(router.get("port", 22)),
            username=router["username"],
            password=router["password"],
            look_for_keys=False,
            allow_agent=False,
            timeout=CONNECT_TIMEOUT,
            banner_timeout=CONNECT_TIMEOUT,
            auth_timeout=CONNECT_TIMEOUT,
        )
    except Exception as exc:
        raise RouterError(f"SSH connect to {router['host']} failed: {type(exc).__name__}: {exc}") from exc

    try:
        shell = ssh.invoke_shell(width=512)
        prompt = _read_until(shell, PROMPT_RE, CONNECT_TIMEOUT).strip().splitlines()[-1]

        if prompt.endswith(">"):
            secret = router.get("enable_secret")
            if not secret:
                raise RouterError("router is in user EXEC mode and no enable_secret is configured")
            _send(shell, "enable", PASSWORD_RE)
            prompt = _send(shell, secret).strip().splitlines()[-1]
            if not prompt.endswith("#"):
                raise RouterError("enable failed: check enable_secret")

        _send(shell, "terminal length 0")

        lines = ["configure terminal", *commands, "end"] if mode == "config" else commands
        transcript = prompt
        for line in lines:
            transcript += _send(shell, line)
        transcript = transcript.replace("\r\n", "\n").replace("\r", "")

        if IOS_ERROR_RE.search(transcript):
            raise RouterError("router rejected a command:\n" + transcript)
        return transcript
    finally:
        ssh.close()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit('usage: python cisco_ssh.py <router-name> "<command>" ["<command>" ...]')
    print(run_commands(find_router(sys.argv[1]), sys.argv[2:]))
