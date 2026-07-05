# Port knocking for clinics behind a dynamic-IP firewall.
#
# The database server (faxcomet) keeps 3306 closed by default and
# runs a knock daemon (e.g. knockd) that briefly opens it to the
# SOURCE IP after seeing a secret sequence of packets. Because
# clinic public IPs are dynamic, the agent re-authorizes its
# current IP by knocking whenever a DB connection fails.
#
# The sequence is a shared secret, so it is NOT stored in this
# repo - it comes from the CPSO_KNOCK environment variable (set
# per machine at deploy time) or the --knock command-line option.
#
#   CPSO_KNOCK        "tcp:7001,8002,9003"  (proto optional,
#                     default tcp; ports in order)
#   CPSO_KNOCK_DELAY  seconds between knocks (default 0.3)

import os
import socket
import time


def knock_config_from_env() -> tuple:
    """(ports, proto, delay) parsed from CPSO_KNOCK /
    CPSO_KNOCK_DELAY. Empty ports list = knocking disabled."""
    raw = os.environ.get('CPSO_KNOCK', '').strip()
    proto = 'tcp'
    # optional "proto:" prefix
    if ':' in raw and not raw.split(':', 1)[0].strip().isdigit():
        proto, _, raw = raw.partition(':')
        proto = proto.strip().lower()

    ports = []
    for tok in raw.replace(';', ',').split(','):
        tok = tok.strip()
        if tok.isdigit():
            ports.append(int(tok))

    try:
        delay = float(os.environ.get('CPSO_KNOCK_DELAY', '0.3'))
    except ValueError:
        delay = 0.3

    return ports, proto, delay


def knock(host: str, ports: list, proto='tcp',
          delay=0.3, timeout=0.5) -> bool:
    """Send the knock sequence to host. Best-effort: what matters
    is that each packet reaches the firewall, not that anything
    replies, so every socket error is swallowed. Returns False
    when nothing was sent (no ports)."""
    if not ports:
        return False

    for port in ports:
        try:
            if proto == 'udp':
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                try:
                    s.sendto(b'', (host, port))
                finally:
                    s.close()
            else:
                s = socket.socket(socket.AF_INET,
                                  socket.SOCK_STREAM)
                s.settimeout(timeout)
                try:
                    s.connect((host, port))
                except OSError:
                    pass                # filtered/closed = expected
                finally:
                    s.close()
        except OSError:
            pass
        time.sleep(delay)

    # give the daemon a moment to open the real port
    time.sleep(delay)
    return True
