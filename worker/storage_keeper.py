"""Retain a bounded output mount until collection or loss of the agent's lease.

This process has no RPC/Docker credentials, network, workload command or write access
to the output. Only the trusted agent can renew it through the Docker signal API.
"""

import math
import os
import signal
import sys
import time


def main() -> None:
    if sys.platform != "linux":
        raise RuntimeError("output retention requires a Linux execution host")
    ttl = float(sys.argv[1])
    if not math.isfinite(ttl) or not 0 < ttl <= 604800:
        raise ValueError("invalid output retention lease")
    deadline = time.monotonic() + ttl

    def renew(*_: object) -> None:
        nonlocal deadline
        deadline = time.monotonic() + ttl

    signal.signal(signal.Signals["SIGUSR1"], renew)
    # Fail closed if a daemon substituted an unbounded or writable helper mount.
    with open("/proc/self/mountinfo") as stream:
        mount = next((line.split() for line in stream if " /retained " in line), None)
    if not mount:
        raise RuntimeError("output retention mount is absent")
    separator = mount.index("-")
    if mount[separator + 1] != "tmpfs" or "ro" not in mount[5].split(","):
        raise RuntimeError("output retention requires a read-only tmpfs mount")
    if "noswap" not in mount[separator + 3].split(","):
        raise RuntimeError("output retention requires a non-swappable mount")
    capacity = os.statvfs("/retained")
    if capacity.f_blocks == 0 or capacity.f_frsize == 0:
        raise RuntimeError("output retention quota is invalid")
    print("storage keeper ready", flush=True)
    while (remaining := deadline - time.monotonic()) > 0:
        time.sleep(min(0.2, remaining))


if __name__ == "__main__":
    main()
