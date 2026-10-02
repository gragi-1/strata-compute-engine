import os
from pathlib import Path


def available_memory_mb(fallback: int) -> int:
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return min(fallback, int(line.split()[1]) // 1024)
    except OSError:
        pass
    return fallback


def available_cpu(total: float, own_reserved: float) -> float:
    if hasattr(os, "getloadavg"):
        outside_load = max(0, os.getloadavg()[0] - own_reserved)
        return float(max(0, min(total, (os.cpu_count() or 1) - outside_load)))
    return total
