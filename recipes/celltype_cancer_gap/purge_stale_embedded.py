"""
Delete result files for h5ad/rds studies scanned before today's real-content-
check fix landed (old shape: metadata == {"embedded": true, "format": ...,
"has_celltype": true/false} with no "columns"/"fields" key -- the blind
format-only assumption). The worker skips any item whose result file already
exists, so these must be deleted to get reprocessed with real content
inspection.

  python recipes/celltype_cancer_gap/purge_stale_embedded.py --apply
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

FLEET_CSV = Path("/NAS/saket/github/seqout-ingestion/yuj/quant/fleet.local.csv")


def load_fleet() -> list[tuple[str, str, str, str, str]]:
    rows = []
    for line in FLEET_CSV.read_text().splitlines()[1:]:
        user, ip, name, password, key = (line.split(",") + [""] * 5)[:5]
        if ip == "localhost":
            ip = "127.0.0.1"
        rows.append(
            (user.strip(), ip.strip(), name.strip(), password.strip(), key.strip())
        )
    return rows


def ssh_cmd(user: str, ip: str, password: str, key: str) -> list[str]:
    common = ["-o", "ConnectTimeout=15", "-o", "StrictHostKeyChecking=no"]
    if key:
        return ["ssh", "-i", key, *common, "-o", "BatchMode=yes", f"{user}@{ip}"]
    if password:
        return [
            "sshpass",
            "-p",
            password,
            "ssh",
            "-o",
            "PreferredAuthentications=password",
            "-o",
            "PubkeyAuthentication=no",
            *common,
            f"{user}@{ip}",
        ]
    return ["ssh", *common, "-o", "BatchMode=yes", f"{user}@{ip}"]


# Runs remotely: find stale h5ad/rds results (old shape, no "columns" key
# in the embedded metadata) among files matching this host's items.txt.
REMOTE_SCRIPT = r"""
import json, os
stale = []
try:
    items = set(open("seqout-sc-count/items.txt").read().split())
except FileNotFoundError:
    items = None
for f in os.listdir("seqout-sc-count/results"):
    if not f.endswith(".json"):
        continue
    acc = f[:-5]
    if items is not None and acc not in items:
        continue
    path = os.path.join("seqout-sc-count/results", f)
    try:
        d = json.load(open(path))
    except Exception:
        continue
    if d.get("single_cell_file_format") not in ("h5ad", "rds"):
        continue
    for s in d.get("per_sample", []):
        m = s.get("metadata")
        if m and m.get("embedded") and "columns" not in m:
            stale.append(path)
            break
print(len(stale))
for p in stale:
    print(p)
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    for user, ip, name, password, key in load_fleet():
        if name == "kcdh":
            continue
        cmd = ssh_cmd(user, ip, password, key)
        out = subprocess.run(
            [*cmd, "python3", "-"],
            input=REMOTE_SCRIPT,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if out.returncode != 0:
            print(f"{name:12s} unreachable/error: {out.stderr.strip()[:100]}")
            continue
        lines = out.stdout.splitlines()
        if not lines:
            print(f"{name:12s} no output")
            continue
        n = int(lines[0])
        paths = lines[1:]
        print(f"{name:12s} stale h5ad/rds results: {n}")
        if n == 0 or not a.apply:
            continue
        rm = subprocess.run(
            [*cmd, "xargs", "rm", "-f"],
            input="\n".join(paths),
            capture_output=True,
            text=True,
            timeout=30,
        )
        if rm.returncode != 0:
            print(f"  delete failed: {rm.stderr.strip()[:120]}")
        else:
            print(f"  deleted {n}")


if __name__ == "__main__":
    main()
