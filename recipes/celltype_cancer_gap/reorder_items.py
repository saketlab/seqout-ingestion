"""
Reorder each host's remaining items.txt so h5ad/rds-linked studies (the only
formats with real embedded-metadata content-inspection, ~4.6% of the worklist
but where nearly all true positives will come from) run before everything
else, with mtx/h5/barcodes-only studies (raw 10x output, structurally almost
never annotated) pushed to the very back.

Does not touch already-completed items or the fleet's running processes --
just reorders each host's local items.txt for its next relaunch cycle.

  python recipes/celltype_cancer_gap/reorder_items.py --apply
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

SIDECAR_DIR = Path("/data/saket/celltype_cancer_gap/yuj_setup/accessions")
FLEET_CSV = Path("/NAS/saket/github/seqout-ingestion/yuj/quant/fleet.local.csv")

TIER_EMBEDDED = 0  # h5ad / rds link present
TIER_TABULAR = 1  # csv/tsv/txt/tar, no h5ad/rds
TIER_RAW_ONLY = 2  # mtx/h5/barcodes only


def classify(accession: str) -> int:
    f = SIDECAR_DIR / f"{accession}.json"
    try:
        d = json.loads(f.read_text())
    except (OSError, json.JSONDecodeError):
        return TIER_TABULAR
    types = {t for t, _ in d.get("links", [])}
    if types & {"h5ad", "rds"}:
        return TIER_EMBEDDED
    if types & {"mtx", "h5", "barcodes"}:
        return TIER_RAW_ONLY
    return TIER_TABULAR


def load_fleet() -> list[tuple[str, str, str, str, str]]:
    rows = []
    for line in FLEET_CSV.read_text().splitlines()[1:]:
        user, ip, name, password, key = (line.split(",") + [""] * 5)[:5]
        if ip == "localhost":
            ip = "127.0.0.1"
        rows.append((user.strip(), ip.strip(), name.strip(), password.strip(), key.strip()))
    return rows


def ssh_cmd(user: str, ip: str, password: str, key: str) -> list[str]:
    common = ["-o", "ConnectTimeout=15", "-o", "StrictHostKeyChecking=no"]
    if key:
        return ["ssh", "-i", key, *common, "-o", "BatchMode=yes", f"{user}@{ip}"]
    if password:
        return ["sshpass", "-p", password, "ssh", "-o", "PreferredAuthentications=password",
                 "-o", "PubkeyAuthentication=no", *common, f"{user}@{ip}"]
    return ["ssh", *common, "-o", "BatchMode=yes", f"{user}@{ip}"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="actually push reordered lists")
    a = ap.parse_args()

    for user, ip, name, password, key in load_fleet():
        if name in ("kcdh",):
            continue
        cmd = ssh_cmd(user, ip, password, key)
        try:
            out = subprocess.run(
                [*cmd, "cat seqout-sc-count/items.txt"],
                capture_output=True, text=True, timeout=20, stdin=subprocess.DEVNULL,
            )
        except subprocess.TimeoutExpired:
            print(f"{name:12s} timeout")
            continue
        if out.returncode != 0:
            print(f"{name:12s} unreachable: {out.stderr.strip()[:80]}")
            continue
        items = [x for x in out.stdout.splitlines() if x.strip()]
        if not items:
            print(f"{name:12s} 0 items")
            continue
        tiers: dict[int, list[str]] = {0: [], 1: [], 2: []}
        for acc in items:
            tiers[classify(acc)].append(acc)
        reordered = tiers[TIER_EMBEDDED] + tiers[TIER_TABULAR] + tiers[TIER_RAW_ONLY]
        print(
            f"{name:12s} total={len(items)} embedded={len(tiers[0])} "
            f"tabular={len(tiers[1])} raw_only={len(tiers[2])}"
        )
        if not a.apply:
            continue
        push = subprocess.run(
            [*cmd, "cat > seqout-sc-count/items.txt"],
            input="\n".join(reordered) + "\n", capture_output=True, text=True, timeout=20,
        )
        if push.returncode != 0:
            print(f"  push failed: {push.stderr.strip()[:120]}")


if __name__ == "__main__":
    main()
