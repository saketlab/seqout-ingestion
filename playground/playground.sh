#!/usr/bin/env bash

for f in "$1"/*.tar; do
    [ -f "$f" ] || continue
    echo "$f"
    tar -tf "$f"
    echo "====="
done
