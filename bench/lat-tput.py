#!/usr/bin/env python3
"""
lat-tput.py -- Parse result.txt and generate a latency-throughput scatter plot.

Usage:
    python3 lat-tput.py [result_file] [output_png]

Defaults:
    result_file : result.txt  (expected in the current working directory)
    output_png  : lat-tput.png
"""

import sys
import os
import re
import matplotlib
matplotlib.use("Agg")          # headless backend — no display needed
import matplotlib.pyplot as plt


def parse_result(path: str):
    """
    Parse a result.txt produced by tput.cpp.

    Expected data lines (after the header) look like:
            1       10.23        9.80       15.20       48.50            97
    Columns: clientCount  latAvg  latP50  latP90  latP99  throughput
    """
    records = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            # Skip header / comment lines
            if line.startswith("#") or line.startswith("-") or not line:
                continue
            parts = line.split()
            if len(parts) < 6:
                continue
            try:
                rec = {
                    "clients":    int(parts[0]),
                    "lat_avg":    float(parts[1]),
                    "lat_p50":    float(parts[2]),
                    "lat_p90":    float(parts[3]),
                    "lat_p99":    float(parts[4]),
                    "throughput": float(parts[5]),
                }
                records.append(rec)
            except ValueError:
                continue
    return records


def plot(records, output_path: str):
    if not records:
        print("No data records found — nothing to plot.", file=sys.stderr)
        sys.exit(1)

    # Sort by throughput so lines don't zigzag when a round has lower
    # throughput than the previous one (e.g. warmup noise at n=2).
    records = sorted(records, key=lambda r: r["throughput"])

    throughputs = [r["throughput"] for r in records]
    lat_avg     = [r["lat_avg"]    for r in records]
    lat_p50     = [r["lat_p50"]    for r in records]
    lat_p90     = [r["lat_p90"]    for r in records]
    lat_p99     = [r["lat_p99"]    for r in records]
    clients     = [r["clients"]    for r in records]

    fig, ax = plt.subplots(figsize=(9, 6))

    ax.plot(throughputs, lat_avg, "o-", label="latAvg",  linewidth=2, markersize=6)
    ax.plot(throughputs, lat_p50, "s-", label="latP50",  linewidth=2, markersize=6)
    ax.plot(throughputs, lat_p90, "^-", label="latP90",  linewidth=2, markersize=6)
    ax.plot(throughputs, lat_p99, "D-", label="latP99",  linewidth=2, markersize=6)

    # Annotate each point with the client count
    for x, y, c in zip(throughputs, lat_p99, clients):
        ax.annotate(
            f"n={c}",
            xy=(x, y),
            xytext=(4, 4),
            textcoords="offset points",
            fontsize=8,
            color="gray",
        )

    ax.set_xlabel("Throughput (ops/sec)", fontsize=12)
    ax.set_ylabel("Latency (ms)", fontsize=12)
    ax.set_title("KV Service: Latency vs Throughput (3-replica cluster)", fontsize=13)
    ax.legend(fontsize=10)
    ax.grid(True, linestyle="--", alpha=0.5)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    print(f"Plot saved to {output_path}")


def main():
    result_file  = sys.argv[1] if len(sys.argv) > 1 else "result.txt"
    output_png   = sys.argv[2] if len(sys.argv) > 2 else "lat-tput.png"

    if not os.path.exists(result_file):
        print(f"Error: '{result_file}' not found.", file=sys.stderr)
        sys.exit(1)

    records = parse_result(result_file)
    print(f"Parsed {len(records)} data points from '{result_file}'.")
    plot(records, output_png)


if __name__ == "__main__":
    main()
