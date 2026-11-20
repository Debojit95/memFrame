#!/usr/bin/env python3
"""Cold-path time and peak-RSS benchmark for groupby/window operations.

Usage:
    uv run python scripts/bench_groupby_memory.py --op cumsum --rows 1000000
    uv run python scripts/bench_groupby_memory.py --op gwindow --rows 5000000 \
        --preview-rows 10

Run one op per process so ``ru_maxrss`` reflects that operation only.
"""

from __future__ import annotations

import argparse
import os
import resource
import time

import pandas as pd

from memframe import MemFrame


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--op", choices=("stats", "cumsum", "gwindow", "window"),
                        default="cumsum")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--groups", type=int, default=100)
    parser.add_argument("--preview-rows", type=int, default=10)
    parser.add_argument("--db-path", default="/tmp/memframe-bench.duckdb")
    args = parser.parse_args()

    try:
        os.remove(args.db_path)
    except OSError:
        pass

    n = args.rows
    frame = pd.DataFrame({
        "g": [str(i % args.groups) for i in range(n)],
        "t": pd.date_range("2024-01-01", periods=n, freq="min"),
        "x": [float(i % 1000) for i in range(n)],
        "y": [float((i * 7) % 1000) for i in range(n)],
        "z": [float((i * 13) % 1000) for i in range(n)],
    })

    mf = MemFrame(
        connection_type="local",
        connection_params={"db_path": args.db_path},
        preview_rows=args.preview_rows,
    )
    mf.connect()
    try:
        ctx = mf.DataFrame(frame, filename=f"bench_{args.op}")
        started = time.perf_counter()
        if args.op == "stats":
            ctx.groupby("g").sum("x")
        elif args.op == "cumsum":
            ctx.groupby("g").cumsum("x", order_col="t")
        elif args.op == "gwindow":
            ctx.groupby("g").rolling(20, order_by="t").mean("x")
        else:
            ctx.rolling("x", window=20, func="mean", order_by="t")
        elapsed = time.perf_counter() - started
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 / 1024
        print(
            f"{args.op}: rows={n} groups={args.groups} "
            f"preview_rows={args.preview_rows} "
            f"cold={elapsed:.2f}s peakRSS={rss:.2f}GB"
        )
    finally:
        mf.close()


if __name__ == "__main__":
    main()
