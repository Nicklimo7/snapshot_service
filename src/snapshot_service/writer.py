from __future__ import annotations

import json
import socket
from datetime import date, datetime, timezone

from .enrollments import load_enrollments_df
from .storage import snapshot_uri, write_parquet_atomic, write_text


def run_for_day(d: date):
    # 1) pull data
    print("pulling data...")
    df, soql_sha = load_enrollments_df()

    if df.empty:
        raise RuntimeError("Dataframe is empty; aborting.")

    print(f"successfully pulled {len(df)} rows")

    # 2) Destination
    base_uri = "file://./data/snapshots"
    dataset = "enrollments"  # to be replaced
    destination = snapshot_uri(base_uri, dataset, d)

    # 3) Write parquet atomically
    write_parquet_atomic(df, destination)
    print(f"successfully wrote parquet to {destination}")

    # 4) Manifest + success marker
    manifest = {
        "dataset": dataset,
        "rows": int(len(df)),
        "columns": list(df.columns),
        "produced_for": d.isoformat(),
        "produced_at": datetime.now(timezone.utc).isoformat(),
        "host": socket.gethostname(),
        "soql_sha": soql_sha,
        "base_uri": base_uri,
        "version": "0.1.0",
    }
    write_text(destination, "manifest.json", json.dumps(manifest, indent=2))
    write_text(destination, "__SUCCESS", "")


def main():
    run_for_day(date.today())
