from __future__ import annotations

import json
import argparse
import os
import socket
from dataclasses import dataclass
from datetime import date, datetime, timezone

from dotenv import load_dotenv

from .airtable import load_airtable_snapshot

# ===== Configuration =====
from .idv import get_idv_checked_df, get_idv_cleared_df
from .salesforce import load_salesforce_df
from .storage import snapshot_uri, write_parquet_atomic, write_text
from .wingspan import fetch_payee_data

load_dotenv()

GSHEET_IDV_SHEET_URL = os.getenv("GSHEET_IDV_SHEET_URL")
GSHEET_IDV_TAB_NAME_CLEARED = os.getenv("GSHEET_IDV_TAB_NAME_CLEARED")
GSHEET_LOG_TAB_NAME = "log"
DATE_TODAY = date.today()

BASE_URI = os.getenv("SNAPSHOT_BASE_DIR")


# ===== Summary dataclass =====
@dataclass
class SnapshotSummary:
    dataset: str
    rows: int
    columns: int
    result: str


def print_summary(summaries: list[SnapshotSummary]):
    print("\n=== Snapshot Summary ===\n")
    for summary in summaries:
        print(
            f"Dataset: {summary.dataset}\n"
            f"Rows: {summary.rows}\n"
            f"Columns: {summary.columns}\n"
            f"Result: {summary.result}\n"
            "-----------------------\n"
        )


# ==== Arg parsing =====
def parse_args():
    parser = argparse.ArgumentParser(
        description="Snapshot writer (runs all datasets by default)"
    )

    parser.add_argument(
        "--only",
        type=str,
        help="Comma-separated list of datasets to run (default: all)",
    )

    parser.add_argument(
        "--list_datasets",
        action="store_true",
        help="List available datasets and exit",
    )

    args = parser.parse_args()

    if args.list_datasets:
        print("Available datasets:")
        for dataset in database_dicts.keys():
            print(f"- {dataset}")
        exit(0)

    return args


def csv_arg(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [x.strip() for x in value.split(",") if x.strip()]


# ===== Loaders for each dataset =====


def load_enrollments_df():
    return load_salesforce_df("soql/enr.soql")


def load_accounts_df():
    return load_salesforce_df("soql/acc.soql")


def load_license_df():
    return load_salesforce_df("soql/lic.soql")


def load_cred_hx_df():
    return load_salesforce_df("soql/cred_hx.soql")


def load_cleared_npis_and_write_to_sheet():
    return get_idv_cleared_df(
        upload_to_sheet=True,
        gsheet_url=GSHEET_IDV_SHEET_URL,
        sheet_name=GSHEET_IDV_TAB_NAME_CLEARED,
    )


# ===== Main snapshot writer =====

database_dicts = {
    "idv_checked_npis": get_idv_checked_df,
    "idv_cleared_npis": load_cleared_npis_and_write_to_sheet,
    "enrollments": load_enrollments_df,
    "licenses": load_license_df,
    "accounts": load_accounts_df,
    "credentialing_history": load_cred_hx_df,
    "wingspan_payees": fetch_payee_data,
    "airtable": load_airtable_snapshot,
}


# Loop through each database and run the snapshot for today
def main(*, only_datasets: list[str] | None = None):
    if not BASE_URI:
        raise RuntimeError(
            "SNAPSHOT_BASE_DIR is not set (BASE_URI is empty). Aborting."
        )

    results: list[SnapshotSummary] = []

    # Filter databases if only_datasets is provided
    if only_datasets is not None:
        filtered_dict = {k: v for k, v in database_dicts.items() if k in only_datasets}
        missing = set(only_datasets) - set(filtered_dict.keys())
        if missing:
            raise ValueError(f"Datasets not found: {', '.join(missing)}")
        database_dicts_to_run = filtered_dict
    else:
        database_dicts_to_run = database_dicts

    print("\n=== Starting snapshot writer ===\n")
    print(f"[writer] Using BASE_URI={BASE_URI}")
    print(f"[writer] Starting snapshot for {DATE_TODAY.isoformat()}\n\n")

    for dataset, load_df in database_dicts_to_run.items():
        success = False
        df = None
        soql_sha = None
        err_msg = None

        print(f"===== {dataset} =====")
        try:
            print(f"Loading {dataset} dataset...")
            loaded = load_df()

            # Support both return shapes: df OR (df, soql_sha)
            if isinstance(loaded, tuple) and len(loaded) == 2:
                df, soql_sha = loaded
            else:
                df = loaded

            if df is None or df.empty:
                raise RuntimeError("Dataframe is empty; aborting.")

            print(f"successfully pulled {len(df)} rows from {dataset}")

            destination = snapshot_uri(BASE_URI, dataset, DATE_TODAY)
            print(f"[writer] Writing snapshot for '{dataset}' to {destination}")

            write_parquet_atomic(df, destination)
            print(f"successfully wrote parquet to {destination}")
            success = True

            manifest = {
                "dataset": dataset,
                "rows": int(len(df)),
                "columns": list(df.columns),
                "produced_for": DATE_TODAY.isoformat(),
                "produced_at": datetime.now(timezone.utc).isoformat(),
                "host": socket.gethostname(),
                "soql_sha": soql_sha,  # will be None for non-SOQL loaders
                "base_uri": BASE_URI,
                "version": "0.1.0",
            }
            write_text(destination, "manifest.json", json.dumps(manifest, indent=2))
            write_text(destination, "__SUCCESS", "")

        except Exception as e:
            err_msg = str(e)
            print(f"Error processing {dataset}: {err_msg}")

        # Append summary safely (even if df never existed)
        results.append(
            SnapshotSummary(
                dataset=dataset,
                rows=int(len(df)) if df is not None else 0,
                columns=len(df.columns) if df is not None else 0,
                result="success" if success else f"failure: {err_msg}",
            )
        )

        print(f"===== Finished {dataset} =====\n\n")

    print_summary(results)


def cli():
    args = parse_args()
    only_datasets = csv_arg(args.only)
    main(only_datasets=only_datasets)


if __name__ == "__main__":
    args = parse_args()
    only_datasets = csv_arg(args.only)

    main(only_datasets=only_datasets)
