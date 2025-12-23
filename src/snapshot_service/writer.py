from __future__ import annotations

import json
import os
import socket
from datetime import date, datetime, timezone

from dotenv import load_dotenv

from .airtable import load_airtable_snapshot
from .idv import get_idv_checked_df, get_idv_cleared_df
from .wingspan import fetch_payee_data
from .salesforce import load_salesforce_df
from .storage import snapshot_uri, write_parquet_atomic, write_text
from .config import cfg

load_dotenv()  # will look for a .env file in your project root

GSHEET_IDV_SHEET_URL = os.getenv("GSHEET_IDV_SHEET_URL")
GSHEET_IDV_TAB_NAME_CLEARED = os.getenv("GSHEET_IDV_TAB_NAME_CLEARED")
GSHEET_LOG_TAB_NAME = "log"

# Use the same base URI configuration as readers (SNAPSHOT_BASE_URI or default)
BASE_URI = cfg.base_uri


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
def main():
    print("\n=== Starting snapshot writer ===\n")
    DATE_TODAY = date.today()
    print(f"[writer] Using BASE_URI={BASE_URI}")
    # get each dataset
    print(f"[writer] Starting snapshot for {DATE_TODAY.isoformat()}\n\n")
    for dataset, load_df in database_dicts.items():
        print(f"===== {dataset} =====")
        try:
            print(f"Loading {dataset} dataset...")
            df, soql_sha = load_df()

            if df.empty:
                raise RuntimeError("Dataframe is empty; aborting.")

            print(f"successfully pulled {len(df)} rows from {dataset}")

            destination = snapshot_uri(BASE_URI, dataset, DATE_TODAY)

            print(f"[writer] Writing snapshot for '{dataset}' to {destination}")

            # Write parquet atomically
            write_parquet_atomic(df, destination)
            print(f"successfully wrote parquet to {destination}")

            # 4) Manifest + success marker
            manifest = {
                "dataset": dataset,
                "rows": int(len(df)),
                "columns": list(df.columns),
                "produced_for": DATE_TODAY.isoformat(),
                "produced_at": datetime.now(timezone.utc).isoformat(),
                "host": socket.gethostname(),
                "soql_sha": soql_sha,
                "base_uri": BASE_URI,
                "version": "0.1.0",
            }
            write_text(destination, "manifest.json", json.dumps(manifest, indent=2))
            write_text(destination, "__SUCCESS", "")
        except Exception as e:
            print(f"Error processing {dataset}: {e}")

        print(f"===== Finished {dataset} =====\n\n")


if __name__ == "__main__":
    main()
