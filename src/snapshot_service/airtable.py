import os
import json
import pandas as pd
import hashlib
from pyairtable import Api
from tqdm import tqdm
from itertools import chain


def load_airtable_df(api_key: str, base_id: str, table_name: str) -> pd.DataFrame:
    api = Api(api_key)
    table = api.table(base_id, table_name)
    all_records = chain.from_iterable(table.iterate())

    line_items = []
    for record in tqdm(all_records, desc="Fetching records"):
        if "fields" not in record or "id" not in record:
            raise ValueError(f"❌ Malformed record received from Airtable: {record}")

        fields = record["fields"]
        fields["record_id"] = record["id"]
        line_items.append(fields)

    df = pd.DataFrame(line_items)

    # Clean and format NPI values safely
    if "NPI Number" in df.columns:
        df["NPI Number"] = pd.to_numeric(df["NPI Number"], errors="coerce")

        bad_npi_count = df["NPI Number"].isna().sum()
        if bad_npi_count > 0:
            print(
                f"⚠️ Skipping {bad_npi_count} records with invalid or blank NPI Number."
            )

        df = df[df["NPI Number"].notna()].copy()

        # Final cast to 10-digit string
        df["NPI Number"] = df["NPI Number"].apply(lambda x: str(int(x)).zfill(10))

        # --- Make Parquet-safe: stringify nested structures & bytes
    import json
    import base64

    problem_cols = []

    def _to_parquet_safe(x):
        if isinstance(x, (dict, list, tuple, set)):
            return json.dumps(x, ensure_ascii=False)
        if isinstance(x, (bytes, bytearray)):
            try:
                return x.decode("utf-8", errors="ignore")
            except Exception:
                return base64.b64encode(x).decode("ascii")
        return x

    for col in df.columns:
        if (
            df[col]
            .map(lambda v: isinstance(v, (dict, list, tuple, set, bytes, bytearray)))
            .any()
        ):
            df[col] = df[col].apply(_to_parquet_safe)
            problem_cols.append(col)

    if problem_cols:
        print(f"ℹ️ Converted nested/bytes to JSON/text in {len(problem_cols)} columns")

    return df


def load_airtable_snapshot() -> tuple[pd.DataFrame, str]:
    """
    Snapshot-style loader: returns (df, sha).
    Reads API key/base/table from env vars.
    """
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("AIRTABLE_BASE_ID")
    table_name = os.getenv("AIRTABLE_TABLE_NAME")

    if not api_key or not base_id or not table_name:
        raise RuntimeError(
            "Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID in environment or AIRTABLE_TABLE_NAME."
        )

    df = load_airtable_df(api_key, base_id, table_name)

    # Create a stable "query fingerprint" like soql_sha
    fingerprint = json.dumps({"base_id": base_id, "table": table_name})
    sha = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()

    return df, sha
