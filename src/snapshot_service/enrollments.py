from __future__ import annotations
from pathlib import Path
from typing import Optional
import hashlib
import pandas as pd
from org_utils.sf_env import client_from_env


def load_enrollments_df(
    soql_path: str = "soql/enr.soql", limit_preview: Optional[int] = None
) -> tuple[pd.DataFrame, str]:
    """Uses the SF bulk API to pull soql and return a df and soql_sha

    Args:
        soql_path (str, optional): _description_. Defaults to "soql/enr.soql".
        limit_preview (Optional[int], optional): Include to limit results. Defaults to None.

    Returns:
        tuple[pd.DataFrame, str]: soql as a df, soql_sha
    """

    # Find the soql file for the query
    path = Path(soql_path)
    if not path.exists():
        raise FileNotFoundError(f"SOQL file not found: {path.resolve()}")

    soql = path.read_text(encoding="utf-8").strip()
    soql_sha = hashlib.sha256(soql.encode("utf-8")).hexdigest()[:12]

    # Set up the client and run the bulk query
    sf = client_from_env()
    df = sf.bulk_query_to_df(soql)

    if limit_preview:
        df = df.head(limit_preview).copy()

    # Basic normalization you’ll want consistently in your snapshots
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    # Ensure datetime-like columns become datetimes and not objects (adjust names as needed)
    for col in [
        c
        for c in df.columns
        if c.lower().endswith(("date", "date__c", "datetime", "datetime__c"))
    ]:
        try:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        except Exception:
            pass

    return df, soql_sha
