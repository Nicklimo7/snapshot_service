from snapshot_service.reader import latest_snapshot
from snapshot_service.field_formatters import format_fields
from org_utils.npi_utils import clean_npi_column, is_valid_npi
import datetime
import numpy as np
import pandas as pd

from dotenv import load_dotenv

load_dotenv()


def generate_initial_cred_df():
    # Get the accounts
    df_acc = latest_snapshot("accounts")

    # Get account history
    df_cred_hx = latest_snapshot("credentialing_history")

    # Format account fields
    df_acc = format_fields("accounts", df_acc)

    # Filter accounts for initial credentialing only
    df_acc = df_acc[df_acc["account_cred_stage"] == "Initial Credentialing"]

    # Filter to only changes *to* Initial Credentialing
    mask_initial_cred = df_cred_hx["NewValue"] == "Initial Credentialing"
    df_initial_cred = df_cred_hx[mask_initial_cred].copy()

    # Keep only the most recent entry per account
    df_initial_cred_latest = df_initial_cred.sort_values(
        "CreatedDate", ascending=False
    ).drop_duplicates(subset="AccountId", keep="first")

    # Merge history to account info
    df = df_acc.merge(
        df_initial_cred_latest,
        left_on="account_id",
        right_on="AccountId",
        how="left",
    )

    # Clean the NPI column
    df["npi"] = clean_npi_column(df, "account_npi")

    # Filter out rows with invalid NPIs
    df["is_valid_npi"] = df["npi"].apply(is_valid_npi)
    mask_valid_npi = df["is_valid_npi"]
    df = df[mask_valid_npi].copy()

    today = datetime.date.today()

    # Ensure CreatedDate and cred status date is a datetime
    df["CreatedDate"] = pd.to_datetime(df["CreatedDate"], errors="coerce")
    df["cvo_cred_status_update_date"] = pd.to_datetime(
        df["cvo_cred_status_update_date"], errors="coerce"
    )

    # Convert to date (still may contain NaT)
    df["CreatedDate_date"] = df["CreatedDate"].dt.date
    df["cvo_cred_status_update_date"] = df["cvo_cred_status_update_date"].dt.date

    # Now safely compute weekday differences
    df["business_days_since_initial_cred_start"] = df["CreatedDate_date"].apply(
        lambda d: np.busday_count(d, today) if pd.notnull(d) else np.nan
    )
    df["business_days_since_cred_status_update"] = df[
        "cvo_cred_status_update_date"
    ].apply(lambda d: np.busday_count(d, today) if pd.notnull(d) else np.nan)

    return df


def main():
    df = generate_initial_cred_df()
    print(df.info())
    df.to_csv("initial_cred_accounts.csv", index=False)


if __name__ == "__main__":
    main()
