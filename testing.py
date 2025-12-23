# Testing
from interactive_set_up import set_up_for_interactive

from dotenv import load_dotenv

load_dotenv()

set_up_for_interactive()

from snapshot_service.reader import latest_snapshot  # noqa
from snapshot_service.field_formatters import format_fields  # noqa
from snapshot_service.basic_merges import generate_initial_cred_df  # noqa

# ============================
# Loading snapshot data
# ============================

DATASET_NAME = "idv_cleared_npis"

df = latest_snapshot(DATASET_NAME)


# ============================
# Loading merged data
# ============================

df = generate_initial_cred_df()

mask = df["account_npi"] == "1750505343"
df_subset = df[mask]

# ============================
# Create CSV
# ============================

df.to_csv("wingspan_payees.csv", index=False)
