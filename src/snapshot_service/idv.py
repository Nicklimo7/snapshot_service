from name_mismatch_workflow.summarize import run_idv_summary
from org_utils.gsheet_utils import upload_dataframe_to_sheet
from dotenv import load_dotenv

load_dotenv()

results = run_idv_summary()


def get_idv_checked_df(
    upload_to_sheet=False, gsheet_url: str = None, sheet_name: str = None
):
    df = results["checked_npis"]["df"]
    if upload_to_sheet and sheet_name and gsheet_url:
        upload_dataframe_to_sheet(
            dataframe=df, gsheet_url=gsheet_url, sheet_name=sheet_name, verbose=True
        )

    return results["checked_npis"]["df"], results["checked_npis"]["sha"]


def get_idv_cleared_df(
    upload_to_sheet=False, gsheet_url: str = None, sheet_name: str = None
):
    df = results["cleared_npis"]["df"]
    if upload_to_sheet and sheet_name and gsheet_url:
        upload_dataframe_to_sheet(
            dataframe=df, gsheet_url=gsheet_url, sheet_name=sheet_name, verbose=True
        )
    return results["cleared_npis"]["df"], results["cleared_npis"]["sha"]
