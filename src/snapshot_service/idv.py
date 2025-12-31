from __future__ import annotations

from dotenv import load_dotenv
from name_mismatch_workflow.summarize import run_idv_summary
from org_utils.gsheet_utils import upload_dataframe_to_sheet

load_dotenv()

_RESULTS: dict | None = None  # Module level cache for results


def _get_results() -> dict:
    global _RESULTS
    if _RESULTS is None:
        _RESULTS = run_idv_summary()
    return _RESULTS


def get_idv_checked_df(
    upload_to_sheet=False, gsheet_url: str = None, sheet_name: str = None
):
    results = _get_results()
    df = results["checked_npis"]["df"]
    if upload_to_sheet and sheet_name and gsheet_url:
        upload_dataframe_to_sheet(
            dataframe=df, gsheet_url=gsheet_url, sheet_name=sheet_name, verbose=True
        )

    return results["checked_npis"]["df"], results["checked_npis"]["sha"]


def get_idv_cleared_df(
    upload_to_sheet=False, gsheet_url: str = None, sheet_name: str = None
):
    results = _get_results()
    df = results["cleared_npis"]["df"]
    if upload_to_sheet and sheet_name and gsheet_url:
        upload_dataframe_to_sheet(
            dataframe=df, gsheet_url=gsheet_url, sheet_name=sheet_name, verbose=True
        )
    return results["cleared_npis"]["df"], results["cleared_npis"]["sha"]
