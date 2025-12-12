# snapshot_service/reader.py (local-only simplified)

from __future__ import annotations
import os
import re
from pathlib import Path
import pandas as pd

# ---- Patterns ---------------------------------------------------------------

DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DATE_FILE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.parquet$", re.IGNORECASE)

# ---- Base directory logic ---------------------------------------------------


def _get_base_dir(base_dir: str | Path | None) -> Path:
    """
    Return the directory where snapshots are stored.

    Priority:
      1. Caller-provided base_dir
      2. SNAPSHOT_BASE_DIR env var

    No defaults other than env var. This keeps behavior obvious.
    """
    bd = base_dir or os.getenv("SNAPSHOT_BASE_DIR")
    if not bd:
        raise ValueError(
            "SNAPSHOT_BASE_DIR is not set and no base_dir was provided.\n"
            "Set it in your environment/.env or pass base_dir=... manually."
        )
    return Path(bd).expanduser().resolve()


def _dataset_dir(dataset: str, base_dir: str | Path | None) -> Path:
    return _get_base_dir(base_dir) / dataset


# ---- Public API -------------------------------------------------------------


def list_dates(dataset: str, base_dir: str | Path | None = None) -> list[str]:
    """
    Return sorted date strings found under the dataset folder.
    Supports folder-per-date and/or flat files.
    """
    root = _dataset_dir(dataset, base_dir)
    if not root.exists():
        return []

    dir_dates = [
        p.name for p in root.iterdir() if p.is_dir() and DATE_DIR_RE.match(p.name)
    ]

    file_dates = []
    for p in root.iterdir():
        if p.is_file():
            m = DATE_FILE_RE.match(p.name)
            if m:
                file_dates.append(m.group(1))

    return sorted(set(dir_dates) | set(file_dates))


def latest_date(dataset: str, base_dir: str | Path | None = None) -> str | None:
    dates = list_dates(dataset, base_dir)
    return dates[-1] if dates else None


def snapshot_path(
    dataset: str, date_str: str, base_dir: str | Path | None = None
) -> Path:
    """
    Folder-per-date preferred layout.
    """
    return _dataset_dir(dataset, base_dir) / date_str / f"{date_str}.parquet"


def _snapshot_path_flat(
    dataset: str, date_str: str, base_dir: str | Path | None = None
) -> Path:
    """
    Legacy flat file layout.
    """
    return _dataset_dir(dataset, base_dir) / f"{date_str}.parquet"


def load(
    dataset: str, date_str: str | None = None, base_dir: str | Path | None = None
) -> pd.DataFrame:
    """
    Load a snapshot.
    Attempts:
      1. folder/date/date.parquet
      2. dataset/date.parquet
    """
    date_str = date_str or latest_date(dataset, base_dir)
    if not date_str:
        raise FileNotFoundError(f"No snapshots found for dataset '{dataset}'.")

    p_pref = snapshot_path(dataset, date_str, base_dir)
    p_flat = _snapshot_path_flat(dataset, date_str, base_dir)

    if p_pref.exists():
        return pd.read_parquet(p_pref)

    if p_flat.exists():
        return pd.read_parquet(p_flat)

    raise FileNotFoundError(
        f"Snapshot not found for '{dataset}' at date {date_str}.\n"
        f"Tried:\n  {p_pref}\n  {p_flat}"
    )


def latest_snapshot(dataset: str, base_dir: str | Path | None = None) -> pd.DataFrame:
    """
    Load the latest available snapshot.
    """
    d = latest_date(dataset, base_dir)
    if d:
        print(f"[latest_snapshot] Loading dataset '{dataset}' for date: {d}")
    else:
        print(f"[latest_snapshot] No available dates for dataset '{dataset}'")
    return load(dataset, d, base_dir)
