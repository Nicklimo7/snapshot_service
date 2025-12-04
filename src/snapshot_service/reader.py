# snapshot_service/reader.py
from __future__ import annotations

import os
import re
import json
from pathlib import Path
from urllib.parse import urlparse, unquote

import pandas as pd

# ---- Patterns for dates -----------------------------------------------------

DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DATE_FILE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.parquet$", re.IGNORECASE)

# ---- URI / Path helpers -----------------------------------------------------


def _is_s3(uri: str) -> bool:
    return uri.startswith("s3://")


def _get_default_base_uri() -> str:
    # Fetch at *call* time so .env loaded after import still works
    return os.getenv("SNAPSHOT_BASE_URI") or "file://./data/snapshots"


def _coerce_base(base_uri: str | None) -> str:
    r"""
    Normalize base into a URI string:
      - s3://... stays as-is
      - file://... stays as-is
      - C:\...  -> file:///C:/...
      - /abs    -> file:///abs
      - relative -> file:///<absolute>
    """
    b = (base_uri or _get_default_base_uri()).strip().strip('"').strip("'")
    if not b:
        raise ValueError(
            "SNAPSHOT_BASE_URI is not set and no base_uri was provided. "
            "Set SNAPSHOT_BASE_URI in your environment or .env file, or pass base_uri=... manually."
        )
    if b.startswith(("s3://", "file://")):
        return b
    # Windows absolute path like C:\...
    if re.match(r"^[A-Za-z]:[\\/]", b):
        return "file:///" + b.replace("\\", "/")
    # POSIX absolute path
    if b.startswith("/"):
        return "file://" + b
    # Relative -> absolute
    return "file://" + str(Path(b).resolve()).replace("\\", "/")


def _uri_to_local_path(s: str) -> Path:
    r"""
    Convert either a plain local path (C:\\... or /...) OR a file:// URI
    into a Path that works on the current OS.
    """
    if s.startswith("file://"):
        parsed = urlparse(s)
        p = unquote(parsed.path)
        # On Windows, urlparse('file:///C:/...').path -> '/C:/...'
        if os.name == "nt" and p.startswith("/"):
            p = p.lstrip("/")
        return Path(p).resolve()
    return Path(s).resolve()


def _dataset_dir(base_uri: str | None, dataset: str) -> str:
    base = _coerce_base(base_uri)
    return f"{base.rstrip('/')}/{dataset}"


# ---- S3 listing helpers -----------------------------------------------------


def _s3_list_paths(root: str) -> list[str]:
    r"""
    List child paths (pseudo 'dir' entries) under an s3:// root using s3fs.
    Returns strings like 'bucket/prefix/date' or 'bucket/prefix/2025-08-16.parquet'
    (no scheme). We only need the last segment anyway.
    """
    import s3fs  # type: ignore

    fs = s3fs.S3FileSystem()
    # strip 's3://' for s3fs
    s3_root = root[5:]
    try:
        return fs.ls(s3_root)
    except FileNotFoundError:
        return []


# ---- Public API -------------------------------------------------------------


def list_dates(dataset: str, base_uri: str | None = None) -> list[str]:
    r"""
    Return sorted list of available snapshot dates (YYYY-MM-DD) for a dataset.
    Supports both:
      - folder-per-date: <dataset>/<YYYY-MM-DD>/
      - flat files:      <dataset>/<YYYY-MM-DD>.parquet
    """
    root = _dataset_dir(base_uri, dataset)

    if _is_s3(root):
        # S3: collect folder names & flat file dates
        entries = _s3_list_paths(root)
        # s3fs.ls returns 'bucket/prefix/...'; extract last segment
        names = [e.split("/")[-1].rstrip("/") for e in entries]
        dir_dates = [n for n in names if DATE_DIR_RE.match(n)]
        file_dates = []
        for n in names:
            m = DATE_FILE_RE.match(n)
            if m:
                file_dates.append(m.group(1))
        return sorted(set(dir_dates) | set(file_dates))

    # Local filesystem
    ds_root = _uri_to_local_path(root)
    if not ds_root.exists():
        return []

    dir_dates = [
        p.name for p in ds_root.iterdir() if p.is_dir() and DATE_DIR_RE.match(p.name)
    ]
    file_dates = []
    for p in ds_root.iterdir():
        if p.is_file():
            m = DATE_FILE_RE.match(p.name)
            if m:
                file_dates.append(m.group(1))
    return sorted(set(dir_dates) | set(file_dates))


def latest_date(dataset: str, base_uri: str | None = None) -> str | None:
    dates = list_dates(dataset, base_uri)
    return dates[-1] if dates else None


def snapshot_path(dataset: str, date_str: str, base_uri: str | None = None) -> str:
    r"""
    Preferred Parquet path (folder-per-date).
    Fallback to flat file is handled in `load()`.
    """
    return f"{_dataset_dir(base_uri, dataset)}/{date_str}/{date_str}.parquet"


def _snapshot_path_flat(
    dataset: str, date_str: str, base_uri: str | None = None
) -> str:
    """Legacy flat file path."""
    return f"{_dataset_dir(base_uri, dataset)}/{date_str}.parquet"


def manifest_path(dataset: str, date_str: str, base_uri: str | None = None) -> str:
    return f"{_dataset_dir(base_uri, dataset)}/{date_str}/manifest.json"


def load(
    dataset: str, date_str: str | None = None, base_uri: str | None = None
) -> pd.DataFrame:
    r"""
    Load a snapshot DataFrame. Tries:
        1) <dataset>/<date>/<date>.parquet
        2) <dataset>/<date>.parquet  (legacy)

    By default, uses the SNAPSHOT_BASE_URI environment variable (set in your environment or .env file).
    You can override this by passing base_uri manually.
    """
    date_str = date_str or latest_date(dataset, base_uri)
    if not date_str:
        raise FileNotFoundError(
            f"No snapshots found for dataset '{dataset}' at {_coerce_base(base_uri)}"
        )

    pref = snapshot_path(dataset, date_str, base_uri)
    flat = _snapshot_path_flat(dataset, date_str, base_uri)

    if _is_s3(pref):
        # Pandas can read s3:// with s3fs installed
        try:
            return pd.read_parquet(pref)
        except Exception:
            return pd.read_parquet(flat)

    # Local paths
    p_pref = _uri_to_local_path(pref)
    if p_pref.exists():
        return pd.read_parquet(p_pref)

    p_flat = _uri_to_local_path(flat)
    if p_flat.exists():
        return pd.read_parquet(p_flat)

    raise FileNotFoundError(
        f"Snapshot files not found for '{dataset}' on {date_str}. "
        f"Tried:\n  {p_pref}\n  {p_flat}"
    )


def load_manifest(
    dataset: str, date_str: str | None = None, base_uri: str | None = None
) -> dict:
    r"""
    Load manifest.json for a snapshot (only exists in folder-per-date layout).
    """
    date_str = date_str or latest_date(dataset, base_uri)
    if not date_str:
        raise FileNotFoundError(
            f"No snapshots found for dataset '{dataset}' at {_coerce_base(base_uri)}"
        )

    mpath = manifest_path(dataset, date_str, base_uri)

    if _is_s3(mpath):
        import s3fs  # type: ignore

        fs = s3fs.S3FileSystem()
        s3_path = mpath[5:]  # strip scheme
        try:
            with fs.open(s3_path, "rb") as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"Manifest not found at {mpath}. Was this snapshot written with the legacy flat layout?"
            )

    lp = _uri_to_local_path(mpath)
    if not lp.exists():
        raise FileNotFoundError(
            f"Manifest not found at {lp}. Was this snapshot written with the legacy flat layout?"
        )
    with open(lp, "r", encoding="utf-8") as f:
        return json.load(f)


def latest_snapshot(dataset: str, base_uri: str | None = None) -> pd.DataFrame:
    r"""
    Load the latest snapshot for a dataset.
    By default, uses the SNAPSHOT_BASE_URI environment variable (set in your environment or .env file).
    You can override this by passing base_uri manually.
    """
    date_str = latest_date(dataset, base_uri)
    if date_str:
        print(f"[latest_snapshot] Loading dataset '{dataset}' for date: {date_str}")
    else:
        print(f"[latest_snapshot] No available dates for dataset '{dataset}'")
    return load(dataset, date_str, base_uri)
