from __future__ import annotations
from datetime import date
from pathlib import Path
import io
import pandas as pd

# --- Small utilities ---------------------------------------------------------


def _is_s3(uri: str) -> bool:
    return uri.startswith("s3://")


def _join_uri(*parts: str) -> str:
    """Join URI segments without duplicating slashes."""
    return "/".join(p.strip("/").replace("\\", "/") for p in parts)


def _local_path_from_uri(uri: str) -> Path:
    """Turn file:// URIs into local Path objects (or pass-through plain local paths)."""
    if uri.startswith("file://"):
        return Path(uri.replace("file://", "")).resolve()
    return Path(uri).resolve()


def _last_segment_from_uri(uri: str) -> str:
    """Return the final path segment (date folder name)."""
    if _is_s3(uri):
        # s3 always uses POSIX separators
        path = uri[5:].rstrip("/")
        return path.split("/")[-1]
    # Local path: use pathlib so backslashes are handled
    return Path(_local_path_from_uri(uri)).name


# --- Paths & conventions -----------------------------------------------------


def snapshot_root(base_uri: str, dataset: str) -> str:
    """
    Base folder for a dataset.
    e.g. s3://bucket/snapshots/enrollments
         file://./data/snapshots/enrollments
    """
    return _join_uri(base_uri, dataset)


def snapshot_uri(base_uri: str, dataset: str, d: date) -> str:
    """
    Daily partition folder for a dataset.
    e.g. .../enrollments/2025/08/10
    """
    return _join_uri(snapshot_root(base_uri, dataset), d.isoformat())


def object_uri(folder_uri: str, name: str) -> str:
    """
    Child object path (file) inside a snapshot folder.
    e.g. .../2025/08/10/data.parquet
    """
    return _join_uri(folder_uri, name)


# --- Existence checks --------------------------------------------------------


def has_success_marker(folder_uri: str) -> bool:
    """
    True if __SUCCESS exists (used by readers to avoid partial snapshots).
    """
    marker = object_uri(folder_uri, "__SUCCESS")
    if _is_s3(folder_uri):
        import boto3

        s3 = boto3.client("s3")
        bucket, key = marker[5:].split("/", 1)
        try:
            s3.head_object(Bucket=bucket, Key=key)
            return True
        except s3.exceptions.ClientError:
            return False
    else:
        return _local_path_from_uri(marker).exists()


# --- Writes (atomic-ish) -----------------------------------------------------


def write_text(folder_uri: str, name: str, content: str) -> None:
    """
    Write a small text file (manifest.json, __SUCCESS, notes.txt).
    """
    if _is_s3(folder_uri):
        import boto3

        s3 = boto3.client("s3")
        bucket, prefix = folder_uri[5:].split("/", 1)
        key = _join_uri(prefix, name)
        s3.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
    else:
        path = _local_path_from_uri(folder_uri)
        path.mkdir(parents=True, exist_ok=True)
        (path / name).write_text(content, encoding="utf-8")


def write_parquet_atomic(
    df: pd.DataFrame, folder_uri: str, filename: str | None = None
) -> None:
    """
    Write parquet via a temp object, then move to final name.
    Readers look only for the final name + __SUCCESS marker.
    """
    # default to <date>.parquet based on the folder name
    if filename is None:
        date_str = _last_segment_from_uri(folder_uri)
        filename = f"{date_str}.parquet"

    if _is_s3(folder_uri):
        import boto3

        s3 = boto3.client("s3")
        bucket, prefix = folder_uri[5:].split("/", 1)
        tmp_key = _join_uri(prefix, "tmp", filename)
        final_key = _join_uri(prefix, filename)

        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)

        # Upload to tmp, then copy to final, then delete tmp
        s3.put_object(Bucket=bucket, Key=tmp_key, Body=buf.getvalue())
        s3.copy_object(
            Bucket=bucket, CopySource={"Bucket": bucket, "Key": tmp_key}, Key=final_key
        )
        s3.delete_object(Bucket=bucket, Key=tmp_key)
    else:
        folder = _local_path_from_uri(folder_uri)
        tmp_dir = folder / "tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)

        tmp_path = tmp_dir / filename
        final_path = folder / filename

        df.to_parquet(tmp_path, index=False)
        tmp_path.replace(final_path)  # atomic rename within the same filesystem


# --- Reads (handy when testing) ----------------------------------------------


def read_parquet(folder_uri: str, filename: str | None = None) -> pd.DataFrame:
    if filename is None:
        filename = f"{_last_segment_from_uri(folder_uri)}.parquet"
    obj = object_uri(folder_uri, filename)
    if _is_s3(obj):
        return pd.read_parquet(obj)
    else:
        return pd.read_parquet(_local_path_from_uri(obj))
