import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    dataset: str = os.getenv("DATASET_NAME", "enrollments")
    base_uri: str = os.getenv("SNAPSHOT_BASE_DIR")
    tz: str = os.getenv("TZ", "America/Chicago")


cfg = Config
