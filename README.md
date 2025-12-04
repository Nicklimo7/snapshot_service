# To use in other projects

- Add this to project.toml:
[tool.uv.sources]
snapshot-service = { path = "C:/Scripts/snapshot_service", editable = true }

- import the package first, then run this code:

import os

from dotenv import load_dotenv

load_dotenv()  # load .env in this consumer project

from snapshot_service.reader import latest_snapshot  # noqa: E402