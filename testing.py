from datetime import date

import pandas as pd

from interactive_set_up import set_up_for_interactive

set_up_for_interactive()

from snapshot_service.storage import (  # noqa
    read_parquet,
    snapshot_uri,
    write_parquet_atomic,
    write_text,
)

base_uri = "file://./data/snapshots"  # or your S3 base like "s3://my-bucket/snapshots"
dataset = "enrollments"
today = date.today()

# 1) Make where-to-put-it
folder = snapshot_uri(base_uri, dataset, today)

# 2) Make a tiny DataFrame
df = pd.DataFrame({"hello": [1, 2, 3]})

# 3) Write the parquet safely
write_parquet_atomic(df, folder)  # writes data.parquet

# 4) Add a small manifest + success flag
write_text(folder, "manifest.json", '{"rows": 3}')
write_text(folder, "__SUCCESS", "")

# 5) Read it back (sanity)
df2 = read_parquet(folder)
print(df2)
