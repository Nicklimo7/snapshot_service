from interactive_set_up import set_up_for_interactive

set_up_for_interactive()

from snapshot_service.storage import (  # noqa
    read_parquet,
    snapshot_uri,
    write_parquet_atomic,
    write_text,
)

from snapshot_service.reader import latest_snapshot  # noqa
from snapshot_service.field_formatters import format_fields  # noqa

dataset = "enrollments"

enr_df = latest_snapshot(dataset)

enr_df = format_fields(dataset, enr_df)

print(enr_df.info())
