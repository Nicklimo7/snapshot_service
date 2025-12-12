from interactive_set_up import set_up_for_interactive

set_up_for_interactive()

from snapshot_service.reader import latest_snapshot  # noqa
from snapshot_service.field_formatters import format_fields  # noqa

dataset = "enrollments"

df = latest_snapshot("wingspan_payees")

print(df.info())
print(df.head())
