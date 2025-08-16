import sys
from pathlib import Path


def set_up_for_interactive():
    # Make notebooks robust to being opened from subfolders

    # Walk up until we find the project root (where pyproject.toml lives)
    p = Path.cwd()
    while p != p.parent and not (p / "pyproject.toml").exists():
        p = p.parent

    # Add src/ to sys.path
    src_path = p / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
