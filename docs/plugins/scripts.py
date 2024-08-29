import shutil
import subprocess  # nosec
from os import getenv
from typing import cast

__all__ = ["copy_changelog", "docs_to_site"]


def copy_changelog(*args, **kwargs):
    shutil.copy("CHANGELOG.md", "docs/CHANGELOG.md")


def docs_to_site(*args, **kwargs):
    source = cast(str, getenv("DOCS_SOURCE"))
    dest = cast(str, getenv("DOCS_DEST"))
    subprocess.call(["rclone", "sync", source, dest])  # nosec
