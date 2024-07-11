import shutil

__all__ = ["copy_changelog"]


def copy_changelog(*args, **kwargs):
    shutil.copy("CHANGELOG.md", "docs/CHANGELOG.md")
