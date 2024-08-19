#!/usr/bin/env python.exe

import re
import sys
from subprocess import run  # nosec

__all__ = [
    "BUILD_TYPE_RE",
    "CHANGE_MAP",
    "CHANGE_TYPE_RE",
    "COMMIT_PATH",
    "POSTRELEAS_TYPES",
    "PRERELEAS_TYPES",
    "TAG_CHECK_RE",
    "UNSTABLE_RE",
    "autobump",
    "get_last_tag",
]

CHANGE_TYPE_RE = re.compile(r"^\w+[^():]")
BUILD_TYPE_RE = re.compile(r"(?<=\[)\w+")
UNSTABLE_RE = re.compile(r"a|b|rc|dev")
TAG_CHECK_RE = re.compile(r"\bv?[\d{1,2}\.?]+(?=\.)\b")

COMMIT_PATH = "C:\\Users\\IvanDanchuk\\byteflows\\hooks\\test_dummy_commit.txt"

CHANGE_MAP = {
    ("build", "docs", "bugfix", "chore", "style"): "micro",
    ("feat", "revert", "perf", "test"): "minor",
}
PRERELEAS_TYPES = ("alpha", "beta", "rc", "dev")
POSTRELEAS_TYPES = "post"


def get_last_tag():
    tag_list = run(
        ["git", "tag", "--list"],
        capture_output=True,
        encoding="utf-8",  # nosec
    ).stdout
    if tag_list:
        for tag in tag_list:
            if TAG_CHECK_RE.search(tag):
                return tag
        return ""


def autobump(commit_path, autocommit=False, autotag=False, test=False):
    with open(commit_path, "r+") as commit:
        commit.seek(0)
        full_msg = commit.readlines()
        top_msg = full_msg[0]
        change_type = re.findall(r"^\w+!?[^():]", top_msg)
        build_type = re.findall(r"(?<=\[)\b\w+\b", top_msg)
        if not (change_type and build_type):
            msg = f"В обычном коммите обязательно должен быть указан тип изменений (feat, bugfix, docs,...) и характер изменений в квадратных скобках (alpha, beta, rc, stable)"
            sys.stderr.write(msg)
            sys.exit(1)
        else:
            change = change_type[0]
            build = build_type[0]
            is_prereleas = build in PRERELEAS_TYPES
            if is_prereleas:
                bump_cmd = f"pdm bump pre-release --pre {build}{' --commit' if autocommit else ''}{' --tag' if autotag else ''}{' --dry-run' if test else ''}"
            else:
                for k in CHANGE_MAP:
                    if change in k and "!" not in change:
                        change_level = CHANGE_MAP[k]
                    else:
                        change_level = "major"
                bump_cmd = f"pdm bump {change_level}{' --commit' if autocommit else ''}{' --tag' if autotag else ''}{' --dry-run' if test else ''}"
            renew_commit = top_msg.replace(f"[{build}]", "")
            commit.seek(0, 0)
            full_msg[0] = f"{renew_commit.strip()}\n"
            commit.writelines(full_msg)
            commit.close()
        run(bump_cmd.split(" "))  # nosec
        sys.exit(0)


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description='Hook for autobumping version')
    # parser.add_argument('commit_path', type=str, help='Path to the file with the commit message.')
    # parser.add_argument('--test', type=bool, help='Dry run without changing the commit message. The changes will be written to standard output.'
    #                     ,default=False, required=False)
    # parser.add_argument('--autotag', type=bool, help='Automatically generate a version tag.', default=False)
    # parser.add_argument('--autocommit', type=bool, help='Automatically commit changes to version and tag.', default=False)
    # args = parser.parse_args()
    # autobump(args.commit_path, args.autocommit, args.autotag, args.test)
    tag = "v2.0.1.abadeika#$%"
    print(TAG_CHECK_RE.search(tag))
