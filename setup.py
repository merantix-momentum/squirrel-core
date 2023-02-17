#!/usr/bin/env python

import ast
import subprocess
import re
import sys


SOURCE_DIR = "squirrel"

# Read package information from other files so that just one version has to be maintained.
with open("pyproject.toml", "rb") as f:
    init_contents = f.read().decode("utf-8")

    def get_var(var_name: str) -> str:
        """Parsing of squirrel project infos defined in __init__.py"""
        pattern = re.compile(r"%s\s+=\s+(.*)" % var_name)
        match = pattern.search(init_contents).group(1)
        return str(ast.literal_eval(match))

    version = get_var("version")


def assert_version(ver: str) -> None:
    """Assert version follows semantics such as 0.0.1 or 0.0.1-dev123. Notice English letters are not allowed after
    'dev'.
    """
    pattern = (
        r"^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)"
        + r"(?P<prepost>\.post\d+|(dev|a|b|rc)\d+)?(?P<devsuffix>[+-]dev)?\d*$"
    )
    assert bool(re.match(pattern, ver)), ValueError(
        f"Version string '{ver}' does not conform with regex '{pattern}', which is required by pypi metadata "
        "normalization."
    )


def normalize_version(_version: str, _version_tag: str) -> str:
    """Normalize version string according to tag build or dev build, to conform with the standard of PEP 440."""
    if "dev" in _version_tag:
        # remove alphabetic characters after keyword 'dev', which is forbidden PEP 440.
        short_sha = _version_tag[3:]  # substring after the word 'dev'
        numberic_sha = "".join([char for char in short_sha if char.isdigit()])
        _version += "-dev" + numberic_sha
    else:
        # In tag build, use the $TAG_NAME as the version string.
        _version = _version_tag.replace("v", "")
    assert_version(_version)
    return _version


# add tag to version if provided
if "--version_tag" in sys.argv:
    v_idx = sys.argv.index("--version_tag")
    version_tag = sys.argv[v_idx + 1]
    version = normalize_version(version, version_tag)
    sys.argv.remove("--version_tag")
    sys.argv.pop(v_idx)


if __name__ == "__main__":
    subprocess.run(
        (
            f"sed -r -i.bak 's/^version.*$/version = \"{version}\"/' pyproject.toml && "
            "python -m build && "
            "mv pyproject.toml.bak pyproject.toml"
        ),
        shell=True,
        check=True,
    )
