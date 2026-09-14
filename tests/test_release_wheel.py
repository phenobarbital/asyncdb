"""Pure wheel archive/tag validation for the release workflow (FEAT-5 / TASK-26).

These helpers mirror the inline validation performed by the "Verify wheel
tags and compiled Cython extension" step in
``.github/workflows/release.yml`` and are exercised here against synthetic
wheel archives, so the tag/extension contract can be regression-tested
without a real cibuildwheel run and without installing any optional
dependency.
"""

import zipfile
from pathlib import Path
from typing import Optional

import pytest

# Supported Windows CPython version tags for the Windows release matrix.
EXPECTED_WINDOWS_CPYTHON_TAGS = frozenset({"cp310", "cp311", "cp312", "cp313", "cp314"})


def parse_wheel_filename(wheel_path: str) -> dict[str, str]:
    """Parse a wheel filename into its PEP 427 tag components.

    Args:
        wheel_path: Path (or bare filename) of a ``.whl`` file.

    Returns:
        A dictionary with ``distribution``, ``version``, ``python_tag``,
        ``abi_tag`` and ``platform_tag`` keys.

    Raises:
        ValueError: If the filename does not look like a valid wheel name.
    """
    filename = Path(wheel_path).name
    if not filename.endswith(".whl"):
        raise ValueError(f"Not a wheel filename: {filename}")
    stem = filename[: -len(".whl")]
    parts = stem.split("-")
    if len(parts) < 5:
        raise ValueError(f"Not a valid wheel filename: {filename}")
    python_tag, abi_tag, platform_tag = parts[-3:]
    version = parts[-4]
    distribution = "-".join(parts[:-4])
    return {
        "distribution": distribution,
        "version": version,
        "python_tag": python_tag,
        "abi_tag": abi_tag,
        "platform_tag": platform_tag,
    }


def find_compiled_extension(wheel_path: str, suffix: str) -> Optional[str]:
    """Find the compiled ``asyncdb.utils.types`` extension inside a wheel.

    Args:
        wheel_path: Path to a ``.whl`` archive.
        suffix: Expected file suffix, e.g. ``".pyd"`` or ``".so"``.

    Returns:
        The matching archive member name, or ``None`` if not found.
    """
    with zipfile.ZipFile(wheel_path) as archive:
        for member in archive.namelist():
            if member.startswith("asyncdb/utils/types.") and member.endswith(suffix):
                return member
    return None


def assert_wheel_is_valid_for_platform(wheel_path: str) -> str:
    """Validate a wheel's platform tag and compiled extension.

    A Windows wheel (``platform_tag`` starting with ``win_amd64``) must
    contain a ``.pyd`` compiled extension. Any other wheel must not use a
    Windows platform tag and must contain a ``.so`` compiled extension.

    Args:
        wheel_path: Path to the ``.whl`` archive to validate.

    Returns:
        The archive member name of the compiled extension that was found.

    Raises:
        AssertionError: If the tag/extension contract is violated.
    """
    tags = parse_wheel_filename(wheel_path)
    platform_tag = tags["platform_tag"]

    if platform_tag.startswith("win_amd64"):
        expected_suffix = ".pyd"
    else:
        assert not platform_tag.startswith("win"), (
            f"unexpected Windows platform tag: {platform_tag}"
        )
        expected_suffix = ".so"

    ext = find_compiled_extension(wheel_path, expected_suffix)
    assert ext, (
        f"compiled Cython extension ({expected_suffix}) missing in {wheel_path}"
    )
    return ext


def _make_synthetic_wheel(tmp_path: Path, filename: str, extension_member: str) -> Path:
    """Create a minimal synthetic wheel archive for tag/extension tests.

    Args:
        tmp_path: Pytest temporary directory fixture value.
        filename: Wheel filename to create, e.g.
            ``"asyncdb-2.0.0-cp311-cp311-win_amd64.whl"``.
        extension_member: Archive member name for the compiled extension,
            e.g. ``"asyncdb/utils/types.cp311-win_amd64.pyd"``.

    Returns:
        Path to the created synthetic wheel archive.
    """
    wheel_path = tmp_path / filename
    with zipfile.ZipFile(wheel_path, "w") as archive:
        archive.writestr(extension_member, b"")
        archive.writestr("asyncdb/__init__.py", b"")
    return wheel_path


@pytest.fixture()
def windows_wheel_path(tmp_path: Path) -> Path:
    """A synthetic, valid Windows (win_amd64/.pyd) wheel archive."""
    return _make_synthetic_wheel(
        tmp_path,
        "asyncdb-2.0.0-cp311-cp311-win_amd64.whl",
        "asyncdb/utils/types.cp311-win_amd64.pyd",
    )


@pytest.fixture()
def linux_wheel_path(tmp_path: Path) -> Path:
    """A synthetic, valid manylinux (.so) wheel archive."""
    return _make_synthetic_wheel(
        tmp_path,
        "asyncdb-2.0.0-cp311-cp311-manylinux_2_17_x86_64.whl",
        "asyncdb/utils/types.cpython-311-x86_64-linux-gnu.so",
    )


def test_windows_wheel_has_expected_tag_and_extension(windows_wheel_path):
    """A Windows wheel must report the `win_amd64` tag and contain a
    `.pyd` compiled extension."""
    tags = parse_wheel_filename(str(windows_wheel_path))
    assert tags["platform_tag"] == "win_amd64"
    assert tags["python_tag"] in EXPECTED_WINDOWS_CPYTHON_TAGS

    member = assert_wheel_is_valid_for_platform(str(windows_wheel_path))
    assert member.endswith(".pyd")


def test_linux_wheel_has_expected_tag_and_extension(linux_wheel_path):
    """A manylinux wheel must not use a Windows platform tag and must
    contain a `.so` compiled extension."""
    member = assert_wheel_is_valid_for_platform(str(linux_wheel_path))
    assert member.endswith(".so")


def test_wheel_missing_compiled_extension_is_rejected(tmp_path):
    """A wheel missing the compiled extension entirely must fail
    validation."""
    wheel_path = tmp_path / "asyncdb-2.0.0-cp311-cp311-win_amd64.whl"
    with zipfile.ZipFile(wheel_path, "w") as archive:
        archive.writestr("asyncdb/__init__.py", b"")

    with pytest.raises(AssertionError):
        assert_wheel_is_valid_for_platform(str(wheel_path))


def test_wheel_with_wrong_extension_for_platform_is_rejected(tmp_path):
    """A Windows-tagged wheel containing a `.so` (not `.pyd`) extension
    must fail validation."""
    wheel_path = _make_synthetic_wheel(
        tmp_path,
        "asyncdb-2.0.0-cp311-cp311-win_amd64.whl",
        "asyncdb/utils/types.cpython-311-x86_64-linux-gnu.so",
    )

    with pytest.raises(AssertionError):
        assert_wheel_is_valid_for_platform(str(wheel_path))
