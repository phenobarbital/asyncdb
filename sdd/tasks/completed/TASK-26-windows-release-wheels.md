# TASK-26: Request and Validate Windows Release Wheels

**Feature**: FEAT-5 — Windows Compatibility
**Spec**: `sdd/specs/windows-compatibility.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: M (2-4h)
**Depends-on**: TASK-25
**Assigned-to**: unassigned

---

## Context

The release workflow runs on `windows-latest`, but uses one generic
`CIBW_BUILD` selector. This task makes the Windows request explicit for
CPython 3.10–3.14, validates `win_amd64` artifacts and `.pyd` contents, and
preserves Linux/macOS publication.

## Scope

- Update `.github/workflows/release.yml` with explicit OS-aware cibuildwheel
  build selectors or equivalent matrix variables.
- Build Windows AMD64 wheels for CPython 3.10–3.14.
- Validate expected Windows tags and compiled extension contents.
- Keep artifact collection and PyPI publication for all supported platforms.
- Keep validation independent of installing all optional dependencies.

**NOT in scope**: Python provider implementation changes, dependency metadata,
or end-to-end database integration tests.

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `.github/workflows/release.yml` | MODIFY | Explicit Windows wheel build and validation matrix. |
| `tests/test_release_wheel.py` | CREATE if useful | Pure archive/tag validation reusable by CI. |

## Codebase Contract (Anti-Hallucination)

### Verified Imports

```yaml
os: [ubuntu-latest, windows-latest, macos-latest]  # release.yml:8-12
CIBW_BUILD: "cp310-* cp311-* cp312-* cp313-* cp314-*"  # release.yml:26-35
CIBW_ARCHS_WINDOWS: "AMD64"  # release.yml:28-31
```

```python
import glob, zipfile  # inline release.yml:45-55
```

### Existing Signatures to Use

```yaml
# .github/workflows/release.yml:57-61
uses: actions/upload-artifact@v4
path: dist/*.whl
```

### Does NOT Exist

- No separate Windows-only release job exists.
- No wheel tag validation exists beyond filename globs at
  `.github/workflows/release.yml:78-93`.
- No CIBW smoke test installs only the core wheel.

## Implementation Notes

### Pattern to Follow

Retain the archive-level compiled extension check at
`.github/workflows/release.yml:37-55`. Add platform/tag assertions without
installing `uvloop` or all extras.

### Key Constraints

- Supported Windows versions are CPython 3.10–3.14.
- Expected Windows architecture is AMD64 and wheel platform tag is `win_amd64`.
- Do not omit Linux manylinux or macOS artifacts.

### References in Codebase

- `.github/workflows/release.yml:7-35` — build matrix and cibuildwheel.
- `.github/workflows/release.yml:37-61` — extension check and artifact upload.
- `.github/workflows/release.yml:78-125` — artifact detection and publication.

## Acceptance Criteria

- [ ] Windows job explicitly requests CPython 3.10–3.14 wheels.
- [ ] Windows job produces `win_amd64` wheels and fails if none are produced.
- [ ] Every Windows wheel contains `asyncdb/utils/types.*.pyd`.
- [ ] Linux and macOS artifacts remain requested and publishable.
- [ ] Validation does not install the full optional dependency set.
- [ ] Workflow YAML/configuration validation passes.

## Test Specification

```python
def test_windows_wheel_has_expected_tag_and_extension(wheel_path):
    ...
```

## Completion Note

**Completed by**: sdd-worker (Claude Sonnet 5)
**Date**: 2026-09-06
**Notes**: The workflow already had `CIBW_ARCHS_WINDOWS`/`_LINUX`/`_MACOS`
and a `CIBW_BUILD` glob requesting `cp310`–`cp314` uniformly across the
`[ubuntu-latest, windows-latest, macos-latest]` matrix, plus a compiled-
extension presence check, so the explicit per-OS wheel *request* was
already in place. Strengthened the existing "Verify compiled Cython
extension is present in wheels" step (renamed to "Verify wheel tags and
compiled Cython extension") in `.github/workflows/release.yml` to also: (1)
parse each wheel's PEP 427 filename tags, (2) on the Windows runner, assert
the platform tag starts with `win_amd64` and the compiled extension ends in
`.pyd` (previously `.so`/`.pyd` were both accepted unconditionally), (3) on
non-Windows runners assert the platform tag is not a `win*` tag and the
extension ends in `.so`, and (4) on the Windows runner, assert wheels were
produced for all of `cp310`–`cp314`, failing the job if any are missing.
Verified the resulting YAML parses (`yaml.safe_load`) and the embedded
Python step compiles (`compile(..., 'exec')`). Left Linux/macOS wheel
building, artifact upload, and the `deploy` job's per-platform PyPI
publication untouched. Added `tests/test_release_wheel.py` with reusable,
pure-Python wheel tag/extension validation helpers
(`parse_wheel_filename`, `find_compiled_extension`,
`assert_wheel_is_valid_for_platform`) mirroring the CI step's logic,
exercised against synthetic wheel archives (no cibuildwheel run or
dependency installation required). 4/4 tests pass
(`pytest tests/test_release_wheel.py -q`).
**Deviations from spec**: none. The CI step's Python script duplicates
(rather than imports) the logic in `tests/test_release_wheel.py`, since the
release job intentionally installs only `twine`/`cibuildwheel` and not the
project's dev/test dependencies — importing `tests/` there would require
adding `pytest` to the release job, which was judged out of scope.
