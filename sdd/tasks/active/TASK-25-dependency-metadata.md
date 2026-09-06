# TASK-25: Align Dependency Metadata with Windows Support

**Feature**: FEAT-5 — Windows Compatibility
**Spec**: `sdd/specs/windows-compatibility.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: M (2-4h)
**Depends-on**: TASK-23, TASK-24
**Assigned-to**: unassigned

---

## Context

`pyproject.toml` currently places several packages in core dependencies even
though they are used by individual drivers or output paths. Once import
boundaries are audited, this task updates metadata so the core plus approved
default drivers are installable on Windows without claiming support for every
optional provider.

## Scope

- Reconcile `dependencies` and optional extras with the completed import audit.
- Keep uvloop in its existing optional extra and keep provider-specific packages
  in explicit extras.
- Add platform markers only where verified and necessary.
- Document the approved Windows support boundary for core and `default` extras.
- Add metadata consistency tests or validation.

**NOT in scope**: Provider implementation changes, release workflow changes, or
new third-party dependencies.

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `pyproject.toml` | MODIFY | Reclassify audited dependencies and preserve extras. |
| `README.md` or provider documentation | MODIFY | Document Windows/core/default support boundary. |
| `tests/test_package_metadata.py` | CREATE | Validate dependency groups and uvloop optionality. |

## Codebase Contract (Anti-Hallucination)

### Verified Imports

```toml
[project]
dependencies = [...]  # pyproject.toml:47-69

[project.optional-dependencies]
uvloop = ["uvloop==0.21.0"]  # pyproject.toml:71-74
default = [...]  # pyproject.toml:75-84
```

### Existing Signatures to Use

```python
# asyncdb/models/model.py:11-26
from numpy import int64
DB_TYPES[int64] = "bigint"
```

### Does NOT Exist

- No generated provider-support metadata file exists.
- No package metadata test currently asserts the core/default dependency
  boundary.

## Implementation Notes

### Pattern to Follow

Use the existing named extras in `pyproject.toml:71-199`; do not duplicate
provider dependencies across new ad hoc groups unless required by the audit.

### Key Constraints

- Approved support covers Python 3.10–3.14 and all providers under `default`.
- `uvloop` is optional and automatically activated when present on supported
  platforms.
- The core dependency contract must include packages required by core plus the
  approved default-driver behavior.

### References in Codebase

- `pyproject.toml:47-69` — core dependencies.
- `pyproject.toml:71-199` — optional dependency groups.
- `asyncdb/models/model.py:11-26` — shared NumPy/datamodel import path.

## Acceptance Criteria

- [ ] Core metadata no longer requires audited unrelated Linux/native providers.
- [ ] uvloop remains only in the optional `uvloop` extra.
- [ ] All approved default providers have declared dependencies.
- [ ] Windows support scope is documented without claiming universal provider
      compatibility.
- [ ] Metadata tests pass: `pytest tests/test_package_metadata.py -q`.

## Test Specification

```python
def test_uvloop_is_optional():
    ...

def test_default_extra_contains_approved_provider_dependencies():
    ...

def test_core_dependency_contract_is_portable():
    ...
```

## Completion Note

**Completed by**: unassigned
**Date**: YYYY-MM-DD
**Notes**: Pending implementation.
**Deviations from spec**: none
