# TASK-014: Update Delta Example Script

**Feature**: FEAT-003 — migrate-delta-driver
**Spec**: sdd/specs/migrate-delta-driver.spec.md
**Status**: pending
**Priority**: low
**Effort**: S
**Depends on**: TASK-011

---

## Objective

Update `examples/test_delta.py` to demonstrate the new `write()` parameters:
`mode`, `schema_mode`, `configuration`, and `predicate`.

## Acceptance Criteria

1. Example shows `write()` with `mode="overwrite"`.
2. Example shows `write()` with `schema_mode="merge"`.
3. Example shows `write()` with `configuration={"delta.appendOnly": "true"}`.
4. Example shows backward-compatible `if_exists` usage with a comment noting deprecation.
5. Example is runnable (no syntax errors).

## Implementation Notes

- Add a new section to the existing example, don't rewrite the entire file.
- Use inline comments to explain each new parameter.
- Keep it concise — this is a reference example, not documentation.

## Files to Create/Modify

- **Modify**: `examples/test_delta.py`

## Test Strategy

- Visual review; example is not part of the automated test suite.
