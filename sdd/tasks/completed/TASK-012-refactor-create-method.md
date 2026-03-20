# TASK-012: Refactor `create()` to Align with New Write Interface

**Feature**: FEAT-003 — migrate-delta-driver
**Spec**: sdd/specs/migrate-delta-driver.spec.md
**Status**: pending
**Priority**: high
**Effort**: S
**Depends on**: TASK-011

---

## Objective

Refactor the `create()` method in `asyncdb/drivers/delta.py` to accept the same
extended parameters as the new `write()` method (`schema_mode`, `configuration`,
`**kwargs`). Wrap the `write_deltalake` call in `asyncio.to_thread()` and remove
the `engine` parameter.

## Acceptance Criteria

1. `create()` signature updated to include `schema_mode` and `configuration` params.
2. `**kwargs` forwarded to `write_deltalake`.
3. `write_deltalake` call wrapped in `await asyncio.to_thread(...)`.
4. `None`-valued optional args excluded from the forwarded dict.
5. File-reading logic (CSV, Excel, Parquet) preserved as-is.
6. Error handling preserved: `DeltaError` -> `DriverError`.
7. No `engine="rust"` in the call.

## Implementation Notes

- The `create()` method (lines 135-165) reads files into Arrow/Pandas then calls
  `write_deltalake`. The file-reading portion stays unchanged.
- Build an args dict similar to `write()`: `{mode, name, schema_mode, configuration, **kwargs}`,
  filter `None` values, then:
  ```python
  await asyncio.to_thread(write_deltalake, path, data, **args)
  ```
- Consider extracting a shared `_build_write_args()` helper if duplication is excessive,
  but keep it simple — don't over-abstract.

## Files to Create/Modify

- **Modify**: `asyncdb/drivers/delta.py` — rewrite `create()` method (lines 135-165)

## Test Strategy

- Covered by TASK-013 (dedicated test task).
- Smoke test: create a delta table from a Pandas DataFrame with `configuration={"delta.appendOnly": "true"}`.
