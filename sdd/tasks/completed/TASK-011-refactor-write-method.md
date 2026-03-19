# TASK-011: Refactor `write()` Method with Full delta-rs API Surface

**Feature**: FEAT-003 — migrate-delta-driver
**Spec**: sdd/specs/migrate-delta-driver.spec.md
**Status**: pending
**Priority**: high
**Effort**: M
**Depends on**: none

---

## Objective

Replace the current `write()` method in `asyncdb/drivers/delta.py` with a new
implementation that exposes the full `write_deltalake` parameter surface: `mode`,
`schema_mode`, `configuration`, `predicate`, `partition_by`, `storage_options`,
`name`, `description`. Unify all data paths (Pandas, Polars, Arrow) through
`write_deltalake`. Wrap the blocking call in `asyncio.to_thread()`.

## Acceptance Criteria

1. `write()` signature matches the spec (keyword-only args after `path`).
2. `mode` parameter replaces `if_exists` as the primary mode selector.
3. `if_exists` still works but emits `DeprecationWarning` via `warnings.warn()`.
4. `schema_mode` parameter accepted and forwarded (`"merge"` / `"overwrite"` / `None`).
5. `configuration` parameter accepted and forwarded as `dict[str, str | None]`.
6. `predicate` parameter accepted and forwarded for targeted overwrites.
7. `storage_options` parameter accepted; defaults to `self.storage_options` when `None`.
8. `name` and `description` forwarded to `write_deltalake`.
9. Polars DataFrames converted via `.to_arrow()` before calling `write_deltalake`
   (no more `pl.DataFrame.write_delta` code path).
10. `engine="rust"` is NOT passed (removed).
11. `write_deltalake` call wrapped in `await asyncio.to_thread(...)`.
12. `None`-valued optional args are excluded from the call (not forwarded as `None`).
13. `**kwargs` forwarded for advanced options (`writer_properties`, `target_file_size`, etc.).
14. Error handling preserved: `DeltaError` / `DeltaProtocolError` -> `DriverError`.
15. `import warnings` added at top of file.

## Implementation Notes

- Add `import warnings` to the imports section.
- The `write()` method should build an args dict, filter out `None` values, then call:
  ```python
  await asyncio.to_thread(write_deltalake, destination, data, **args)
  ```
- For `storage_options`: use `storage_options or self.storage_options` so the
  driver's default options are used when caller doesn't override.
- Convert `pl.DataFrame` -> Arrow: `data = data.to_arrow()`
- Keep `self._delta = destination` after successful write.

## Files to Create/Modify

- **Modify**: `asyncdb/drivers/delta.py` — rewrite `write()` method (lines 376-412)

## Test Strategy

- Covered by TASK-013 (dedicated test task).
- Manual smoke test: write a Pandas DataFrame with `mode="overwrite"` and `schema_mode="merge"`.
