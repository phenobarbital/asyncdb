# FEAT-003: Migrate Delta Driver to delta-rs

**Title**: Migrate Delta Driver to delta-rs
**Status**: approved
**Created**: 2026-03-19
**Author**: Jesus Lara

---

## Problem Statement

The current `delta` driver (`asyncdb/drivers/delta.py`) has a `write()` method with a
limited interface that does not expose the full power of the `write_deltalake` function
from the `deltalake` (delta-rs) library. Key gaps:

1. **Missing `schema_mode`** — no support for `merge` or `overwrite` schema evolution.
2. **Missing `configuration`** — cannot pass Delta table configuration metadata
   (e.g., `delta.appendOnly`, `delta.logRetentionDuration`).
3. **Deprecated `engine` parameter** — the current code passes `engine="rust"`, which is
   no longer needed since delta-rs is the only engine.
4. **Inconsistent mode naming** — the driver uses `if_exists` instead of the canonical
   `mode` parameter name from `write_deltalake`.
5. **Missing advanced writer options** — `predicate` (targeted overwrites),
   `writer_properties`, `target_file_size`, `name`, `description` are not exposed.
6. **Polars code path divergence** — Polars `write_delta` is called separately with
   different semantics; the driver should unify all paths through `write_deltalake`
   after converting Polars DataFrames to Arrow.

## Proposed Solution

Refactor the `write()` method to be a thin, well-typed wrapper around `write_deltalake`,
exposing all parameters from the delta-rs API while maintaining backward compatibility.

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Unify all data paths through `write_deltalake` | Polars DataFrames convert to Arrow trivially via `.to_arrow()`; a single code path is easier to maintain and test. |
| Keep `if_exists` as deprecated alias for `mode` | Backward compatibility — existing callers use `if_exists`. Emit a `DeprecationWarning`. |
| Remove `engine="rust"` | The `engine` parameter was removed in deltalake >= 0.18. |
| Expose `schema_mode`, `configuration`, `predicate` as first-class params | These are the most commonly needed delta-rs features beyond basic writes. |
| Pass remaining options via `**kwargs` | Advanced options (`writer_properties`, `commit_properties`, `post_commithook_properties`, `target_file_size`) are forwarded without wrapping, keeping the driver thin. |
| Wrap blocking `write_deltalake` in `asyncio.to_thread` | The driver is async but `write_deltalake` is synchronous; wrapping prevents event loop blocking for large writes. |
| Also refactor `create()` to align | `create()` also calls `write_deltalake` — it should use the same unified logic. |

---

## Detailed Design

### File Layout

| File | Action | Description |
|------|--------|-------------|
| `asyncdb/drivers/delta.py` | **Modify** | Refactor `write()` and `create()` methods |
| `tests/test_delta_write.py` | **Create** | Unit tests for the new write interface |
| `examples/test_delta.py` | **Modify** | Update example to demonstrate new write options |

### Method Signatures

#### New `write()` signature

```python
async def write(
    self,
    data: Union[pd.DataFrame, pl.DataFrame, Table, Iterable],
    table_id: str,
    path: Union[str, Path, PurePath],
    *,
    mode: Literal["error", "append", "overwrite", "ignore"] = "append",
    schema_mode: Optional[Literal["merge", "overwrite"]] = None,
    partition_by: Optional[Union[list[str], str]] = None,
    configuration: Optional[dict[str, Optional[str]]] = None,
    storage_options: Optional[dict[str, str]] = None,
    predicate: Optional[str] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    # Deprecated
    if_exists: Optional[str] = None,
    **kwargs,
) -> None:
    """Write data to a Delta Table.

    Wraps `deltalake.write_deltalake` with full parameter support.

    Args:
        data: Input data — Pandas DataFrame, Polars DataFrame,
              PyArrow Table, or any Arrow-compatible iterable.
        table_id: Table name (appended to path as subdirectory).
        path: Root path for the Delta Table.
        mode: Write mode — 'error', 'append', 'overwrite', 'ignore'.
        schema_mode: Schema evolution — 'merge' or 'overwrite'.
        partition_by: Column(s) to partition by.
        configuration: Delta table configuration metadata.
        storage_options: Storage backend options (S3, Azure, GCS).
        predicate: SQL predicate for targeted overwrite.
        name: User-provided table identifier in metadata.
        description: User-provided table description in metadata.
        if_exists: **Deprecated** — use `mode` instead.
        **kwargs: Additional arguments forwarded to `write_deltalake`
                  (e.g., writer_properties, target_file_size,
                  commit_properties, post_commithook_properties).

    Raises:
        DriverError: On any delta write failure.
        DeprecationWarning: When `if_exists` is used instead of `mode`.
    """
```

#### Refactored `create()` signature

```python
async def create(
    self,
    path: Union[str, Path],
    data: Any,
    name: Optional[str] = None,
    mode: str = "append",
    schema_mode: Optional[str] = None,
    configuration: Optional[dict[str, Optional[str]]] = None,
    **kwargs,
) -> None:
```

### Internal Logic — `write()`

```
1. Handle deprecated `if_exists`:
   - If `if_exists` is set and `mode` was not explicitly changed from default:
     warnings.warn("if_exists is deprecated, use mode", DeprecationWarning)
     mode = if_exists

2. Normalize data to Arrow:
   - pd.DataFrame → passed directly (write_deltalake handles it)
   - pl.DataFrame → data.to_arrow()
   - PyArrow Table / Iterable → passed directly

3. Build destination:
   destination = Path(path) / table_id

4. Build args dict:
   args = {mode, schema_mode, partition_by, configuration,
           storage_options, predicate, name, description, **kwargs}
   (omit None values)

5. Execute write in thread:
   await asyncio.to_thread(write_deltalake, destination, data, **args)

6. Update self._delta = destination
```

### Dependencies

| Package | Version | Notes |
|---------|---------|-------|
| `deltalake` | `>=0.19.2` | Already in `pyproject.toml` — no change needed |
| `pyarrow` | `>=19.0.1` | Already present |
| `polars` | `>=1.12.0` | Already present (optional) |
| `pandas` | existing | Already present |

No new dependencies required.

### Integration Points

- **`InitDriver` base class** — no changes needed; `write()` is driver-specific.
- **`create()` method** — will share the same `write_deltalake` call path.
- **`connection()` / `storage_options`** — the new `storage_options` param on `write()`
  defaults to `self.storage_options` when not explicitly provided, allowing per-write
  override.

---

## Acceptance Criteria

1. **New `write()` method** supports `mode`, `schema_mode`, `configuration`, `predicate`,
   `partition_by`, `storage_options`, `name`, `description` parameters.
2. **Backward compatible** — existing calls using `if_exists` still work with a
   deprecation warning.
3. **Polars path unified** — Polars DataFrames are converted to Arrow and written via
   `write_deltalake` (not `pl.DataFrame.write_delta`).
4. **Async-safe** — `write_deltalake` is wrapped in `asyncio.to_thread()`.
5. **`create()` refactored** — uses the same parameter surface as `write()`.
6. **`engine="rust"` removed** — no longer passed.
7. **Unit tests** cover: append, overwrite, ignore, error modes; schema_mode merge;
   configuration passing; deprecation warning on `if_exists`; Polars and Pandas input.
8. **Example updated** — `examples/test_delta.py` demonstrates new parameters.

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Breaking existing callers using `if_exists` | Medium | Keep `if_exists` as deprecated alias; emit `DeprecationWarning` |
| `asyncio.to_thread` overhead for small writes | Low | Negligible for real workloads; benefits outweigh cost for large writes |
| Polars `.to_arrow()` memory copy | Low | Arrow uses zero-copy where possible; Polars interop is efficient |
| `schema_mode="merge"` may conflict with partition changes | Medium | Document limitation; delta-rs handles this with clear errors |

---

## Out of Scope

- Streaming / incremental write (CDC) support.
- Merge (upsert) operations via `DeltaTable.merge()` — separate feature.
- Vacuum / optimize / compaction operations.
- Changes to `query()`, `get()`, `to_df()`, or read-side methods.
- S3/Azure/GCS credential management changes.

---

## Worktree Strategy

- **Isolation unit**: `per-spec` (sequential tasks).
- **Rationale**: All tasks modify the same file (`delta.py`) and the test file depends
  on the refactored write method. Sequential execution avoids merge conflicts.
- **Cross-feature dependencies**: None. FEAT-001 (exception migration) may change
  `DriverError` import path, but the class interface is stable.

---

## Dependencies

- No blocking dependencies on other features.
- If FEAT-001 (exception migration) lands first, update import path for `DriverError`
  accordingly — but the migration preserves the same class API.
