# TASK-009: Metadata, Snapshots & History

**Feature**: FEAT-002 — apache-iceberg-support
**Status**: pending
**Priority**: medium
**Effort**: S
**Depends on**: [TASK-006]

---

## Objective

Implement metadata inspection methods: snapshot history, current snapshot, table properties, and partition spec inspection.

## Acceptance Criteria

1. `history(table_id)` returns a list of snapshot dicts with `snapshot_id`, `timestamp_ms`, `parent_id`, `summary`.
2. `snapshots(table_id)` returns the raw list of snapshot objects.
3. `current_snapshot(table_id)` returns the current snapshot info as a dict.
4. `metadata(table_id)` returns a dict with `location`, `properties`, `schema`, `partition_spec`, `snapshot_count`, `current_snapshot_id`.
5. All methods async.
6. Returns empty lists/dicts gracefully for tables with no snapshots.

## Implementation Notes

- PyIceberg: `table.metadata`, `table.current_snapshot()`, `table.metadata.snapshots`.
- Snapshot summary contains operation type, added/deleted files count, etc.

## Files to Modify

- `asyncdb/drivers/iceberg.py`

## Test Strategy

- Create table, write data (creates snapshot), verify history has 1 entry, verify current_snapshot matches.
