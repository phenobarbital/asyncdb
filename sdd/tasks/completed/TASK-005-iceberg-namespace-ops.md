# TASK-005: Namespace Operations

**Feature**: FEAT-002 — apache-iceberg-support
**Status**: pending
**Priority**: high
**Effort**: S
**Depends on**: [TASK-004]

---

## Objective

Implement namespace (database/schema) management methods on the `iceberg` driver.

## Acceptance Criteria

1. `create_namespace(namespace, properties=None)` creates a namespace in the catalog.
2. `list_namespaces()` returns a list of namespace names.
3. `drop_namespace(namespace)` drops a namespace.
4. `namespace_properties(namespace)` returns namespace metadata as a dict.
5. `use(namespace)` sets the default namespace for subsequent operations.
6. All methods are async and wrap PyIceberg calls in `asyncio.to_thread()`.
7. Proper error handling: non-existent namespace raises `DriverError`.

## Implementation Notes

- PyIceberg API: `catalog.create_namespace()`, `catalog.list_namespaces()`, `catalog.drop_namespace()`, `catalog.load_namespace_properties()`.
- The `use()` method sets `self._namespace` as default for table operations.

## Files to Modify

- `asyncdb/drivers/iceberg.py`

## Test Strategy

- Create namespace, verify it appears in list, check properties, drop it, verify removal.
