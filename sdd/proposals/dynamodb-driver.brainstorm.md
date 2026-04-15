# SDD Brainstorm: dynamodb-driver

| Field       | Value                  |
|-------------|------------------------|
| **Feature** | dynamodb-driver        |
| **Status**  | exploration            |
| **Date**    | 2026-04-16             |
| **Author**  | Jesus Lara / Claude    |

---

## 1. Problem Statement

AsyncDB has 30+ async database drivers but no native AWS DynamoDB driver.
DynamoDB access currently requires going through the Iceberg driver's catalog
abstraction, which only supports DynamoDB as a _catalog backend_, not as a
first-class data store. Users need a dedicated DynamoDB driver that follows the
same patterns as existing drivers (connection lifecycle, async context manager,
query/write/execute) while exposing DynamoDB-specific features (PartiQL,
secondary indexes, batch operations, DDL).

### Who Is Affected

- **Developers** using asyncdb who need DynamoDB access in async Python applications.
- **Ops teams** who want consistent connection management patterns across all data stores.

### Constraints

- Must follow the `InitDriver` pattern (like redis, mongo, iceberg drivers).
- Must use `aiobotocore` for low-level async AWS SDK access.
- Must reuse the existing `boto3` optional dependency group in `pyproject.toml`.
- Must auto-deserialize DynamoDB type descriptors to plain Python dicts.
- Must support `self._serializer` (OutputFactory) for output formatting.
- Must support DynamoDB Local (`endpoint_url`) for testing.
- Must include both a single-connection driver (`dynamodb`) and pool driver (`dynamodbPool`).
- Streams and TTL are **out of scope** for the initial driver.

---

## 2. Interactive Discovery Summary

### Round 1 — Scope & Patterns

| Question | Answer |
|----------|--------|
| Base class | `InitDriver` (like redis/mongo), not SQL-oriented |
| Query model | All: CRUD, Query, Scan, PartiQL, Batch ops |
| Underlying library | `aiobotocore` (lower-level, more feature coverage) |
| DDL support | Yes — create/delete/describe/list tables |
| Auth pattern | Via `params` dict, same as other drivers |

### Round 2 — Data Model & API

| Question | Answer |
|----------|--------|
| Return format | Auto-deserialize to Python dicts + support `self._serializer` |
| Method mapping | `execute()` for writes, plus explicit `get`/`set`/`delete`/`write`/`write_batch` like redis |
| Pool driver | Yes — `dynamodbPool` with HTTP connection pooling |
| Local dev | Support `endpoint_url` param for DynamoDB Local |
| Region param | `region_name` as top-level in `params` dict |

### Round 3 — Details

| Question | Answer |
|----------|--------|
| File naming | `asyncdb/drivers/dynamodb.py` with both classes |
| Dependency group | Reuse existing `boto3` extras group |
| PartiQL | Dedicated `partiql()` method AND auto-detection in `execute()` |
| Index support | Full GSI/LSI — query against them and DDL management |
| Streams/TTL | Out of scope |

---

## 3. Code Context

### Verified Base Classes (MUST inherit from)

**`InitDriver`** — `asyncdb/drivers/base.py:33-100`
```
class InitDriver(ConnectionBackend, DatabaseBackend, ABC):
    _provider: str = "init"
    _syntax: str = "init"
    def __init__(self, loop, params, **kwargs)
    def __enter__(self) -> self
    def __exit__(self, *args)
    async def connection_context(self)   # async context manager
    def row_format(self, frmt: str)
    def output_format(self, frmt: str)
    async def valid_operation(self, sentence: Any)
```

**`_KwargsTerminator`** — `asyncdb/drivers/iceberg.py:44-59`
```
class _KwargsTerminator:
    def __init__(self, **kwargs)  # swallows residual kwargs
```
_Note: This should be extracted to a shared location (e.g., `asyncdb/drivers/base.py`) since the DynamoDB driver needs it too._

**`BasePool`** — `asyncdb/drivers/base.py:17-30`
```
class BasePool(PoolBackend, ConnectionDSNBackend, ABC):
    def __init__(self, dsn, loop, params, **kwargs)
```

### Verified Abstract Methods (MUST implement)

**From `DatabaseBackend`** — `asyncdb/interfaces/database.py:38-150`
```
async def use(self, database: str) -> None
async def execute(self, sentence: Any, *args, **kwargs) -> Optional[Any]
async def execute_many(self, sentence: list, *args) -> Optional[Any]
async def query(self, sentence: Union[str, list], **kwargs) -> Optional[Sequence]
async def queryrow(self, sentence: Union[str, list]) -> Optional[Iterable]
async def prepare(self, sentence: Union[str, list]) -> Any
async def fetch_all(self, sentence: str, **kwargs) -> list[Sequence]
async def fetch_one(self, sentence: str, **kwargs) -> Optional[dict]
```

**From `AbstractDriver`** — `asyncdb/interfaces/abstract.py:10-73`
```
async def connection(self) -> Any
async def close(self, timeout: int = 10) -> None
```

**From `PoolBackend`** — `asyncdb/interfaces/pool.py:11-121`
```
async def connect(self) -> "PoolBackend"
async def disconnect(self, timeout: int = 5) -> None
async def acquire(self)
async def release(self, connection, timeout: int = 10) -> None
```

### Verified Factory (auto-discovery pattern)

**`AsyncDB`** — `asyncdb/connections.py:31-44`
```python
classpath = f"asyncdb.drivers.{driver}"  # → "asyncdb.drivers.dynamodb"
mdl = module_exists(driver, classpath)   # → loads class `dynamodb`
```

**`AsyncPool`** — `asyncdb/connections.py:13-28`
```python
pool = f"{driver}Pool"                   # → "dynamodbPool"
mdl = module_exists(pool, classpath)     # → loads class `dynamodbPool`
```

### Verified Dependencies

**`pyproject.toml`** — boto3 optional group (lines 127-129):
```toml
boto3 = [
    "aiobotocore[boto3]==2.15.2",
    "aioboto3==13.2.0"
]
```

### Reference Drivers (patterns to follow)

| Driver | File | Pattern | Relevance |
|--------|------|---------|-----------|
| `redis` | `asyncdb/drivers/redis.py:121` | `BaseDriver` + explicit `get`/`set`/`delete` | API style model |
| `redisPool` | `asyncdb/drivers/redis.py:27` | `BasePool` + HTTP pool | Pool pattern |
| `iceberg` | `asyncdb/drivers/iceberg.py:98` | `InitDriver` + `_KwargsTerminator` + `asyncio.to_thread()` | InitDriver + blocking-call wrapping |
| `mongo` | `asyncdb/drivers/mongo.py:25` | `BaseDriver` + `write()`/`delete()` | NoSQL write pattern |

### Does NOT Exist (prevent hallucination)

- No `asyncdb/drivers/dynamodb.py` file exists.
- No `DynamoDBPool` or `dynamodb` class in any driver file.
- No `_KwargsTerminator` in `base.py` — currently only in `iceberg.py`.
- No shared AWS credential helper or boto session manager in asyncdb.
- `aiobotocore` is NOT directly imported anywhere in the drivers — only referenced as a dependency.

---

## 4. Options

### Option A: Direct aiobotocore Client

Use `aiobotocore.AioSession` to create a raw DynamoDB client. All DynamoDB
API calls go through the low-level client (`create_client("dynamodb")`).
Type deserialization handled by a custom `TypeDeserializer`/`TypeSerializer`
utility within the driver.

**Pros:**
- Full control over every DynamoDB API call.
- No extra abstraction layers — direct mapping to AWS API.
- `aiobotocore` is already a declared dependency.
- Straightforward session/client lifecycle management.
- Connection pooling via `aiohttp.TCPConnector` (built into aiobotocore).

**Cons:**
- Must implement type (de)serialization manually (or use `boto3.dynamodb.types`).
- More boilerplate for marshalling/unmarshalling.
- Must handle pagination manually for Scan/Query.

**Effort:** Medium

**Libraries / Tools:**

| Package | Version | Purpose | Notes |
|---------|---------|---------|-------|
| `aiobotocore` | 2.15.2 | Async AWS SDK (low-level) | Already in `boto3` extras |
| `boto3` | (bundled) | Type serializer/deserializer utilities | `boto3.dynamodb.types` |

**Existing Code to Reuse:**
- `asyncdb/drivers/base.py` — `InitDriver`, `BasePool`
- `asyncdb/drivers/iceberg.py:44-59` — `_KwargsTerminator` mixin
- `asyncdb/drivers/outputs.py` — `OutputFactory` for serialization
- `asyncdb/exceptions.py` — `DriverError`, `ConnectionTimeout`

---

### Option B: aioboto3 Resource-Level API

Use `aioboto3.Session().resource("dynamodb")` which provides a higher-level
Table resource with automatic type marshalling (no manual
serialization/deserialization needed).

**Pros:**
- Built-in type marshalling — no manual (de)serialization.
- Cleaner API for common CRUD operations (Table.get_item, put_item, etc.).
- Less boilerplate code.

**Cons:**
- Resource API does not cover all DynamoDB features (e.g., PartiQL, some DDL).
- Falls back to client API for advanced features → two abstraction levels.
- `aioboto3` resource API is a thin async wrapper; less community battle-testing.
- Harder to control connection pooling at the HTTP level.

**Effort:** Low–Medium

**Libraries / Tools:**

| Package | Version | Purpose | Notes |
|---------|---------|---------|-------|
| `aioboto3` | 13.2.0 | Async high-level AWS SDK | Already in `boto3` extras |
| `aiobotocore` | 2.15.2 | Underlying transport | Pulled in by aioboto3 |

**Existing Code to Reuse:**
- Same as Option A.

---

### Option C: Hybrid — aiobotocore Client + boto3 Type Utilities

Use `aiobotocore` for the async client (full API coverage) but leverage
`boto3.dynamodb.types.TypeDeserializer` and `TypeSerializer` for
marshalling/unmarshalling. This combines the full control of the low-level
client with the convenience of boto3's type system.

**Pros:**
- Full DynamoDB API coverage (PartiQL, GSI/LSI DDL, batch ops).
- Proven type (de)serialization from boto3 — no reinventing the wheel.
- Clean session lifecycle via `aiobotocore.AioSession`.
- HTTP connection pooling via `aiohttp.TCPConnector` parameters.
- Single abstraction level — everything goes through one client.

**Cons:**
- Slightly more complex than Option B for simple CRUD.
- Pagination still manual (but can be abstracted into helper methods).
- `boto3.dynamodb.types` is sync but pure-Python — no blocking I/O concern.

**Effort:** Medium

**Libraries / Tools:**

| Package | Version | Purpose | Notes |
|---------|---------|---------|-------|
| `aiobotocore` | 2.15.2 | Async DynamoDB client | Already in `boto3` extras |
| `boto3.dynamodb.types` | (bundled) | TypeSerializer / TypeDeserializer | Sync, pure-Python, no I/O |

**Existing Code to Reuse:**
- Same as Option A, plus `boto3.dynamodb.types` for marshalling.

---

## 5. Recommendation

**Option C — Hybrid (aiobotocore + boto3 Type Utilities)** is the recommended approach.

### Reasoning

1. **Full API coverage**: Option B (aioboto3 resource) cannot cover PartiQL
   or some DDL operations without falling back to the client API anyway. Since
   we need PartiQL, GSI/LSI DDL, and batch operations, we'd end up with a
   hybrid in Option B regardless — but an inconsistent one.

2. **Type handling**: Option A requires writing custom (de)serialization.
   `boto3.dynamodb.types.TypeDeserializer` is battle-tested, pure-Python, and
   has zero I/O — using it costs nothing and prevents bugs.

3. **Connection pooling**: `aiobotocore` exposes `aiohttp.TCPConnector`
   parameters, giving us direct control over max connections, keepalive, and
   timeouts — exactly what `dynamodbPool` needs.

4. **Consistency**: The iceberg driver already uses the pattern of wrapping
   blocking calls via `asyncio.to_thread()`. Here, `aiobotocore` is natively
   async so we don't even need that — it's a cleaner fit.

**Tradeoff accepted**: Slightly more boilerplate than Option B for simple CRUD,
but we gain full feature coverage and a single consistent abstraction.

---

## 6. Feature Description

### User-Facing Behavior

```python
from asyncdb import AsyncDB, AsyncPool

# Single connection
db = AsyncDB("dynamodb", params={
    "aws_access_key_id": "...",
    "aws_secret_access_key": "...",
    "region_name": "us-east-1",
    "endpoint_url": "http://localhost:8000",  # optional, DynamoDB Local
})
async with db as conn:
    # Get item
    item = await conn.get("MyTable", {"pk": "user-123"})

    # Put item
    await conn.set("MyTable", {"pk": "user-123", "name": "Alice", "age": 30})

    # Query by partition key
    results = await conn.query("MyTable", KeyConditionExpression="pk = :pk",
                               ExpressionAttributeValues={":pk": "user-123"})

    # PartiQL
    rows = await conn.partiql("SELECT * FROM MyTable WHERE pk = ?", parameters=["user-123"])

    # Batch write
    await conn.write_batch("MyTable", [
        {"pk": "u1", "name": "Alice"},
        {"pk": "u2", "name": "Bob"},
    ])

    # Scan (full table)
    all_items = await conn.fetch_all("MyTable")

    # DDL
    await conn.create_table("NewTable", key_schema=[...], attribute_definitions=[...])
    tables = await conn.tables()

# Pool connection
pool = AsyncPool("dynamodb", params={...})
async with pool as p:
    conn = await p.acquire()
    try:
        item = await conn.get("MyTable", {"pk": "user-123"})
    finally:
        await p.release(conn)
```

### Internal Behavior

1. **Connection**: Creates `aiobotocore.AioSession`, calls
   `session.create_client("dynamodb", region_name=..., endpoint_url=..., ...)`.
   Stores client as `self._connection`.

2. **Type Marshalling**: All outgoing data passes through
   `boto3.dynamodb.types.TypeSerializer`. All incoming data passes through
   `TypeDeserializer`. This happens transparently inside each method.

3. **Method Mapping**:
   - `get(table, key)` → `client.get_item()` → deserialize → return dict
   - `set(table, item)` → serialize → `client.put_item()`
   - `delete(table, key)` → `client.delete_item()`
   - `write(table, item)` → alias for `set()`
   - `write_batch(table, items)` → `client.batch_write_item()`
   - `query(table, **kwargs)` → `client.query()` → deserialize → return list[dict]
   - `queryrow(table, **kwargs)` → `client.query(Limit=1)` → deserialize → return dict
   - `execute(sentence, *args)` → PartiQL if string looks like SQL, else `put_item`
   - `partiql(statement, parameters)` → `client.execute_statement()`
   - `fetch_all(table, **kwargs)` → `client.scan()` with auto-pagination → return list[dict]
   - `fetch_one(table, key)` → alias for `get()`
   - `tables()` → `client.list_tables()`
   - `create_table(...)` → `client.create_table()`
   - `delete_table(table)` → `client.delete_table()`
   - `describe_table(table)` → `client.describe_table()`

4. **Pool**: `dynamodbPool` manages an `aiobotocore.AioSession` with a shared
   `aiohttp.TCPConnector(limit=N)`. `acquire()` creates a new `dynamodb`
   driver instance sharing the session. `release()` closes the individual client.

5. **Output Serialization**: After type deserialization, results pass through
   `self._serializer` (OutputFactory) if configured, matching the pattern
   used by all other drivers.

### Edge Cases & Error Handling

- **Pagination**: `query()` and `fetch_all()` auto-paginate using `LastEvaluatedKey`.
- **Conditional writes**: Support `ConditionExpression` via kwargs pass-through.
- **Throughput exceptions**: Catch `ProvisionedThroughputExceededException` and
  wrap as `DriverError` with descriptive message.
- **Auth failures**: Catch `ClientError` with `UnrecognizedClientException` or
  `AccessDeniedException` → `ConnectionTimeout` or `DriverError`.
- **DynamoDB Local**: When `endpoint_url` is set, skip SSL verification.
- **Empty results**: `get()` returns `None` when item not found (not an error).
- **Batch limits**: `write_batch()` auto-chunks into groups of 25 (DynamoDB limit).

---

## 7. Capabilities

### New Capabilities

| ID | Description |
|----|-------------|
| `dynamodb-connection` | Async connection lifecycle via aiobotocore |
| `dynamodb-crud` | get/set/delete/write single-item operations |
| `dynamodb-query` | Query by partition key + sort key conditions |
| `dynamodb-scan` | Full-table scan with auto-pagination |
| `dynamodb-partiql` | PartiQL statement execution |
| `dynamodb-batch` | Batch write/read operations |
| `dynamodb-ddl` | Table create/delete/describe/list + GSI/LSI management |
| `dynamodb-pool` | HTTP connection pooling via dynamodbPool |
| `dynamodb-type-marshal` | Transparent Python ↔ DynamoDB type conversion |

### Modified Capabilities

| ID | Change |
|----|--------|
| `driver-factory` | `AsyncDB("dynamodb")` and `AsyncPool("dynamodb")` auto-discovery |

---

## 8. Impact & Integration

| Component | File | Impact | Notes |
|-----------|------|--------|-------|
| New driver | `asyncdb/drivers/dynamodb.py` | **New file** | Main driver + pool class |
| Factory | `asyncdb/connections.py` | None (auto-discovery) | No changes needed |
| Base classes | `asyncdb/drivers/base.py` | Minor | Extract `_KwargsTerminator` here |
| Dependencies | `pyproject.toml` | None | Reuse `boto3` extras group |
| Tests | `tests/test_dynamodb.py` | **New file** | DynamoDB Local tests |
| Iceberg driver | `asyncdb/drivers/iceberg.py` | Minor | Import `_KwargsTerminator` from `base.py` |

---

## 9. Open Questions

| # | Question | Owner | Priority |
|---|----------|-------|----------|
| 1 | Should `_KwargsTerminator` be extracted to `base.py` now, or duplicated in the DynamoDB driver? | Dev team | Medium |
| 2 | Should batch read (`batch_get_item`) be a separate `read_batch()` method or overloaded on `get()`? | Dev team | Low |
| 3 | Should GSI/LSI query support be via a `query(IndexName=...)` kwarg pass-through or a dedicated `query_index()` method? | Dev team | Medium |
| 4 | What DynamoDB Local Docker image version to pin for CI tests? | DevOps | Low |

---

## 10. Parallelism Assessment

- **Internal parallelism**: **Yes** — tasks can be split:
  - Connection lifecycle + type marshalling (core)
  - CRUD operations (`get`/`set`/`delete`/`write`)
  - Query/Scan operations
  - PartiQL support
  - Batch operations
  - DDL operations (create/delete/describe tables, GSI/LSI)
  - Pool driver (`dynamodbPool`)
  - Tests (depends on all above)

- **Cross-feature independence**: No conflicts with in-flight specs. The only
  shared file is `base.py` if we extract `_KwargsTerminator`.

- **Recommended isolation**: `per-spec` — all tasks in one worktree. The driver
  is a single file with tightly coupled components (type marshalling is used by
  every method). Splitting across worktrees would create merge conflicts.

- **Rationale**: While capabilities are logically separable, they all live in
  one file (`dynamodb.py`) and share the connection/session/marshalling
  infrastructure. Sequential implementation in one worktree avoids conflicts
  and allows natural incremental testing.
