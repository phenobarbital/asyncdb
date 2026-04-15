# FEAT-004: dynamodb-driver

**Title**: AWS DynamoDB Async Driver for AsyncDB
**Status**: approved
**Created**: 2026-04-16
**Author**: Jesus Lara
**Brainstorm**: `sdd/proposals/dynamodb-driver.brainstorm.md`

---

## 1. Problem Statement

AsyncDB has 30+ async database drivers but no native AWS DynamoDB driver. DynamoDB
access currently only exists as a catalog backend for the Iceberg driver — not as a
first-class data store. Users need a dedicated DynamoDB driver that follows existing
asyncdb patterns (connection lifecycle, async context manager, query/write/execute)
while exposing DynamoDB-specific features: PartiQL, secondary indexes, batch
operations, and DDL.

### Business Requirements

1. Developers using asyncdb must be able to access DynamoDB with the same interface
   patterns they use for Redis, MongoDB, and other NoSQL drivers.
2. The driver must support all core DynamoDB operations: CRUD, Query, Scan, PartiQL,
   batch reads/writes, and table DDL (including GSI/LSI management).
3. Must integrate with the existing factory pattern (`AsyncDB("dynamodb")` and
   `AsyncPool("dynamodb")`).
4. Must support DynamoDB Local for development and testing.

---

## 2. Architectural Design

### Overview

Implement a new `dynamodb` driver at `asyncdb/drivers/dynamodb.py` using
`aiobotocore` for native async DynamoDB access and `boto3.dynamodb.types` for
transparent Python-to-DynamoDB type marshalling.

### Key Design Decisions

1. **Base class**: `InitDriver` (not `BaseDriver` or `SQLDriver`) — DynamoDB is a
   NoSQL key-value/document store. No DSN needed; configuration via `params` dict.
   Uses `_KwargsTerminator` mixin for cooperative MRO (same pattern as `iceberg`).

2. **Async library**: `aiobotocore` (low-level, full API coverage). The driver
   creates an `aiobotocore.AioSession` and calls `session.create_client("dynamodb")`
   for a natively async client. No `asyncio.to_thread()` wrapping needed.

3. **Type marshalling**: `boto3.dynamodb.types.TypeSerializer` and
   `TypeDeserializer` for transparent Python dict ↔ DynamoDB type conversion.
   These are pure-Python, no I/O — safe to call synchronously.

4. **Method style**: Redis-like explicit methods (`get`, `set`, `delete`, `write`,
   `write_batch`) alongside the abstract interface methods (`query`, `execute`,
   `fetch_all`, etc.).

5. **PartiQL**: Dedicated `partiql(statement, parameters)` method AND auto-detection
   in `execute()` — when the sentence is a string that looks like SQL (starts with
   SELECT/INSERT/UPDATE/DELETE), route to PartiQL `execute_statement`.

6. **Pool driver**: `dynamodbPool` extends `BasePool`. Manages a shared
   `aiobotocore.AioSession` with `aiohttp.TCPConnector(limit=N)` for HTTP connection
   pooling. `acquire()` returns a `dynamodb` driver instance sharing the session.

### Class Hierarchy

```
InitDriver + _KwargsTerminator
  └── dynamodb          (single-connection driver)

BasePool
  └── dynamodbPool      (HTTP connection pool)
```

### Provider Metadata

```python
_provider = "dynamodb"
_syntax = "nosql"
```

---

## 3. Module Breakdown

### 3.1 Constructor Parameters

| Parameter | Type | Description | Required |
|-----------|------|-------------|----------|
| `params["aws_access_key_id"]` | `str` | AWS access key | No (falls back to env/profile) |
| `params["aws_secret_access_key"]` | `str` | AWS secret key | No (falls back to env/profile) |
| `params["region_name"]` | `str` | AWS region (e.g., `"us-east-1"`) | Yes |
| `params["endpoint_url"]` | `str` | Custom endpoint (DynamoDB Local) | No |
| `params["aws_session_token"]` | `str` | Temporary session token | No |
| `params["profile_name"]` | `str` | AWS profile from `~/.aws/credentials` | No |
| `params["table"]` | `str` | Default table name | No |
| `params["max_pool_connections"]` | `int` | HTTP connector pool size (pool driver) | No (default: 10) |

### 3.2 Connection Lifecycle

```python
async def connection(self, **kwargs) -> "dynamodb":
    """Create aiobotocore session and DynamoDB client."""

async def close(self, timeout: int = 10) -> None:
    """Close the DynamoDB client."""

async def __aenter__(self) -> "dynamodb":
    """Async context manager entry — calls connection() if needed."""

async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
    """Async context manager exit — calls close()."""
```

### 3.3 CRUD Operations (Redis-style)

```python
async def get(self, table: str, key: dict, **kwargs) -> Optional[dict]:
    """Get a single item by primary key. Returns deserialized dict or None."""

async def set(self, table: str, item: dict, **kwargs) -> bool:
    """Put an item into a table. Returns True on success."""

async def delete(self, table: str, key: dict, **kwargs) -> bool:
    """Delete an item by primary key. Returns True on success."""

async def write(self, table: str, item: dict, **kwargs) -> bool:
    """Alias for set() — write a single item."""

async def update(self, table: str, key: dict, update_expression: str,
                 expression_values: dict = None, **kwargs) -> Optional[dict]:
    """Update an item with an UpdateExpression. Returns updated attributes."""
```

### 3.4 Batch Operations

```python
async def write_batch(self, table: str, items: list[dict], **kwargs) -> bool:
    """Batch write items. Auto-chunks into groups of 25 (DynamoDB limit).
    Handles UnprocessedItems with retry."""

async def get_batch(self, table: str, keys: list[dict], **kwargs) -> list[dict]:
    """Batch get items by keys. Auto-chunks into groups of 100."""
```

### 3.5 Query & Scan (Abstract Interface Implementation)

```python
async def query(self, table: str, **kwargs) -> Optional[list[dict]]:
    """Query by partition key + optional sort key conditions.
    Supports IndexName kwarg for GSI/LSI queries.
    Auto-paginates using LastEvaluatedKey."""

async def queryrow(self, table: str, **kwargs) -> Optional[dict]:
    """Query returning a single item (Limit=1)."""

async def fetch_all(self, table: str, **kwargs) -> list[dict]:
    """Full table scan with auto-pagination."""

async def fetch_one(self, table: str, key: dict = None, **kwargs) -> Optional[dict]:
    """Alias for get() — fetch single item by key."""

async def execute(self, sentence: Any, *args, **kwargs) -> Optional[Any]:
    """Smart execute: if sentence is a SQL-like string, routes to PartiQL.
    Otherwise treats as put_item (write operation)."""

async def execute_many(self, sentences: list, *args) -> Optional[Any]:
    """Execute multiple PartiQL statements via batch_execute_statement."""
```

### 3.6 PartiQL Support

```python
async def partiql(self, statement: str, parameters: list = None,
                  **kwargs) -> Optional[list[dict]]:
    """Execute a PartiQL statement via execute_statement API.
    Returns deserialized results."""

async def partiql_batch(self, statements: list[dict],
                        **kwargs) -> Optional[list[dict]]:
    """Execute multiple PartiQL statements via batch_execute_statement."""
```

### 3.7 DDL Operations

```python
async def create_table(self, table: str, key_schema: list[dict],
                       attribute_definitions: list[dict],
                       billing_mode: str = "PAY_PER_REQUEST",
                       **kwargs) -> dict:
    """Create a DynamoDB table. Supports GSI/LSI via kwargs
    (GlobalSecondaryIndexes, LocalSecondaryIndexes)."""

async def delete_table(self, table: str) -> bool:
    """Delete a DynamoDB table."""

async def describe_table(self, table: str) -> dict:
    """Describe a table — returns full table metadata."""

async def tables(self, **kwargs) -> list[str]:
    """List all tables in the account/region."""

async def table(self, tablename: str = "") -> dict:
    """Alias for describe_table()."""

async def wait_for_table(self, table: str, status: str = "ACTIVE",
                         timeout: int = 300) -> bool:
    """Poll until table reaches the desired status."""

async def use(self, database: str) -> None:
    """Set the default table name for subsequent operations."""
```

### 3.8 Index Management (GSI/LSI)

```python
async def create_index(self, table: str, index_name: str,
                       key_schema: list[dict],
                       projection: dict,
                       **kwargs) -> dict:
    """Create a Global Secondary Index on an existing table
    via update_table."""

async def delete_index(self, table: str, index_name: str) -> dict:
    """Delete a Global Secondary Index via update_table."""

async def list_indexes(self, table: str) -> list[dict]:
    """List all GSIs and LSIs on a table (from describe_table)."""
```

### 3.9 Pool Driver: `dynamodbPool`

```python
class dynamodbPool(BasePool):
    """Connection pool for DynamoDB using a shared aiobotocore session."""

    async def connect(self, **kwargs) -> "dynamodbPool":
        """Create shared AioSession with configured TCPConnector."""

    async def disconnect(self, timeout: int = 5) -> None:
        """Close all client connections and the session."""

    async def acquire(self) -> "dynamodb":
        """Create a new dynamodb driver instance sharing the session."""

    async def release(self, connection=None, timeout: int = 10) -> None:
        """Close the individual client returned by acquire()."""
```

### 3.10 Internal Utilities

```python
def _serialize(self, item: dict) -> dict:
    """Serialize a Python dict to DynamoDB format using TypeSerializer."""

def _deserialize(self, item: dict) -> dict:
    """Deserialize a DynamoDB item to a plain Python dict using TypeDeserializer."""

def _deserialize_items(self, items: list[dict]) -> list[dict]:
    """Deserialize a list of DynamoDB items."""

def _is_partiql(self, sentence: Any) -> bool:
    """Detect if a sentence string looks like PartiQL/SQL."""

async def _paginate_query(self, method: str, table: str, **kwargs) -> list[dict]:
    """Auto-paginate Query or Scan operations using LastEvaluatedKey."""

def _chunk_items(self, items: list, chunk_size: int = 25) -> list[list]:
    """Split items into chunks for batch operations."""
```

---

## 4. Integration Points

### Factory Auto-Discovery (no changes needed)

```python
# asyncdb/connections.py — existing code handles this automatically:
# AsyncDB("dynamodb") → loads asyncdb.drivers.dynamodb.dynamodb
# AsyncPool("dynamodb") → loads asyncdb.drivers.dynamodb.dynamodbPool
```

### Output Serialization

The driver uses `self._serializer` (inherited from `InitDriver`) for output
formatting. When `output_format("native")` (default), the driver returns plain
Python dicts. Other formats (json, pandas, etc.) are handled by `OutputFactory`.

### Error Mapping

| AWS/boto Exception | asyncdb Exception |
|-------------------|-------------------|
| `ClientError` (AccessDenied, UnrecognizedClient) | `ConnectionTimeout` |
| `ClientError` (ResourceNotFoundException) | `DriverError` |
| `ClientError` (ConditionalCheckFailedException) | `DriverError` |
| `ClientError` (ProvisionedThroughputExceededException) | `DriverError` |
| `EndpointConnectionError` | `ConnectionTimeout` |
| `BotoCoreError` (generic) | `DriverError` |
| Empty key / missing table | `EmptyStatement` |

---

## 5. Acceptance Criteria

### Must Have

- [ ] `AsyncDB("dynamodb", params={...})` creates a working driver instance.
- [ ] `AsyncPool("dynamodb", params={...})` creates a working pool instance.
- [ ] Async context manager (`async with`) works for both driver and pool.
- [ ] `get()`, `set()`, `delete()`, `write()`, `update()` work for single items.
- [ ] `write_batch()` handles batches >25 items with auto-chunking.
- [ ] `get_batch()` handles batches >100 keys with auto-chunking.
- [ ] `query()` supports KeyConditionExpression and auto-pagination.
- [ ] `query()` supports `IndexName` kwarg for GSI/LSI queries.
- [ ] `fetch_all()` performs full-table scan with auto-pagination.
- [ ] `fetch_one()` retrieves a single item by key.
- [ ] `execute()` auto-detects PartiQL statements.
- [ ] `partiql()` executes PartiQL statements with parameters.
- [ ] `create_table()`, `delete_table()`, `describe_table()`, `tables()` work.
- [ ] `create_index()` and `delete_index()` manage GSIs.
- [ ] DynamoDB items are auto-deserialized to plain Python dicts.
- [ ] `self._serializer` output formatting is supported.
- [ ] `endpoint_url` parameter enables DynamoDB Local connections.
- [ ] All tests pass against DynamoDB Local.
- [ ] Type hints on all public methods.
- [ ] Google-style docstrings on all public methods.

### Should Have

- [ ] `wait_for_table()` polls until table is ACTIVE.
- [ ] `list_indexes()` returns GSI/LSI metadata.
- [ ] `execute_many()` uses `batch_execute_statement` for PartiQL.
- [ ] `partiql_batch()` for batch PartiQL execution.
- [ ] `test_connection()` verifies connectivity.
- [ ] Proper logging via `self._logger`.

### Won't Have (out of scope)

- DynamoDB Streams support.
- TTL configuration.
- DAX (DynamoDB Accelerator) integration.
- Schema evolution / migration tools.
- Automatic retry with exponential backoff (rely on aiobotocore defaults).

---

## 6. Codebase Contract

### Verified Base Classes

**`InitDriver`** — `asyncdb/drivers/base.py:33-100`
- Inherits: `ConnectionBackend`, `DatabaseBackend`, `ABC`
- `__init__(self, loop: Union[asyncio.AbstractEventLoop, None] = None, params: Union[dict, None] = None, **kwargs)` (line 43)
- Sets up: `self._max_connections`, `self._parameters`, `self._serializer`, `self._row_format`
- Calls `self.output_format("native")` in `__init__`
- Provides: `__enter__`, `__exit__`, `connection_context()`, `row_format()`, `output_format()`, `valid_operation()`

**`BasePool`** — `asyncdb/drivers/base.py:17-30`
- Inherits: `PoolBackend`, `ConnectionDSNBackend`, `ABC`
- `__init__(self, dsn: Union[str, None] = None, loop = None, params: Optional[Union[dict, None]] = None, **kwargs)` (line 23)

**`_KwargsTerminator`** — `asyncdb/drivers/iceberg.py:44-59`
- `__init__(self, **kwargs)` — swallows residual kwargs, calls `super().__init__()`
- Required for `InitDriver`-only drivers to prevent `TypeError` from kwargs leaking to `object.__init__()`

### Verified Abstract Methods (MUST implement)

**From `DatabaseBackend`** — `asyncdb/interfaces/database.py:38-150`
- `async def use(self, database: str) -> None` (line 78)
- `async def execute(self, sentence: Any, *args, **kwargs) -> Optional[Any]` (line 85)
- `async def execute_many(self, sentence: list, *args) -> Optional[Any]` (line 92)
- `async def query(self, sentence: Union[str, list], **kwargs) -> Optional[Sequence]` (line 98)
- `async def queryrow(self, sentence: Union[str, list]) -> Optional[Iterable]` (line 111)
- `async def prepare(self, sentence: Union[str, list]) -> Any` (line 123)
- `async def fetch_all(self, sentence: str, **kwargs) -> list[Sequence]` (line 135)
- `async def fetch_one(self, sentence: str, **kwargs) -> Optional[dict]` (line 139)

**From `AbstractDriver`** — `asyncdb/interfaces/abstract.py:10-73`
- `async def connection(self) -> Any` (line 37)
- `async def close(self, timeout: int = 10) -> None` (line 46)

**From `PoolBackend`** — `asyncdb/interfaces/pool.py:11-121`
- `async def connect(self) -> "PoolBackend"` (line 37)
- `async def disconnect(self, timeout: int = 5) -> None` (line 45)
- `async def acquire(self)` (line 54)
- `async def release(self, connection, timeout: int = 10) -> None` (line 61)

### Verified Exceptions

**`asyncdb/exceptions/exceptions.py`**:
- `DriverError(AsyncDBException)` — line 78
- `ConnectionTimeout(ProviderError)` — line 106
- `EmptyStatement(AsyncDBException)` — line 98

Import pattern: `from ..exceptions import ConnectionTimeout, DriverError`

### Verified Factory

**`asyncdb/connections.py:31-44`** — `AsyncDB`:
```python
classpath = f"asyncdb.drivers.{driver}"   # "asyncdb.drivers.dynamodb"
mdl = module_exists(driver, classpath)     # loads class `dynamodb`
```

**`asyncdb/connections.py:13-28`** — `AsyncPool`:
```python
pool = f"{driver}Pool"                     # "dynamodbPool"
mdl = module_exists(pool, classpath)       # loads class `dynamodbPool`
```

No changes to `connections.py` needed — auto-discovery works by naming convention.

### Verified OutputFactory

**`asyncdb/drivers/outputs/output.py:8-29`** — `OutputFactory`:
- `__new__(cls, driver, frmt: str, *args, **kwargs)` — line 11
- Returns `driver.output` for `frmt="native"`, dynamically loads format modules otherwise

### Verified Reference Drivers

| Driver | File | Line | Pattern |
|--------|------|------|---------|
| `redis` | `asyncdb/drivers/redis.py` | 121 | `BaseDriver` + `get`/`set`/`delete` |
| `redisPool` | `asyncdb/drivers/redis.py` | 27 | `BasePool` + `connect`/`acquire`/`release` |
| `iceberg` | `asyncdb/drivers/iceberg.py` | 98 | `InitDriver` + `_KwargsTerminator` |

### Does NOT Exist (anti-hallucination)

- `asyncdb/drivers/dynamodb.py` — does not exist (this spec creates it)
- `_KwargsTerminator` in `asyncdb/drivers/base.py` — only in `iceberg.py`
- No shared AWS session manager or boto helper anywhere in asyncdb
- No `aiobotocore` import in any existing driver
- No `asyncdb/drivers/outputs/output.py` default export for DynamoDB-specific format
- No `prepare()` concept in DynamoDB — method should raise `NotImplementedError`

---

## 7. External Dependencies

| Package | Version | Purpose | Extras Group |
|---------|---------|---------|-------------|
| `aiobotocore` | 2.15.2 | Async DynamoDB client | `boto3` (existing) |
| `boto3` | (bundled with aiobotocore) | `TypeSerializer`/`TypeDeserializer` | `boto3` (existing) |

No new dependency groups needed. The existing `boto3` extras group in
`pyproject.toml` (lines 127-129) already includes both packages.

---

## 8. Out of Scope

- **DynamoDB Streams** — event-driven patterns are a separate feature.
- **TTL Configuration** — table-level setting, not driver responsibility.
- **DAX Integration** — DynamoDB Accelerator requires a separate client.
- **Automatic retry with backoff** — rely on aiobotocore's built-in retry.
- **Schema evolution tools** — DynamoDB is schemaless.

---

## Worktree Strategy

- **Isolation unit**: `per-spec` (all tasks sequential in one worktree)
- **Rationale**: The driver is a single file (`dynamodb.py`) with tightly coupled
  components — type marshalling, connection, and all operations share the same
  session/client infrastructure. Splitting across worktrees would create merge
  conflicts.
- **Cross-feature dependencies**: None. Independent of FEAT-001, FEAT-002, FEAT-003.
- **Shared file risk**: If `_KwargsTerminator` is extracted to `base.py`, the
  iceberg driver import changes — low risk, isolated change.

---

## Open Questions

| # | Question | Owner | Priority | Resolution |
|---|----------|-------|----------|------------|
| 1 | Extract `_KwargsTerminator` to `base.py` or duplicate in `dynamodb.py`? | Dev | Medium | Recommend: duplicate for now, refactor later |
| 2 | Should `get_batch()` be a separate method or overloaded on `get()`? | Dev | Low | Separate method — clearer API |
| 3 | GSI/LSI queries via `query(IndexName=...)` kwarg or dedicated `query_index()`? | Dev | Medium | kwargs pass-through — simpler, matches AWS SDK style |
| 4 | DynamoDB Local Docker image version for CI? | DevOps | Low | `amazon/dynamodb-local:latest` |

---

## References

- [AWS DynamoDB Developer Guide](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/)
- [aiobotocore Documentation](https://aiobotocore.readthedocs.io/)
- [boto3 DynamoDB Types](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html)
- [PartiQL for DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.html)
- Existing driver reference: `asyncdb/drivers/redis.py` (API style model)
- Existing driver reference: `asyncdb/drivers/iceberg.py` (InitDriver pattern)
- Brainstorm: `sdd/proposals/dynamodb-driver.brainstorm.md`
