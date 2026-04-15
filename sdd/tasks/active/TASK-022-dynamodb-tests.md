# TASK-022: DynamoDB Tests (DynamoDB Local)

**Feature**: FEAT-004 — dynamodb-driver
**Status**: pending
**Priority**: high
**Effort**: L
**Depends on**: [TASK-016, TASK-017, TASK-018, TASK-019, TASK-020, TASK-021]

---

## Objective

Create comprehensive pytest test suite using DynamoDB Local for all driver
functionality: connection lifecycle, CRUD, query, scan, PartiQL, batch ops,
DDL, index management, pool driver, and factory auto-discovery.

## Acceptance Criteria

1. `tests/test_dynamodb.py` exists with pytest + pytest-asyncio tests.
2. Tests connect to DynamoDB Local via `endpoint_url="http://localhost:8000"`.
3. Test coverage includes:
   - **Factory**: `AsyncDB("dynamodb")` and `AsyncPool("dynamodb")` auto-discovery.
   - **Connection**: `connection()`, `close()`, `async with` context manager.
   - **CRUD**: `get()`, `set()`, `delete()`, `write()`, `update()`, `fetch_one()`.
   - **Query**: `query()` with KeyConditionExpression, with `IndexName` for GSI.
   - **Scan**: `fetch_all()` full table scan.
   - **Pagination**: query/scan with enough items to trigger multi-page responses.
   - **PartiQL**: `partiql()` SELECT/INSERT, `execute()` PartiQL routing.
   - **Batch**: `write_batch()` with >25 items (chunking), `get_batch()` with >100 keys.
   - **DDL**: `create_table()`, `describe_table()`, `tables()`, `delete_table()`.
   - **Indexes**: `create_index()`, `list_indexes()`, `delete_index()`.
   - **Pool**: `connect()`, `acquire()`, `release()`, `disconnect()`.
   - **Error handling**: missing table, bad key, connection failure.
   - **Type marshalling**: verify Python types round-trip correctly (str, int, float,
     Decimal, bool, None, list, dict, set, binary).
4. Tests use fixtures for table creation/cleanup.
5. Tests are marked with `@pytest.mark.asyncio`.
6. Tests are skippable when DynamoDB Local is not running:
   ```python
   pytestmark = pytest.mark.skipif(
       not DYNAMODB_LOCAL_AVAILABLE,
       reason="DynamoDB Local not running on localhost:8000"
   )
   ```
7. All tests pass with `pytest tests/test_dynamodb.py -v`.

## Implementation Notes

- DynamoDB Local Docker setup (for CI and local dev):
  ```bash
  docker run -d -p 8000:8000 amazon/dynamodb-local:latest
  ```
- Test fixture pattern:
  ```python
  @pytest.fixture
  async def dynamo():
      db = AsyncDB("dynamodb", params={
          "region_name": "us-east-1",
          "endpoint_url": "http://localhost:8000",
          "aws_access_key_id": "testing",
          "aws_secret_access_key": "testing",
      })
      async with db as conn:
          yield conn

  @pytest.fixture
  async def test_table(dynamo):
      table_name = f"test_{uuid.uuid4().hex[:8]}"
      await dynamo.create_table(
          table_name,
          key_schema=[{"AttributeName": "pk", "KeyType": "HASH"}],
          attribute_definitions=[{"AttributeName": "pk", "AttributeType": "S"}],
      )
      await dynamo.wait_for_table(table_name)
      yield table_name
      await dynamo.delete_table(table_name)
  ```
- For pagination tests, insert >100 items and verify all are returned.
- For GSI tests, create table with a GSI and query against it.

## Codebase Contract

### Must Import

```python
from asyncdb import AsyncDB, AsyncPool
# Source: asyncdb/connections.py:31 (AsyncDB), :13 (AsyncPool)
```

### Reference Test Pattern

**`tests/` directory** — follow existing test patterns in the project.
Use `pytest.mark.asyncio` for all async tests.

### Does NOT Exist

- No `tests/test_dynamodb.py` — this task creates it
- No DynamoDB Local Docker compose file in the project — tests assume it's running externally
