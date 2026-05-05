"""Tests for _pgAcquireContext and pgPool.acquire() concurrent-acquire fix.

These are integration tests that require a live PostgreSQL instance.
Set TEST_DSN environment variable or ensure the default DSN is reachable.

Run with:
    pytest tests/test_pgpool_concurrent_acquire.py -v
"""
import asyncio
import os

import pytest
import pytest_asyncio

from asyncdb import AsyncPool
from asyncdb.drivers.pg import _pgAcquireContext

# ---------------------------------------------------------------------------
# Test configuration
# ---------------------------------------------------------------------------

DRIVER = "pg"
DSN = os.getenv(
    "TEST_DSN",
    "postgres://troc_pgdata:12345678@127.0.0.1:5432/navigator",
)

# Pool settings that give enough room for the concurrency tests.
POOL_MIN = 2
POOL_MAX = 15


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture()
async def pool():
    """Create a connected pgPool and tear it down after each test."""
    args = {
        "timeout": 30,
        "server_settings": {
            "application_name": "TestPgAcquireContext",
        },
    }
    p = AsyncPool(DRIVER, dsn=DSN, min_size=POOL_MIN, max_size=POOL_MAX, **args)
    await p.connect()
    yield p
    await p.wait_close(gracefully=True, timeout=10)


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------


class TestPgAcquireContextConcurrency:
    """Validate that concurrent callers each receive a distinct connection."""

    async def test_concurrent_acquire_distinct_connections(self, pool):
        """10 concurrent acquire() calls each get a distinct connection object.

        Each coroutine holds its connection for 50 ms so all 10 overlap.
        The set of id() values must have exactly 10 members, proving that no
        two coroutines shared the same object.
        """
        connection_ids = []

        async def grab():
            async with pool.acquire() as conn:
                connection_ids.append(id(conn))
                await asyncio.sleep(0.05)  # hold long enough for overlap

        await asyncio.gather(*[grab() for _ in range(10)])

        assert len(connection_ids) == 10, "Expected 10 acquire results"
        assert len(set(connection_ids)) == 10, (
            "Each coroutine must receive a distinct connection object; "
            f"got {len(set(connection_ids))} distinct IDs out of 10"
        )


class TestPatternA:
    """Pattern A: conn = await pool.acquire()"""

    async def test_pattern_a_returns_pg_wrapper(self, pool):
        """await pool.acquire() returns a pg wrapper that can execute queries."""
        conn = await pool.acquire()
        assert conn is not None, "acquire() must return a non-None pg wrapper"
        row = await conn.fetch_one("SELECT 1 AS n")
        assert row["n"] == 1, "Expected query result n=1"
        await pool.release(conn)

    async def test_pattern_a_returns_awaitable(self, pool):
        """pool.acquire() returns a _pgAcquireContext before being awaited."""
        ctx = pool.acquire()
        assert isinstance(ctx, _pgAcquireContext), (
            "pool.acquire() must return a _pgAcquireContext, "
            f"got {type(ctx)}"
        )
        # Now consume it so the fixture teardown does not see a leaked connection.
        conn = await ctx
        await pool.release(conn)


class TestPatternB:
    """Pattern B: async with await pool.acquire() as conn: ..."""

    async def test_pattern_b_cm_with_await(self, pool):
        """async with await pool.acquire() as conn: works and can run a query."""
        async with await pool.acquire() as conn:
            row = await conn.fetch_one("SELECT 2 AS n")
            assert row["n"] == 2, "Expected query result n=2"

    async def test_pattern_b_conn_is_pg_wrapper(self, pool):
        """conn inside 'async with await pool.acquire() as conn' is the pg wrapper."""
        from asyncdb.drivers.pg import pg as PgDriver
        async with await pool.acquire() as conn:
            assert isinstance(conn, PgDriver), (
                f"Expected pg wrapper, got {type(conn)}"
            )


class TestPatternC:
    """Pattern C: async with pool.acquire() as conn: ... (preferred)"""

    async def test_pattern_c_cm_no_await(self, pool):
        """async with pool.acquire() as conn: works without explicit await."""
        async with pool.acquire() as conn:
            row = await conn.fetch_one("SELECT 3 AS n")
            assert row["n"] == 3, "Expected query result n=3"

    async def test_pattern_c_conn_is_pg_wrapper(self, pool):
        """conn inside 'async with pool.acquire() as conn' is the pg wrapper."""
        from asyncdb.drivers.pg import pg as PgDriver
        async with pool.acquire() as conn:
            assert isinstance(conn, PgDriver), (
                f"Expected pg wrapper, got {type(conn)}"
            )


class TestConnectionRelease:
    """Validate that connections are returned to the pool after CM exit."""

    async def test_connection_released_after_cm_exit(self, pool):
        """Pool remains functional after a Pattern-C context manager exits.

        If the connection were not released, a second acquire on a saturated
        pool would time-out.  With proper release it must succeed immediately.
        """
        async with pool.acquire() as _:
            pass  # connection is acquired then released

        # Must be able to acquire again right away.
        async with pool.acquire() as conn:
            row = await conn.fetch_one("SELECT 1 AS n")
            assert row["n"] == 1

    async def test_sequential_acquires_do_not_exhaust_pool(self, pool):
        """Repeated sequential acquires must all succeed without leak."""
        for i in range(5):
            async with pool.acquire() as conn:
                row = await conn.fetch_one("SELECT $1::int AS n", i)
                assert row["n"] == i


class TestDoneFlagBehavior:
    """Validate _done flag semantics on _pgAcquireContext."""

    async def test_done_flag_set_after_await(self, pool):
        """After await-ing a _pgAcquireContext the _done flag must be True.

        This mirrors asyncpg's PoolAcquireContext behaviour: once a context
        has been used as an awaitable it is considered consumed.
        """
        ctx = pool.acquire()
        assert ctx._done is False, "_done must start as False"
        conn = await ctx  # triggers __await__ which sets _done=True
        assert ctx._done is True, "_done must be True after await"
        await pool.release(conn)

    async def test_done_flag_set_after_cm_exit(self, pool):
        """After a CM exit the _done flag must be True."""
        ctx = pool.acquire()
        assert ctx._done is False
        async with ctx as _:
            pass
        assert ctx._done is True, "_done must be True after __aexit__"

    async def test_done_flag_prevents_cm_reuse_after_await(self, pool):
        """A context already consumed via await cannot be used as a CM.

        __aenter__ checks _done and raises InterfaceError (asyncpg.InterfaceError)
        when _done is True.
        """
        from asyncpg.exceptions import InterfaceError
        ctx = pool.acquire()
        conn = await ctx  # sets _done=True
        try:
            with pytest.raises(InterfaceError):
                async with ctx as _:
                    pass  # must not reach here
        finally:
            # Release the connection acquired during the first await.
            await pool.release(conn)

    async def test_done_flag_prevents_cm_reuse_after_cm_exit(self, pool):
        """A context already used as a CM cannot be reused.

        After __aexit__ sets _done=True, a second __aenter__ call must raise.
        """
        from asyncpg.exceptions import InterfaceError
        ctx = pool.acquire()
        async with ctx:
            pass  # first use — __aexit__ sets _done=True

        with pytest.raises(InterfaceError):
            async with ctx as _:
                pass  # second use — must raise
