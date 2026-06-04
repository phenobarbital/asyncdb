"""Unit tests for the explicit ``sslmode`` / ``PGSSLMODE`` support in the pg driver.

These tests do NOT require a live PostgreSQL instance: the asyncpg connection
entrypoints are mocked so we can assert how the driver resolves the ``ssl``
keyword passed down to asyncpg.

Run with:
    pytest tests/test_pg_sslmode.py -v
"""
import ssl as ssl_module

import pytest

from asyncdb.drivers.pg import (
    pg,
    pgPool,
    _resolve_sslmode,
    VALID_SSLMODES,
)

PARAMS = {
    "host": "localhost",
    "port": 5432,
    "user": "troc_pgdata",
    "password": "12345678",
    "database": "postgres",
}


# ---------------------------------------------------------------------------
# _resolve_sslmode helper
# ---------------------------------------------------------------------------


class TestResolveSSLMode:
    """Validation/normalization of libpq sslmode values."""

    @pytest.mark.parametrize("mode", VALID_SSLMODES)
    def test_valid_modes_pass_through(self, mode, monkeypatch):
        monkeypatch.delenv("PGSSLMODE", raising=False)
        assert _resolve_sslmode(mode) == mode

    def test_normalizes_case_and_whitespace(self, monkeypatch):
        monkeypatch.delenv("PGSSLMODE", raising=False)
        assert _resolve_sslmode("  REQUIRE ") == "require"
        assert _resolve_sslmode("Verify-Full") == "verify-full"

    def test_none_without_env_returns_none(self, monkeypatch):
        monkeypatch.delenv("PGSSLMODE", raising=False)
        assert _resolve_sslmode(None) is None

    def test_falls_back_to_env(self, monkeypatch):
        monkeypatch.setenv("PGSSLMODE", "verify-full")
        assert _resolve_sslmode(None) == "verify-full"

    def test_explicit_value_overrides_env(self, monkeypatch):
        monkeypatch.setenv("PGSSLMODE", "disable")
        assert _resolve_sslmode("require") == "require"

    def test_invalid_mode_raises(self, monkeypatch):
        monkeypatch.delenv("PGSSLMODE", raising=False)
        with pytest.raises(ValueError) as exc:
            _resolve_sslmode("bogus")
        assert "Invalid sslmode" in str(exc.value)


# ---------------------------------------------------------------------------
# __init__ parsing of sslmode (both classes)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls", [pg, pgPool])
class TestSSLModeInit:
    """Both pg and pgPool parse the sslmode option consistently."""

    def test_explicit_sslmode_param(self, cls, monkeypatch):
        monkeypatch.delenv("PGSSLMODE", raising=False)
        obj = cls(params=dict(PARAMS), sslmode="require")
        assert obj._sslmode == "require"
        assert obj.ssl is False

    def test_default_is_none(self, cls, monkeypatch):
        monkeypatch.delenv("PGSSLMODE", raising=False)
        obj = cls(params=dict(PARAMS))
        assert obj._sslmode is None
        assert obj.ssl is False

    def test_env_var_picked_up(self, cls, monkeypatch):
        monkeypatch.setenv("PGSSLMODE", "prefer")
        obj = cls(params=dict(PARAMS))
        assert obj._sslmode == "prefer"

    def test_invalid_sslmode_raises_on_init(self, cls, monkeypatch):
        monkeypatch.delenv("PGSSLMODE", raising=False)
        with pytest.raises(ValueError):
            cls(params=dict(PARAMS), sslmode="nope")


# ---------------------------------------------------------------------------
# ssl keyword resolution priority (mocking asyncpg)
# ---------------------------------------------------------------------------


class _FakePool:
    """Minimal stand-in for an asyncpg pool object."""


@pytest.mark.asyncio
class TestPoolSSLKwarg:
    """pgPool.connect passes the right ``ssl`` value to asyncpg.create_pool."""

    async def _capture_create_pool(self, monkeypatch, **init_kwargs):
        captured = {}

        async def fake_create_pool(*args, **kwargs):
            captured.update(kwargs)
            return _FakePool()

        monkeypatch.setattr(
            "asyncdb.drivers.pg.asyncpg.create_pool", fake_create_pool
        )
        monkeypatch.delenv("PGSSLMODE", raising=False)
        obj = pgPool(params=dict(PARAMS), **init_kwargs)
        await obj.connect()
        return captured

    async def test_default_passes_ssl_false(self, monkeypatch):
        captured = await self._capture_create_pool(monkeypatch)
        assert captured["ssl"] is False

    async def test_explicit_sslmode_string(self, monkeypatch):
        captured = await self._capture_create_pool(monkeypatch, sslmode="disable")
        assert captured["ssl"] == "disable"

    async def test_env_sslmode_string(self, monkeypatch):
        monkeypatch.setenv("PGSSLMODE", "prefer")

        captured = {}

        async def fake_create_pool(*args, **kwargs):
            captured.update(kwargs)
            return _FakePool()

        monkeypatch.setattr(
            "asyncdb.drivers.pg.asyncpg.create_pool", fake_create_pool
        )
        obj = pgPool(params=dict(PARAMS))
        await obj.connect()
        assert captured["ssl"] == "prefer"

    async def test_ssl_context_takes_precedence(self, monkeypatch):
        # A full SSL context (self.ssl=True) must win over sslmode.
        captured = await self._capture_create_pool(
            monkeypatch,
            sslmode="disable",
            ssl={"check_hostname": False},
        )
        assert isinstance(captured["ssl"], ssl_module.SSLContext)


@pytest.mark.asyncio
class TestSingleConnSSLKwarg:
    """pg.connection passes the right ``ssl`` value to asyncpg.connect."""

    async def _capture_connect(self, monkeypatch, **init_kwargs):
        captured = {}

        async def fake_connect(*args, **kwargs):
            captured.update(kwargs)
            raise RuntimeError("stop-after-capture")

        monkeypatch.setattr("asyncdb.drivers.pg.asyncpg.connect", fake_connect)
        monkeypatch.delenv("PGSSLMODE", raising=False)
        obj = pg(params=dict(PARAMS), **init_kwargs)
        with pytest.raises(Exception):
            await obj.connection()
        return captured

    async def test_default_passes_ssl_false(self, monkeypatch):
        captured = await self._capture_connect(monkeypatch)
        assert captured["ssl"] is False

    async def test_explicit_sslmode_string(self, monkeypatch):
        captured = await self._capture_connect(monkeypatch, sslmode="require")
        assert captured["ssl"] == "require"
