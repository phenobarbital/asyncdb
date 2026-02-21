import pytest

from asyncdb import AsyncDB


pytestmark = pytest.mark.asyncio


class DummyMetadata:
    topic = "events"
    partition = 0
    offset = 7
    timestamp = 123456


class DummyMessage:
    def __init__(self):
        self.topic = "events"
        self.partition = 0
        self.offset = 1
        self.timestamp = 777
        self.key = b"user-1"
        self.value = b'{"event":"created"}'


class DummyProducer:
    def __init__(self, *args, **kwargs):
        self.started = False
        self.stopped = False
        self.kwargs = kwargs

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def send_and_wait(self, *args, **kwargs):
        return DummyMetadata()


class DummyConsumer:
    def __init__(self, *args, **kwargs):
        self.started = False
        self.stopped = False

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def getmany(self, timeout_ms=0, max_records=1):
        return {"tp": [DummyMessage()]}


class DummyAdmin:
    async def close(self):
        return True


@pytest.fixture
def patch_aiokafka(monkeypatch):
    from asyncdb.drivers.redpanda import redpanda

    monkeypatch.setattr(
        redpanda,
        "_load_aiokafka",
        staticmethod(lambda: (DummyProducer, DummyConsumer, DummyAdmin)),
    )


async def test_redpanda_connection_and_publish(patch_aiokafka):
    db = AsyncDB("redpanda", params={"host": "127.0.0.1", "port": 9092, "topic": "events"})
    await db.connection()
    assert db.is_connected() is True

    result, error = await db.execute({"event": "created"})
    assert error is None
    assert result["topic"] == "events"
    assert result["offset"] == 7

    await db.close()


async def test_redpanda_query_and_queryrow(patch_aiokafka):
    db = AsyncDB("redpanda", params={"host": "127.0.0.1", "port": 9092, "topic": "events"})

    records, error = await db.query("events")
    assert error is None
    assert len(records) == 1
    assert records[0]["key"] == "user-1"

    row, error = await db.queryrow("events")
    assert error is None
    assert row["value"] == '{"event":"created"}'
