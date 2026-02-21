"""Redpanda async driver built on top of aiokafka."""

from __future__ import annotations

import json
from typing import Any, Optional, Union

from ..exceptions import DriverError
from .base import BaseDriver


class redpanda(BaseDriver):
    """Async Redpanda driver.

    Redpanda is Kafka API compatible, so this driver uses aiokafka under the hood.
    """

    _provider = "redpanda"
    _syntax = "kafka"
    _dsn_template: str = "{host}:{port}"

    def __init__(self, dsn: str = None, loop=None, params: Optional[dict] = None, **kwargs):
        params = params or {}
        self._topic = kwargs.pop("topic", params.get("topic"))
        self._group_id = kwargs.pop("group_id", params.get("group_id", "asyncdb-redpanda"))
        self._client_id = kwargs.pop("client_id", params.get("client_id", "asyncdb"))
        self._producer = None
        self._admin = None
        super(redpanda, self).__init__(dsn=dsn, loop=loop, params=params, **kwargs)

    @staticmethod
    def _load_aiokafka():
        try:
            from aiokafka import AIOKafkaAdminClient, AIOKafkaConsumer, AIOKafkaProducer
        except ImportError as err:
            raise DriverError(
                "aiokafka is required for Redpanda support. Install with: pip install asyncdb[redpanda]"
            ) from err
        return AIOKafkaProducer, AIOKafkaConsumer, AIOKafkaAdminClient

    def _bootstrap_servers(self) -> str:
        if self._dsn:
            return self._dsn
        host = self._params.get("host", "127.0.0.1")
        port = self._params.get("port", 9092)
        return f"{host}:{port}"

    async def connection(self, **kwargs):
        AIOKafkaProducer, _, _ = self._load_aiokafka()
        servers = kwargs.pop("bootstrap_servers", self._bootstrap_servers())
        try:
            self._producer = AIOKafkaProducer(
                loop=self._loop,
                bootstrap_servers=servers,
                client_id=self._client_id,
                **kwargs,
            )
            await self._producer.start()
            self._connection = self._producer
            self._connected = True
            return self
        except Exception as err:
            self._connected = False
            raise DriverError(f"Redpanda connection error: {err}") from err

    async def close(self):
        try:
            if self._producer is not None:
                await self._producer.stop()
            if self._admin is not None:
                await self._admin.close()
            self._connected = False
            self._connection = None
            return True
        except Exception as err:
            raise DriverError(f"Error closing Redpanda connection: {err}") from err

    async def use(self, database: str):
        self._topic = database

    async def prepare(self, sentence: Union[str, list]) -> Any:
        self._prepared = sentence
        return sentence

    def _encode_value(self, value: Any) -> bytes:
        if value is None:
            raise DriverError("Cannot publish an empty message")
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode("utf-8")
        return json.dumps(value).encode("utf-8")

    async def execute(self, sentence: Any, *args, **kwargs) -> Optional[Any]:
        if not self._producer:
            await self.connection()
        topic = kwargs.pop("topic", self._topic)
        if not topic:
            raise DriverError("No topic selected. Use use(<topic>) or pass topic=<topic>.")

        key = kwargs.pop("key", None)
        partition = kwargs.pop("partition", None)
        headers = kwargs.pop("headers", None)
        timestamp_ms = kwargs.pop("timestamp_ms", None)

        payload = self._encode_value(sentence)
        encoded_key = self._encode_value(key) if key is not None else None
        try:
            metadata = await self._producer.send_and_wait(
                topic,
                payload,
                key=encoded_key,
                partition=partition,
                headers=headers,
                timestamp_ms=timestamp_ms,
            )
            result = {
                "topic": metadata.topic,
                "partition": metadata.partition,
                "offset": metadata.offset,
                "timestamp": metadata.timestamp,
            }
            return await self._serializer(result, None)
        except Exception as err:
            raise DriverError(f"Error publishing message to Redpanda: {err}") from err

    async def execute_many(self, sentence: list, *args) -> Optional[Any]:
        results = []
        for message in sentence:
            response, error = await self.execute(message, *args)
            if error:
                return await self._serializer(results, error)
            results.append(response)
        return await self._serializer(results, None)

    async def query(self, sentence: Union[str, list], **kwargs):
        _, AIOKafkaConsumer, _ = self._load_aiokafka()
        topic = sentence if isinstance(sentence, str) and sentence else kwargs.pop("topic", self._topic)
        if not topic:
            raise DriverError("No topic selected. Use use(<topic>) or pass topic=<topic>.")

        timeout_ms = kwargs.pop("timeout_ms", 1000)
        max_records = kwargs.pop("max_records", 100)
        consumer = AIOKafkaConsumer(
            topic,
            loop=self._loop,
            bootstrap_servers=kwargs.pop("bootstrap_servers", self._bootstrap_servers()),
            group_id=kwargs.pop("group_id", self._group_id),
            client_id=self._client_id,
            auto_offset_reset=kwargs.pop("auto_offset_reset", "earliest"),
            enable_auto_commit=kwargs.pop("enable_auto_commit", False),
            **kwargs,
        )

        try:
            await consumer.start()
            records = await consumer.getmany(timeout_ms=timeout_ms, max_records=max_records)
            messages = []
            for _, batch in records.items():
                for msg in batch:
                    messages.append(
                        {
                            "topic": msg.topic,
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "timestamp": msg.timestamp,
                            "key": msg.key.decode("utf-8") if msg.key else None,
                            "value": msg.value.decode("utf-8") if msg.value else None,
                        }
                    )
            return await self._serializer(messages, None)
        except Exception as err:
            raise DriverError(f"Error consuming messages from Redpanda: {err}") from err
        finally:
            await consumer.stop()

    fetch_all = query

    async def queryrow(self, sentence: Union[str, list]):
        messages, error = await self.query(sentence, max_records=1)
        if error:
            return await self._serializer(None, error)
        row = messages[0] if messages else None
        return await self._serializer(row, None)

    fetch_one = queryrow
