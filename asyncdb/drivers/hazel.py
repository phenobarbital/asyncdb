"""Hazelcast AsyncDB Driver.

"""
import asyncio
import logging
from datetime import datetime
from typing import (
    Union,
    Any,
    ClassVar
)
from collections.abc import Sequence
from datamodel import BaseModel, Field

import hazelcast
from hazelcast.serialization.api import Portable
from hazelcast.errors import (
    HazelcastError,
    HazelcastClientNotActiveError
)
from asyncdb.utils.types import Entity
from asyncdb.exceptions import (
    DriverError,
    ProviderError,
    NoDataFound,
)
from .abstract import (
    InitDriver
)


class HazelPortable(BaseModel, Portable):
    FACTORY_ID: ClassVar[int] = Field(default=1, repr=False)
    CLASS_ID: ClassVar[int] = Field(default=1, repr=False)

    def write_portable(self, writer):
        for name, f in self.columns().items():
            if name == 'FACTORY_ID':
                continue
            _type = f.type
            value = getattr(self, name)
            if Entity.is_integer(_type):
                writer.write_int(name, value)
            elif _type == bool:
                writer.write_boolean(name, value)
            elif Entity.is_number(_type):
                writer.write_long(name, value)
            else:
                writer.write_string(name, value)

    def read_portable(self, reader):
        for name, f in self.columns().items():
            if name == 'FACTORY_ID':
                continue
            _type = f.type
            if Entity.is_integer(_type):
                value = reader.read_int(name)
            elif _type == bool:
                val = reader.read_boolean(name)
                value = bool(val)
            elif Entity.is_number(_type):
                value = reader.read_long(name)
            else:
                value = reader.read_string(name)
            try:
                setattr(self, name, value)
            except (TypeError, ValueError) as e:
                logging.warning(f'Hazelcast Error on Portable: {e}')

    @classmethod
    def set_factory(cls, fid: int = 1):
        cls.FACTORY_ID = fid

    def get_factory_id(self):
        return self.__class__.FACTORY_ID

    def get_class_id(self):
        return self.__class__.FACTORY_ID

class hazel(InitDriver):
    _provider = "hazelcast"
    _syntax = "sql"

    def __init__(
            self,
            dsn: str = None,
            loop: asyncio.AbstractEventLoop = None,
            params: dict = None,
            **kwargs
        ):
        self._test_query = None
        self._server = None
        _starttime = datetime.now()
        self._timeout: int = 10
        try:
            self._map_name = params['map_name']
            del params['map_name']
        except KeyError:
            self._map_name = 'asyncdb-map'
        try:
            self._cluster = params['cluster']
            del params['cluster']
        except KeyError:
            self._cluster = None
        try:
            self._client = params['client']
            del params['client']
        except KeyError:
            self._client = 'asyncdb'
        try:
            host = params["host"]
        except KeyError as ex:
            raise DriverError(
                "Hazelcast: Unable to find *host* in parameters."
            ) from ex
        try:
            port = params["port"]
        except KeyError:
            port = 5701
        self._server = f"{host}:{port}"
        ### factories:
        try:
            self.factories = params["factories"]
            del params["factories"]
        except KeyError:
            self.factories = []
        try:
            super(hazel, self).__init__(dsn=dsn, loop=loop, params=params, **kwargs)
            _generated = datetime.now() - _starttime
            print(f"HazelCast Started in: {_generated}")
        except Exception as err:
            raise DriverError(
                f"HazelCast Error: {err}"
            ) from err

    async def prepare(self, sentence: Union[str, list]) -> Any:
        self._prepared = sentence
        return self._prepared

    async def connection(self):
        try:
            print(
                f'{self._provider}: connecting at {self._server}'
            )
            # processing the factories:
            n = 1
            factories = {}
            for factory in self.factories:
                if not issubclass(factory, HazelPortable):
                    raise TypeError(
                        f"Wrong instance type for a Hazelcast Portable: {factory!r}"
                    )
                factory.set_factory(n)
                factories[factory.FACTORY_ID] = {factory.CLASS_ID: factory}
                n+=1
            self._connection = hazelcast.HazelcastClient(
                cluster_members=[
                    self._server
                ],
                client_name=self._client,
                connection_timeout=self._timeout,
                cluster_name=self._cluster,
                retry_initial_backoff=1,
                retry_max_backoff=15,
                retry_multiplier=1.5,
                retry_jitter=0.2,
                cluster_connect_timeout=120,
                portable_factories=factories
            )
            self._connected = True
            return self
        except RuntimeError as ex:
            raise DriverError(
                f"Error connecting to Hazelcast Cluster: {ex}"
            ) from ex
        except Exception as err:
            raise ProviderError(
                message=f"Unknown Hazelcast Error: {err}"
            ) from err

    def add_member(self, server):
        try:
            config = hazelcast.ClientConfig()
            config.network_config.addresses.append(server)
        except Exception as ex:
            self._logger.exception(ex)
            raise

    async def close(self, timeout: int = 10) -> None:
        try:
            self._connection.shutdown()
        except HazelcastClientNotActiveError as ex:
            self._logger.warning(
                f"Hazelcast Client is not Active: {ex}"
            )
        except Exception as err:
            raise ProviderError(
                message=f"Close Hazelcast Error: {err}"
            ) from err
        finally:
            self._connected = False

    async def get(self, key, map_name: str = None):
        if not map_name:
            map_name = self._map_name
        try:
            a_map = self._connection.get_map(map_name)
            result = a_map.get(key)
            if result:
                return result.result()
            else:
                return None
        except (HazelcastError) as err:
            raise ProviderError(
                f"Get Hazelcast Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Hazelcast Unknown Error: {err}"
            ) from err

    async def set(self, key, value: Any, map_name: str = None) -> None:
        if not map_name:
            map_name = self._map_name
        try:
            a_map = self._connection.get_map(map_name)
            a_map.set(key, value)
        except (HazelcastError) as err:
            raise ProviderError(
                f"Get Hazelcast Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Hazelcast Unknown Error: {err}"
            ) from err

    async def put(self, key, value: Any, map_name: str = None) -> None:
        if not map_name:
            map_name = self._map_name
        try:
            a_map = self._connection.get_map(map_name)
            a_map.put(key, value)
        except (HazelcastError) as err:
            raise ProviderError(
                f"Get Hazelcast Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Hazelcast Unknown Error: {err}"
            ) from err

    async def set_multi(self, key, *args, map_name: str = None):
        if not map_name:
            map_name = self._map_name
        try:
            mmap = self._connection.get_multi_map(map_name)
            for el in args:
                mmap.put(key, el)
        except (HazelcastError) as err:
            raise ProviderError(
                f"Set-Multi Hazelcast Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Hazelcast Unknown Error: {err}"
            ) from err

    async def get_multi(self, key, map_name: str = None):
        if not map_name:
            map_name = self._map_name
        try:
            mmap = self._connection.get_multi_map(map_name)
            result = mmap.get(key)
            if not result:
                raise NoDataFound(
                    f"No Data was found: {key}"
                )
            return result.result()
        except (HazelcastError) as err:
            raise ProviderError(
                f"Set-Multi Hazelcast Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Hazelcast Unknown Error: {err}"
            ) from err

    async def delete_multi(self, *keys, map_name: str = None):
        if not map_name:
            map_name = self._map_name
        try:
            mmap = self._connection.get_map(map_name)
            for key in keys:
                mmap.remove(key)
            self._logger.debug(
                f"Map {mmap}, size: {mmap.entry_set().result()}"
            )
            print(f"Map {mmap}, size: {mmap.entry_set().result()}")
        except (HazelcastError) as err:
            raise ProviderError(
                f"Set-Multi Hazelcast Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Hazelcast Unknown Error: {err}"
            ) from err

    def all(self, map_name: str = None):
        """all.
        Get all elements in a Distributed Map.
        """
        if not map_name:
            map_name = self._map_name
        try:
            a_map = self._connection.get_map(map_name)
            return a_map.entry_set().result()
        except (HazelcastError) as err:
            raise ProviderError(
                f"Get Hazelcast Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Hazelcast Unknown Error: {err}"
            ) from err

    async def exists(self, key: str, map_name: str = None) -> bool:
        if not map_name:
            map_name = self._map_name
        try:
            a_map = self._connection.get_map(map_name)
            return a_map.contains_key(key).result()
        except (HazelcastError) as err:
            raise ProviderError(
                f"Get Hazelcast Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Hazelcast Unknown Error: {err}"
            ) from err

    contains = exists

    async def delete(self, key: str, map_name: str = None):
        if not map_name:
            map_name = self._map_name
        try:
            a_map = self._connection.get_map(map_name)
            a_map.remove(key)
        except (HazelcastError) as err:
            raise ProviderError(
                f"Get Hazelcast Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Hazelcast Unknown Error: {err}"
            ) from err

    remove = delete

    def get_columns(self):
        raise NotImplementedError

    async def use(self, database):
        raise NotImplementedError

    async def query(self, sentence: Any = None, map_name: str = None, **kwargs):
        error = None
        result = None
        if not map_name:
            map_name = self._map_name
        try:
            mmap = self._connection.get_map(map_name)
            if not sentence:
                result = mmap.entry_set().result()
            else:
                result = mmap.get(sentence).result()
        except (HazelcastError) as err:
            error = f"Get Hazelcast Error: {err}"
        except Exception as err: # pylint: disable=W0703
            error = f"Hazelcast Unknown Error: {err}"
        finally:
            return await self._serializer(result, error) # pylint: disable=W0150

    queryrow = query

    async def fetch_all(self, sentence, map_name: str = None, **kwargs):
        result = None
        if not map_name:
            map_name = self._map_name
        try:
            mmap = self._connection.get_multi_map(map_name)
            result = mmap.get(sentence).result()
            if not result:
                raise NoDataFound()
            return result
        except (HazelcastError) as err:
            raise ProviderError(
                f"Get Hazelcast Error: {err}"
            ) from err
        except Exception as err:
            raise ProviderError(
                f"Hazelcast Unknown Error: {err}"
            ) from err

    fetch_one = fetch_all

    async def execute(self, sentence: Union[Any, str], *args, fut: bool = False, map_name: str = None, **kwargs) -> Union[Sequence, None]:
        print(f"Execute Query {sentence}")
        result = []
        error = None
        try:
            if map_name:
                self._connection.get_map(map_name).blocking()
            if not fut:
                result = self._connection.sql.execute(sentence, *args).result()
            else:
                result = self._connection.sql.execute(sentence, *args)
        except (HazelcastError) as err:
            error = f"Get Hazelcast Error: {err}"
        except Exception as err: # pylint: disable=W0703
            error = f"Hazelcast Unknown Error: {err}"
        finally:
            return await self._serializer(result, error) # pylint: disable=W0150

    execute_many = execute
