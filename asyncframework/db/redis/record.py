# -*- coding:utf-8 -*-
from typing import Union, Iterable, Optional, List, Generic, TypeVar, Tuple, Set
from asyncframework.log.log import get_logger
from packets import PacketBase
from .connection import RedisConnection
from .record_field import RedisRecordField
from ._base import RedisRecordBase


__all__ = ['RedisRecord']


T = TypeVar('T', bound=PacketBase)


class RedisRecord(RedisRecordBase, Generic[T]):
    """Redis record class
    """
    log = get_logger('typed_collection')

    def __init__(self, connection: RedisConnection, record_info: RedisRecordField) -> None:
        """Constructor

        Args:
            connection (RedisConnection): connection to redis
            record_info (RedisRecordField): the record additional info
        """
        super().__init__(connection, record_info)

    async def load(self, mask: str = '*', count: Optional[int] = None) -> List[T]:
        """Load elements from keys by mask

        Args:
            mask (str, optional): mask for keys, not including prefix. Defaults to '*'.
            count (Optional[int], optional): amount of keys to load. Defaults to all keys.

        Returns:
            List[T]: loaded PacketBase instances of key values
        """
        result: List[T] = []
        match = self._record_info.full_key(mask)
        async for key in self._connection.iscan(match=match, count=count):
            obj: T = await self._load(key)
            result.append(obj)
        return result

    async def load_one(self, key: str) -> T:
        """Load one value by key name

        Args:
            key (str): the key not including prefix

        Returns:
            T: loaded PacketBase instance of value of the key
        """
        match = self._record_info.full_key(key)
        return await self._load(match)

    async def store(self, key: Union[Union[List[str], Tuple[str]], str], data: Union[Union[List[T], Tuple[T], Set[T]], T], upsert=True) -> None:
        storage: Iterable[tuple]
        if isinstance(key, (list, tuple)):
            if isinstance(data, PacketBase):
                v = data.dumps()
                storage = ((x, v) for x in key)
            elif isinstance(data, (list, tuple, set)):
                if len(data) == len(key):
                    storage = zip(self._record_info.full_keys(key), (x.dumps() for x in data))
                else:
                    raise AttributeError(f'Length of key array ({len(key)}) is not the same as of data array ({len(key)})')
                
        else:
            if isinstance(data, PacketBase):
                storage = ((key, data.dumps()), )
            else:
                storage = ((key, [d.dumps() for d in data]),)
        exist = None if upsert else False
        for k, v in storage:
            await self._connection.set(k, v, expire=self._record_info.expire, exist=exist)

    async def _load(self, key) -> Optional[T]:
        assert issubclass(self._record_info.record_type, PacketBase)
        data = await self._connection.get(key)
        if data:
            return self._record_info.record_type.loads(data)
        return None
