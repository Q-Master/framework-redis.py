# -*- coding:utf-8 -*-
from typing import Union, List, Optional
from asyncframework.log.log import get_logger
from .connection import RedisConnection
from .set_field import RedisSetField
from ._base import RedisRecordBase, T


__all__ = ['RedisSet']


DataType = Union[T, List[T]]


class RedisSet(RedisRecordBase[RedisSetField[T]]):
    """Redis set
    """
    log = get_logger('redis_set')

    def __init__(self, connection: RedisConnection, set_info: RedisSetField):
        """Constructor

        Args:
            connection (RedisConnection): connection to redis
            set_info (SetField): set field info
        """
        super().__init__(connection, set_info)

    def __getattr__(self, item):
        return getattr(self._connection, item)

    async def load(self, key: str, count: Optional[int] = None) -> List[T]:
        """Load set

        Args:
            key (str): set key

        Returns:
            List[Any]: resulting list of values
        """
        result = await self._connection.smembers(self._record_info.full_key(key))
        return self._record_info.load(result)
    
    async def pop(self, key: str) -> T:
        """Pop value from set

        Args:
            key (str): set key

        Returns:
            Any: the popped value
        """
        data = await self._connection.spop(self._record_info.full_key(key))
        return self._record_info.load(data)[0]
    
    async def append(self, key: str, data: DataType) -> int:
        """Append value to set

        Args:
            key (str): set key
            data (DataType): set value to append
        """
        raw_data = self._record_info.dump(data)
        result = await self._connection.sadd(self._record_info.full_key(key), *raw_data)
        return result

    async def remove(self, key: str, data: DataType) -> int:
        """Remove data from set

        Args:
            key (str): set key
            data (DataType): data to remove
        
        Returns:
            bool: True if removed successfully else False
        """
        raw_data = self._record_info.dump(data)
        return await self._connection.srem(self._record_info.full_key(key), *raw_data)

    async def merge(self, dest: str, *sources: str) -> None:
        """Merge sources and put them to destination

        Args:
            dest (str): destination key
            sources(str): source keys
        """
        await self._connection.sunionstore(self._record_info.full_key(dest), *self._record_info.full_keys(sources))
