# -*- coding:utf-8 -*-
from typing import Union, List, Any, Tuple, Optional
import asyncio
from asyncframework.log.log import get_logger
from packets import PacketBase
from .connection import RedisConnection
from .sorted_set_field import RedisSortedSetField, RedisSortedSetData
from ._base import RedisRecordBase


__all__ = ['RedisSortedSet']


DataType = Union[RedisSortedSetData, List[RedisSortedSetData]]


class RedisSortedSet(RedisRecordBase[RedisSortedSetField]):
    """Redis set
    """
    log = get_logger('redis_set')

    def __init__(self, connection: RedisConnection, set_info: RedisSortedSetField):
        """Constructor

        Args:
            connection (RedisConnection): connection to redis
            set_info (SetField): set field info
        """
        super().__init__(connection, set_info)

    def __getattr__(self, item):
        return getattr(self._connection, item)

    async def load(self, key: str, start: int = 0, end: int = -1, desc: bool = False, withscores=True) -> List[Union[RedisSortedSetData, Any]]:
        """Load set

        Args:
            key (str): set key

        Returns:
            List[Any]: resulting list of values
        """
        result = await self._connection.zrange(self._record_info.full_key(key), start, end, desc=desc, withscores=withscores)
        if withscores:
            return self._load_with_scores(result)
        else:
            return self._load(result)
    
    async def range_by_score(self, key: str, min: float, max: float, start: Optional[int] = None, amount: Optional[int] = None, withscores=True) -> List[Union[RedisSortedSetData, Any]]:
        result = await self._connection.zrangebyscore(self._record_info.full_key(key), min, max, start, amount, withscores=withscores)
        if withscores:
            return self._load_with_scores(result)
        else:
            return self._load(result)

    async def pop_min(self, key: str, count: Optional[int] = None) -> RedisSortedSetData:
        """Pop value from set

        Args:
            key (str): set key

        Returns:
            Any: the popped value
        """
        data = await self._connection.zpopmin(self._record_info.full_key(key), count)
        return self._load(data)

    async def pop_max(self, key: str, count: Optional[int] = None) -> RedisSortedSetData:
        """Pop value from set

        Args:
            key (str): set key

        Returns:
            Any: the popped value
        """
        data = await self._connection.zpopmax(self._record_info.full_key(key), count)
        return self._load(data)

    async def count(self, key: str, min: float, max: float):
        return await self._connection.zcount(self._record_info.full_key(key), min, max)

    async def append(self, key: str, data: DataType):
        """Append value to set

        Args:
            key (str): set key
            data (DataType): set value to append
        """
        if isinstance(self._record_info.record_type, PacketBase):
            if isinstance(data, list):
                store = {k.dumps(): v for (v, k) in data}
            else:
                store = {data[1].dumps(): data[0]}
        else:
            if isinstance(data, list):
                store = {k: v for (v, k) in data}
            else:
                store = {data[1]: data[0]}
        await self._connection.zadd(self._record_info.full_key(key), store)

    async def incr(self, key: str, data: DataType):
        if isinstance(self._record_info.record_type, PacketBase):
            if isinstance(data, list):
                store = {k.dumps(): v for (v, k) in data}
            else:
                store = {data[1].dumps(): data[0]}
        else:
            if isinstance(data, list):
                store = {k: v for (v, k) in data}
            else:
                store = {data[1]: data[0]}
        futures = []
        for amount, value in store:
            futures.append(self._connection.zincrby(self._record_info.full_key(key), amount, value))
            if len(futures) % 100 == 0:
                await asyncio.gather(*futures)
                futures = []
        if len(futures):
            await asyncio.gather(*futures)
            futures = []

    async def remove(self, key: str, data: Union[Any, List[Any]]):
        """Remove data from set

        Args:
            key (str): set key
            data (Union[Any, List[Any]]): data to remove
        """
        if isinstance(self._record_info.record_type, PacketBase):
            rem = [x.dumps() for x in data] if isinstance(data, (list, tuple)) else [data.dumps(),]
        elif not isinstance(data, (list, tuple)):
            rem = [data, ]
        await self._connection.zrem(self._record_info.full_key(key), *rem)

    def _load_with_scores(self, data: Union[RedisSortedSetData, List[RedisSortedSetData]]):
        if isinstance(self._record_info.record_type, PacketBase):
            if isinstance(data, list):
                return [(y, self._record_info.record_type.loads(x)) for (x, y) in data]
            else:
                return (data[1], self._record_info.record_type.loads(data[0]))
        else:
            if isinstance(data, list):
                return [(y, x) for (x, y) in data]
            else:
                return (data[1], data[0])

    def _load(self, data: Union[Any, List[Any]]):
        if isinstance(self._record_info.record_type, PacketBase):
            if isinstance(data, (list, tuple)):
                return [self._record_info.record_type.loads(x) for x in data]
            else:
                return self._record_info.record_type.loads(data)
        else:
            return data
