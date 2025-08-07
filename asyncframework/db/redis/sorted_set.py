# -*- coding:utf-8 -*-
from typing import Union, List, Any, Sequence, Optional, Literal, overload
import asyncio
from asyncframework.log.log import get_logger
from packets import PacketBase
from .connection import RedisConnection
from .sorted_set_field import RedisSortedSetField, RedisSortedSetData, DataType
from ._base import RedisRecordBase, T


__all__ = ['RedisSortedSet']


class RedisSortedSet(RedisRecordBase[RedisSortedSetField[T]]):
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

    @overload
    async def load(self, key: str, start: int = 0, end: int = -1, desc: bool = False, withscores: Literal[True]=True) -> Sequence[RedisSortedSetData[T]]:...

    @overload
    async def load(self, key: str, start: int = 0, end: int = -1, desc: bool = False, withscores: Literal[False]=False) -> Sequence[T]:...


    async def load(self, key: str, start: int = 0, end: int = -1, desc: bool = False, withscores=True) -> Sequence[Union[RedisSortedSetData[T], T]]:
        """Load set

        Args:
            key (str): set key

        Returns:
            List[Any]: resulting list of values
        """
        result = await self._connection.zrange(self._record_info.full_key(key), start, end, desc=desc, withscores=withscores)
        if withscores:
            return self._record_info.load_with_scores(result)
        else:
            return self._record_info.load(result)
    

    @overload
    async def range_by_score(self, key: str, min: float, max: float, start: Optional[int] = None, amount: Optional[int] = None, withscores: Literal[True] = True) -> Sequence[RedisSortedSetData[T]]: ...
    @overload
    async def range_by_score(self, key: str, min: float, max: float, start: Optional[int] = None, amount: Optional[int] = None, withscores: Literal[False] = False) -> Sequence[Union[RedisSortedSetData[T], T]]: ...

    async def range_by_score(self, key: str, min: float, max: float, start: Optional[int] = None, amount: Optional[int] = None, withscores=True) -> Sequence[Union[RedisSortedSetData[T], T]]:
        result = await self._connection.zrangebyscore(self._record_info.full_key(key), min, max, start, amount, withscores=withscores)
        if withscores:
            return self._record_info.load_with_scores(result)
        else:
            return self._record_info.load(result)

    async def pop_min(self, key: str, count: Optional[int] = None) -> RedisSortedSetData[T]:
        """Pop value from set

        Args:
            key (str): set key

        Returns:
            Any: the popped value
        """
        data = await self._connection.zpopmin(self._record_info.full_key(key), count)
        return self._record_info.load_with_scores(data)[0]

    async def pop_max(self, key: str, count: Optional[int] = None) -> RedisSortedSetData[T]:
        """Pop value from set

        Args:
            key (str): set key

        Returns:
            Any: the popped value
        """
        data = await self._connection.zpopmax(self._record_info.full_key(key), count)
        return self._record_info.load_with_scores(data)[0]

    async def count(self, key: str, min: float, max: float) -> int:
        return await self._connection.zcount(self._record_info.full_key(key), min, max)

    async def append(self, key: str, data: DataType):
        """Append value to set

        Args:
            key (str): set key
            data (DataType): set value to append
        """
        store = self._record_info.dump_with_scores(data)
        await self._connection.zadd(self._record_info.full_key(key), store)

    async def incr(self, key: str, data: DataType):
        store = self._record_info.dump_with_scores(data)
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
