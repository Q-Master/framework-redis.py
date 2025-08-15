# -*- coding:utf-8 -*-
from typing import Union, List, Any, Sequence, Optional, Literal, overload, Tuple, Self, Dict
import asyncio
from packets import FieldProcessor
from asyncframework.log.log import get_logger
from packets import PacketBase
from .record import _RedisRecordField
from ._base import RedisRecordBase, T, RecordType


__all__ = ['RedisSortedSet', 'RedisSortedSetField', 'RedisSortedSetData']


RedisSortedSetData = Tuple[float, T]
RedisRawSortedSetData = Tuple[Any, float]
DataType = Union[RedisSortedSetData[T], List[RedisSortedSetData[T]]]


class _RedisSortedSetField(_RedisRecordField[T]):
    """Field for the redis set
    """
    def __init__(self, record_type: RecordType, prefix: Optional[str] = None, expire: int = 0):
        """Constructor

        Args:
            record_type (RecordType): the type of the record value (field processor or packet)
            prefix (Optional[str], optional): the prefix for set key. Defaults to None.
            expire (int, optional): expiration timeout in seconds. Defaults to 0.
        """
        super().__init__(record_type, prefix, expire)

    def clone(self) -> Self:
        return self.__class__(self.record_type, self.prefix, self.expire)

    def load_with_scores(self, data: Union[RedisRawSortedSetData, List[RedisRawSortedSetData]]) -> List[RedisSortedSetData[T | None]]:
        loader = super().load
        if isinstance(data, list):
            return [(y, loader(x)) for (x, y) in data]
        else:
            return [(data[1], loader(data[0])), ]

    def load(self, data: Union[Any, List[Any]]) -> List[T | None]:
        loader = super().load
        if isinstance(data, list):
            return [loader(x) for x in data]
        else:
            return [loader(data), ]

    def dump(self, data: Union[T, List[T]]) -> List[Any]:
        dumper = super().dump
        if isinstance(data, list):
            return [dumper(x) for x in data]
        else:
            return [dumper(data), ]
    
    def dump_with_scores(self, data: DataType) -> Dict[Any, Union[float, int]]:
        dumper = super().dump
        if isinstance(data, list):
            return {
                dumper(k): v for v, k in data
            }
        else:
            return {
                dumper(data[1]): data[0]
            }


class RedisSortedSet(RedisRecordBase[_RedisSortedSetField[T]]):
    """Redis set
    """
    log = get_logger('redis_set')

    def __init__(self, set_info: _RedisSortedSetField):
        """Constructor

        Args:
            set_info (SetField): set field info
        """
        super().__init__(set_info)

    @overload
    async def load(self, key: str, start: int = 0, end: int = -1, desc: bool = False, withscores: Literal[True]=True) -> Sequence[RedisSortedSetData[T | None]]:...

    @overload
    async def load(self, key: str, start: int = 0, end: int = -1, desc: bool = False, withscores: Literal[False]=False) -> Sequence[T | None]:...


    async def load(self, key: str, start: int = 0, end: int = -1, desc: bool = False, withscores=True) -> Sequence[Union[RedisSortedSetData[T | None], T | None]]:
        """Load set

        Args:
            key (str): set key

        Returns:
            List[Any]: resulting list of values
        """
        result = await self.connection.zrange(self._record_info.full_key(key), start, end, desc=desc, withscores=withscores)
        if withscores:
            return self._record_info.load_with_scores(result)
        else:
            return self._record_info.load(result)
    

    @overload
    async def range_by_score(self, key: str, min: float, max: float, start: Optional[int] = None, amount: Optional[int] = None, withscores: Literal[True] = True) -> Sequence[RedisSortedSetData[T | None]]: ...
    @overload
    async def range_by_score(self, key: str, min: float, max: float, start: Optional[int] = None, amount: Optional[int] = None, withscores: Literal[False] = False) -> Sequence[Union[RedisSortedSetData[T], T]]: ...

    async def range_by_score(self, key: str, min: float, max: float, start: Optional[int] = None, amount: Optional[int] = None, withscores=True) -> Sequence[Union[RedisSortedSetData[T | None], T | None]]:
        result = await self.connection.zrangebyscore(self._record_info.full_key(key), min, max, start, amount, withscores=withscores)
        if withscores:
            return self._record_info.load_with_scores(result)
        else:
            return self._record_info.load(result)

    async def pop_min(self, key: str, count: Optional[int] = None) -> RedisSortedSetData[T | None]:
        """Pop value from set

        Args:
            key (str): set key

        Returns:
            Any: the popped value
        """
        data = await self.connection.zpopmin(self._record_info.full_key(key), count)
        return self._record_info.load_with_scores(data)[0]

    async def pop_max(self, key: str, count: Optional[int] = None) -> RedisSortedSetData[T | None]:
        """Pop value from set

        Args:
            key (str): set key

        Returns:
            Any: the popped value
        """
        data = await self.connection.zpopmax(self._record_info.full_key(key), count)
        return self._record_info.load_with_scores(data)[0]

    async def count(self, key: str, min: float, max: float) -> int:
        return await self.connection.zcount(self._record_info.full_key(key), min, max)

    async def append(self, key: str, data: DataType):
        """Append value to set

        Args:
            key (str): set key
            data (DataType): set value to append
        """
        store = self._record_info.dump_with_scores(data)
        await self.connection.zadd(self._record_info.full_key(key), store)

    async def incr(self, key: str, data: DataType):
        store = self._record_info.dump_with_scores(data)
        futures = []
        for amount, value in store:
            futures.append(self.connection.zincrby(self._record_info.full_key(key), amount, value))
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
        await self.connection.zrem(self._record_info.full_key(key), *rem)


def RedisSortedSetField(record_type: RecordType, prefix: Optional[str] = None, expire: int = 0) -> RedisSortedSet:
    rt = record_type.my_type if isinstance(record_type, FieldProcessor) else record_type
    return RedisSortedSet(_RedisSortedSetField[rt](record_type, prefix, expire))
