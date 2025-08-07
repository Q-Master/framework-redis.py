# -*- coding:utf-8 -*-
from typing import Optional, Tuple, Self, Union, List, Any, Dict
from .record_field import RedisRecordField, T
from ._base import RecordType


__all__ = ['RedisSortedSetField', 'RedisSortedSetData']


RedisSortedSetData = Tuple[float, T]
RedisRawSortedSetData = Tuple[Any, float]
DataType = Union[RedisSortedSetData[T], List[RedisSortedSetData[T]]]


class RedisSortedSetField(RedisRecordField[T]):
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

    def load_with_scores(self, data: Union[RedisRawSortedSetData, List[RedisRawSortedSetData]]) -> List[RedisSortedSetData[T]]:
        loader = super().load
        if isinstance(data, list):
            return [(y, loader(x)) for (x, y) in data]
        else:
            return [(data[1], loader(data[0])), ]

    def load(self, data: Union[Any, List[Any]]) -> List[T]:
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
