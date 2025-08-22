# -*- coding:utf-8 -*-
from typing import Union, List, Optional, Any, Self
from packets import FieldProcessor
from asyncframework.log.log import get_logger
from .record import _RedisRecordField
from ._base import RedisRecordBase, T, RecordType


__all__ = ['RedisSet', 'RedisSetField']


DataType = Union[T, List[T]]


class _RedisSetField(_RedisRecordField[T]):
    """Field for the redis set
    """
    def __init__(self, record_type: RecordType, prefix: Optional[str] = None, expire: Optional[int] = None):
        """Constructor

        Args:
            record_type (RecordType): the type of the record value (field processor or packet)
            prefix (Optional[str], optional): the prefix for set key. Defaults to None.
            expire (int, optional): expiration timeout in seconds. Defaults to 0.
        """
        super().__init__(record_type, prefix, expire)

    def clone(self) -> Self:
        return self.__class__(self.record_type, self.prefix, self.expire)

    def dump(self, py_data: Union[T, List[T]]) -> List[Any]:
        dumper = super().dump
        if isinstance(py_data, (list, tuple)):
            raw_data = [dumper(x) for x in py_data]
        else:
            raw_data = [dumper(py_data), ]
        return raw_data

    def load(self, raw_data: Union[Any, List[Any]]) -> List[T]:
        loader = super().load
        if isinstance(raw_data, list):
            return [loader(x) for x in raw_data]
        else:
            return [loader(raw_data), ]


class RedisSet(RedisRecordBase[_RedisSetField[T]]):
    """Redis set
    """
    log = get_logger('redis_set')

    def __init__(self, set_info: _RedisSetField):
        """Constructor

        Args:
            set_info (SetField): set field info
        """
        super().__init__(set_info)

    def __getattr__(self, item):
        return getattr(self.connection, item)

    async def load(self, key: str, count: Optional[int] = None) -> List[T]:
        """Load set

        Args:
            key (str): set key

        Returns:
            List[Any]: resulting list of values
        """
        result = await self.connection.smembers(self._record_info.full_key(key))
        return self._record_info.load(result)
    
    async def pop(self, key: str) -> Optional[T]:
        """Pop value from set

        Args:
            key (str): set key

        Returns:
            Any: the popped value
        """
        data = await self.connection.spop(self._record_info.full_key(key))
        if data:
            return self._record_info.load(data)[0]
        else:
            return None
    
    async def append(self, key: str, data: DataType) -> int:
        """Append value to set

        Args:
            key (str): set key
            data (DataType): set value to append
        """
        raw_data = self._record_info.dump(data)
        result = await self.connection.sadd(self._record_info.full_key(key), *raw_data)
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
        return await self.connection.srem(self._record_info.full_key(key), *raw_data)

    async def merge(self, dest: str, *sources: str) -> None:
        """Merge sources and put them to destination

        Args:
            dest (str): destination key
            sources(str): source keys
        """
        await self.connection.sunionstore(self._record_info.full_key(dest), *self._record_info.full_keys(sources))


def RedisSetField(record_type: RecordType, prefix: Optional[str] = None, expire: int = 0) -> RedisSet:
    rt = record_type.my_type if isinstance(record_type, FieldProcessor) else record_type
    return RedisSet(_RedisSetField[rt](record_type, prefix, expire))
