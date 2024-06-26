# -*- coding:utf-8 -*-
from typing import Union, List, Any, Tuple, Optional
from asyncframework.log.log import get_logger
from packets import PacketBase
from .connection import RedisConnection
from .set_field import RedisSetField
from ._base import RedisRecordBase


__all__ = ['RedisSet']


DataType = Union[Any, Union[List[Any], Tuple[Any]]]


class RedisSet(RedisRecordBase):
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

    async def load(self, key: str, count: Optional[int] = None) -> List[Any]:
        """Load set

        Args:
            key (str): set key

        Returns:
            List[Any]: resulting list of values
        """
        result = await self._connection.smembers(self._record_info.full_key(key))
        return self._load(result)
    
    async def pop(self, key: str) -> Any:
        """Pop value from set

        Args:
            key (str): set key

        Returns:
            Any: the popped value
        """
        data = await self._connection.spop(self._record_info.full_key(key))
        return self._load(data)
    
    async def append(self, key: str, data: DataType) -> int:
        """Append value to set

        Args:
            key (str): set key
            data (DataType): set value to append
        """
        if isinstance(self._record_info.record_type, PacketBase):
            data = [x.dumps() for x in data] if isinstance(data, (list, tuple)) else [data.dumps(),]
        elif not isinstance(data, (list, tuple)):
            data = [data, ]
        result = await self._connection.sadd(self._record_info.full_key(key), *data)
        return result

    async def remove(self, key: str, data: Union[Any, List[Any]]) -> int:
        """Remove data from set

        Args:
            key (str): set key
            data (Union[Any, List[Any]]): data to remove
        
        Returns:
            bool: True if removed successfully else False
        """
        if isinstance(self._record_info.record_type, PacketBase):
            data = [x.dumps() for x in data] if isinstance(data, (list, tuple)) else [data.dumps(),]
        elif not isinstance(data, (list, tuple)):
            data = [data, ]
        return await self._connection.srem(self._record_info.full_key(key), *data)

    async def merge(self, dest: str, *sources: str) -> None:
        """Merge sources and put them to destination

        Args:
            dest (str): destination key
            sources(str): source keys
        """
        await self._connection.sunionstore(self._record_info.full_key(dest), *self._record_info.full_keys(sources))

    def _load(self, data: Union[Any, List[Any]]):
        if isinstance(self._record_info.record_type, PacketBase):
            if isinstance(data, (list, tuple)):
                return [self._record_info.record_type.loads(x) for x in data]
            else:
                return self._record_info.record_type.loads(data)
        else:
            return data
