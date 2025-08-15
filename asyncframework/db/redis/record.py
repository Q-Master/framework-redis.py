# -*- coding:utf-8 -*-
from typing import Union, Iterable, Optional, List, Tuple, Self
from asyncframework.log.log import get_logger
from packets import FieldProcessor
from ._base import RedisRecordFieldBase, RedisRecordBase, RecordType, T


__all__ = ['RedisRecord', 'RedisRecordField']


DataType = Union[T, List[T]]


class _RedisRecordField(RedisRecordFieldBase[T]):
    """Redis record field
    """
    def __init__(self, record_type: RecordType, prefix: Optional[str] = None, expire: Optional[int] = None):
        """Constructor

        Args:
            record_type (RecordType): the type of the record value (field processor or packet)
            prefix (Optional[str], optional): record key prefix. Defaults to None.
            expire (int, optional): expiration in seconds (None - not expiring). Defaults to None.
        """
        super().__init__(prefix, expire)
        self.set_check_record_type(record_type)

    def clone(self) -> Self:
        return self.__class__(self.record_type, self.prefix, self.expire)


class RedisRecord(RedisRecordBase[_RedisRecordField[T]]):
    """Redis record class
    """
    log = get_logger('typed_collection')

    def __init__(self, record_info: _RedisRecordField) -> None:
        """Constructor

        Args:
            record_info (RedisRecordField): the record additional info
        """
        super().__init__(record_info)

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
        async for key in self.connection.iscan(match=match, count=count):
            obj: T | None = await self._load(key)
            if obj is not None:
                result.append(obj)
        return result

    async def load_one(self, key: str) -> Optional[T]:
        """Load one value by key name

        Args:
            key (str): the key not including prefix

        Returns:
            T: loaded PacketBase instance of value of the key
        """
        match = self._record_info.full_key(key)
        return await self._load(match)

    async def store(self, key: Union[List[str], Tuple[str], str], data: DataType, upsert=True) -> None:
        """Store data to redis key

        Args:
            key (Union[List[str], Tuple[str], str]): keys to store (might be a list of keys)
            data (DataType): data to store (might be either single data copied to all keys, or list of different data)
            upsert (bool, optional): if we need to insert key if it is not exist. Defaults to True.

        Raises:
            AttributeError: raised if key and data are both iterables and their size differs
        """
        storage: Iterable[tuple]
        if isinstance(key, (list, tuple)):
            if isinstance(data, (list, tuple, set)):
                if len(data) == len(key):
                    storage = zip(self._record_info.full_keys(key), (self._record_info.dump(d) for d in data))
                else:
                    raise AttributeError(f'Length of key array ({len(key)}) is not the same as of data array ({len(key)})')
            else:
                v = self._record_info.dump(data)
                storage = ((x, v) for x in self._record_info.full_keys(key))
                
        else:
            if isinstance(data, (list, tuple, set)):
                storage = ((self._record_info.full_key(key), [self._record_info.dump(d) for d in data]), )
            else:
                storage = ((self._record_info.full_key(key), self._record_info.dump(data)), )
        if upsert:
            nx=None
            xx=None
        else:
            nx=True
            xx=None
        for k, v in storage:
            await self.connection.set(k, v, ex=self._record_info.expire, nx=nx, xx=xx)

    async def _load(self, key) -> Optional[T]:
        data = await self.connection.get(key)
        if data:
            return self._record_info.load(data)
        return None


def RedisRecordField(record_type: RecordType, prefix: Optional[str] = None, expire: Optional[int] = None) -> RedisRecord:
    rt = record_type.my_type if isinstance(record_type, FieldProcessor) else record_type
    return RedisRecord(_RedisRecordField[rt](record_type, prefix, expire))
