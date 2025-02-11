# -*- coding:utf-8 -*-
from typing import Optional, Iterable, Generator, Any, Type, TypeVar, Generic, List, Union, Tuple
import datetime
from asyncframework.log.log import get_logger
from packets import PacketBase
from packets.processors import FieldProcessor
from .connection import RedisConnection


__all__ = ['RedisRecordFieldBase', 'RedisRecordBase']


RecordType = Union[FieldProcessor, Type[PacketBase]]
_T = TypeVar('T')


class RedisRecordFieldBase(Generic[_T]):
    """Redis record field base
    """
    prefix: Optional[str] = None
    expire: int = 0
    record_type: RecordType

    def __init__(self, prefix: Optional[str] = None, expire: Optional[int] = None):
        """Constructor

        Args:
            prefix (Optional[str], optional): record key prefix. Defaults to None.
            expire (int, optional): expiration in seconds (None - not expiring). Defaults to None.
        """
        self.prefix = prefix
        self.expire = expire

    def full_key(self, key: str) -> str:
        """Return full key in redis using predefined prefix

        Args:
            key (str): key name

        Returns:
            str: full key name including prefix if set
        """
        return ''.join((self.prefix, key)) if self.prefix else key

    def full_keys(self, keys: Iterable[str]) -> Generator:
        """Lazy generate full keys from keys

        Args:
            keys (Iterable[str]): list of keys

        Yields:
            str: full key name including prefix if set
        """
        for key in keys:
            yield(self.full_key(key))

    def set_check_record_type(self, record_type: RecordType):
        if isinstance(record_type, FieldProcessor) or issubclass(record_type, PacketBase):
            self.record_type = record_type
        else:
            raise TypeError(f'Unknown processor: {type(record_type)}')

    def dump(self, py_data: _T) -> Any:
        if isinstance(self.record_type, FieldProcessor):
            self.record_type.check_py(py_data)
            return self.record_type.py_to_raw(py_data)
        else:
            return py_data.dumps()

    def load(self, raw_data: Any) -> _T:
        if isinstance(self.record_type, FieldProcessor):
            self.record_type.check_raw(raw_data)
            return self.record_type.raw_to_py(raw_data, False)
        else:
            return self.record_type.loads(raw_data)

    def clone(self):
        pass


_U = TypeVar('U', bound=RedisRecordFieldBase)


class RedisRecordBase(Generic[_U]):
    log = get_logger('typed_collection')
    _connection: RedisConnection
    _record_info: _U

    def __init__(self, connection: RedisConnection, record_info: _U) -> None:
        self._connection = connection
        self._record_info = record_info

    def __getattr__(self, item):
        return getattr(self._connection, item)

    async def delete(self, key: Union[Union[List[str], Tuple[str]], str]) -> int:
        if isinstance(key, (list, tuple)):
            to_delete = list(self._record_info.full_keys(key))
        else:
            to_delete = [self._record_info.full_key(key)]
        return await self._connection.unlink(*to_delete)

    async def rename(self, src: str, dest: str) -> None:
        await self._connection.rename(self._record_info.full_key(src), self._record_info.full_key(dest))

    async def exists(self, key: str) -> bool:
        res = await self._connection.exists(self._record_info.full_key(key))
        return res > 0

    async def expire(self, key: str) -> bool:
        res = await self._connection.expire(self._record_info.full_key(key), time=self._record_info.expire)
        return res > 0

    async def expire_at(self, key: str, when: int | datetime.datetime) -> bool:
        res = await self._connection.expireat(self._record_info.full_key(key), when)
        return res > 0

    async def expire_time(self, key: str) -> int:
        return await self._connection.expiretime(self._record_info.full_key(key))

    async def copy(self, src: str, dest: str, replace: bool = False) -> bool:
        res = await self._connection.copy(self._record_info.full_key(src), self._record_info.full_key(dest), replace=replace)
        return res > 0
