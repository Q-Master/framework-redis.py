# -*- coding:utf-8 -*-
from typing import Optional, Iterable, Generator, Any, Type, TypeVar, Generic, List
from asyncframework.log import getLogger
from packets import PacketBase
from .connection import RedisConnection


__all__ = ['RedisRecordFieldBase', 'RedisRecordBase']


class RedisRecordFieldBase():
    """Redis record field base
    """
    prefix: Optional[str] = None
    expire: int = 0
    record_type: Type[PacketBase]

    def __init__(self, prefix: Optional[str] = None, expire: int = 0):
        """Constructor

        Args:
            prefix (Optional[str], optional): record key prefix. Defaults to None.
            expire (int, optional): expiration in seconds (0 - not expiring). Defaults to 0.
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
        return ':'.join((self.prefix, key)) if self.prefix else key

    def full_keys(self, keys: Iterable[str]) -> Generator:
        """Lazy generate full keys from keys

        Args:
            keys (Iterable[str]): list of keys

        Yields:
            str: full key name including prefix if set
        """
        for key in keys:
            yield(self.full_key(key))

    def clone(self):
        pass


T = TypeVar('T', bound=RedisRecordFieldBase)

class RedisRecordBase(Generic[T]):
    log = getLogger('typed_collection')
    _connection: RedisConnection
    _record_info: T

    def __init__(self, connection: RedisConnection, record_info: T) -> None:
        self._connection = connection
        self._record_info = record_info

    def __getattr__(self, item):
        return getattr(self._connection, item)

    async def load(self, key: str) -> List[Any]:
        return []

    async def load_one(self, key: str) -> Any:
        return None

    async def store(self, key, data, upsert=True):
        pass
