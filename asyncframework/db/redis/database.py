# -*- coding:utf-8 -*-
import asyncio
from typing import Dict, Union, Sequence, List, Type, Hashable, Tuple
from abc import ABCMeta
from asyncframework.app.service import Service
from asyncframework.log.log import get_logger
from .connection import RedisConnection
from .lock_field import RedisLockField
from .record_field import RedisRecordField
from .script_field import RedisScriptField
from .set_field import RedisSetField
from .lock import RedisLock
from .record import RedisRecord
from .script import RedisScript
from .set import RedisSet
from ._base import RedisRecordFieldBase, RedisRecordBase


__all__ = ['RedisDb']


config_type = Union[Sequence[Union[str, dict]], Union[str, dict]]
key_type = Union[bytes, int]


class RedisDbMeta(ABCMeta):
    def __new__(cls, name, bases, namespace):
        records = {}
        for name, value in list(namespace.items()):
            if isinstance(value, RedisRecordFieldBase):
                if isinstance(value, RedisLockField):
                    records[name] = (value, RedisLock)
                elif isinstance(value, RedisRecordField):
                    records[name] = (value, RedisRecord)
                elif isinstance(value, RedisScriptField):
                    records[name] = (value, RedisScript)
                elif isinstance(value, RedisSetField):
                    records[name] = (value, RedisSet)
                del namespace[name]
        namespace['__records__'] = records
        return super().__new__(cls, name, bases, namespace)


class ShardObject():
    pass


class RedisDb(Service, metaclass=RedisDbMeta):
    __records__: Dict[str, Tuple[RedisRecordFieldBase, RedisRecordBase]] = {}
    log = get_logger('typeddb')

    __shards: List[RedisConnection] = []
    __items: List[ShardObject] = []
    __sharded: bool = False

    @property
    def sharded(self) -> bool:
        return self.__sharded

    @property
    def shards(self) -> int:
        if not self.sharded:
            return 0
        return len(self.__shards)

    def __init__(self, config: config_type) -> None:
        super(RedisDb, self).__init__()
        self.__shards = []
        self.__items = []
        self.__sharded = False
        if isinstance(config, (list, tuple)):
            for element_config in config:
                if isinstance(element_config, str):
                    conn = RedisConnection(element_config)
                elif isinstance(element_config, dict):
                    conn = RedisConnection.from_host_port(**element_config)
                else:
                    raise TypeError(u'Ошибка конфига %s(%s)' % (element_config, type(element_config)))
                self.__shards.append(conn)
                self.__sharded = True
        elif isinstance(config, str):
            conn = RedisConnection(config)
            self.__shards.append(conn)
        elif isinstance(config, dict):
            conn = RedisConnection.from_host_port(**config)
            self.__shards.append(conn)
        else:
            raise TypeError('Ошибка конфига %s(%s)' % (config, type(config)))

    @classmethod
    def with_records(cls, *records) -> Type['RedisDb']:
        collections_set = set(records)
        namespace = {collection_name: (cls.__records__[collection_name][0].clone(), cls.__records__[collection_name][1]) for collection_name in collections_set}
        namespace['log'] = cls.log
        partial_class = type(cls.__name__, cls.__bases__, namespace)
        return partial_class

    async def __start__(self, *args, **kwargs):
        await asyncio.gather(*[connection.start() for connection in self.__shards])
        for connection in self.__shards:
            if self.__sharded:
                shard = ShardObject()
                for coll_name, (record_info, record_class) in self.__records__.items():
                    setattr(shard, coll_name, record_class(connection, record_info))
                self.__items.append(shard)
            else:
                for coll_name, (record_info, record_class) in self.__records__.items():
                    setattr(self, coll_name, record_class(connection, record_info))

    async def __stop__(self):
        await asyncio.gather(*[connection.stop() for connection in self.__shards])

    def __getitem__(self, key: key_type) -> ShardObject:
        if not self.__sharded:
            raise AttributeError('Not sharded DB')
        shard_id = -1
        if isinstance(key, int):
            shard_id = key
        elif isinstance(key, Hashable):
            shard_id = hash(key) % len(self.__items)
        assert 0 <= shard_id < len(self.__items)
        return self.__items[shard_id]
