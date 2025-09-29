# -*- coding:utf-8 -*-
from typing import Union
from asyncframework.app.service import Service
from asyncframework.log.log import get_logger
from .connection import RedisConnection
from ._base import RedisRecordBase


__all__ = ['RedisDb']


config_type = Union[str, dict]
key_type = Union[str, int]


class RedisDb(Service):
    log = get_logger('typeddb')

    __connection: RedisConnection

    def __init__(self, config: config_type) -> None:
        super(RedisDb, self).__init__()
        if isinstance(config, str):
            self.__connection = RedisConnection(config)
        elif isinstance(config, dict):
            self.__connection = RedisConnection.from_host_port(**config)
        else:
            raise TypeError('Ошибка конфига %s(%s)' % (config, type(config)))

    async def __start__(self, *args, **kwargs):
        await self.__connection.start()
        for value in self.__class__.__dict__.values():
            if isinstance(value, RedisRecordBase):
                value.set_connection(self.__connection)

    async def __stop__(self):
        await self.__connection.stop()
