# -*- coding: utf-8 -*-
from typing import Any
from aioredis.exceptions import NoScriptError
from .connection import RedisConnection
from .script_field import RedisScriptField
from ._base import RedisRecordBase


__all__ = ['RedisScript']


class RedisScript(RedisRecordBase):
    """The lua script class
    """
    def __init__(self, connection: RedisConnection, script_info: RedisScriptField) -> None:
        """Constructor

        Args:
            connection (RedisConnection): the connection to redis
            script_info (ScriptField): the `ScriptField` info for the script
        """
        super().__init__(connection, script_info)
    
    async def __call__(self, *args, **kwargs) -> Any:
        """Execute the script

        Returns:
            Any: the result of execution
        """
        keys_num = len(kwargs) + len(args)
        keys_args = tuple(args) + tuple(kwargs.keys()) + tuple(kwargs.values()) if keys_num > 0 else []
        try:
            result = await self._connection.evalsha(self._record_info.code_sha1, keys_num, *keys_args)
        except NoScriptError:
            result = await self._connection.eval(self._record_info.code, keys_num, *keys_args)
        return result
