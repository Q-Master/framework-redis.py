# -*- coding: utf-8 -*-
from typing import Optional
from redis.asyncio import ConnectionPool, Redis
from urllib.parse import quote_plus
from asyncframework.app.service import Service


__all__ = ['RedisConnection']


class RedisConnection(Service):
    """Redis connection service
    """
    __redis_pool: Optional[ConnectionPool] = None
    __redis_connection: Optional[Redis] = None
    __uri: str

    def __init__(self, uri: str):
        """Constructor
        URI's might be:
            redis://[[username]:[password]]@localhost:6379/0
            rediss://[[username]:[password]]@localhost:6379/0
            unix://[[username]:[password]]@/path/to/socket.sock?db=0

        Args:
            uri (str): URI to connect to redis
        """
        self.__uri = uri
        super().__init__()

    @property
    def uri(self):
        return self.__uri

    @classmethod
    def from_host_port(cls, 
        host: str, port: int, 
        user: Optional[str] = None, password: Optional[str] = None, 
        database: Optional[int] = None, 
        ssl: bool = False, 
        **additional_params) -> 'RedisConnection':
        """Constructor from host and port

        Args:
            host (str): host to connect to
            port (int): port number
            user (Optional[str], optional): optional username. Defaults to None.
            password (Optional[str], optional): optional password. Defaults to None.
            database (Optional[int], optional): database number. Defaults to None.
            ssl (bool, optional): if redis support ssl connection. Defaults to False.

        Returns:
            RedisConnection: the connection class
        """
        userpass = f'{quote_plus(user) if user else ""}:{quote_plus(password) if password else ""}@' if user or password else ''
        scheme = 'rediss' if ssl else 'redis'
        db = f'/{database}' if database else ''
        uri = f'{scheme}://{userpass}{host}:{port}{db}'
        if additional_params:
            uri = '?'.join([uri, '&'.join(f'{n}={v}' for n, v in additional_params.items())])
        return cls(uri)

    async def __start__(self, *args, **kwargs):
        self.__redis_pool = ConnectionPool.from_url(self.__uri, decode_responses=True)
        self.__redis_connection = Redis(connection_pool=self.__redis_pool)

    async def __stop__(self):
        await self.__redis_pool.disconnect()

    def __getattr__(self, item):
        return getattr(self.__redis_connection, item)
