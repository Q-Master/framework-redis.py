# -*- coding: utf-8 -*-
from typing import Optional
from uuid import uuid4
from logging import Logger
from asyncio import sleep, TimerHandle, CancelledError
from asyncframework.log import get_logger
from .script import _RedisScriptField, RedisScriptData, RedisScript
from .connection import RedisConnection
from ._base import RedisRecordFieldBase, RedisRecordBase


__all__ = ['RedisLock', 'DeadlockError', 'LockTimeout', 'RedisLockField']


RETRY_LOCK_DELAY = 0.1


class _RedisLockField(RedisRecordFieldBase):
    """Field for lock
    """
    def __init__(self, prefix: Optional[str] = None, expire: int = 1, id: Optional[str] = None):
        """Constructor

        Args:
            prefix (Optional[str], optional): lock key prefix. Defaults to None.
            recursion_prefix (str, optional): recursion lock key prefix. Defaults to 'rec'
            expire (int, optional): time in seconds when lock will expire. Defaults to 1.
            id (Optional[str], optional): lock id. Defaults to None.
        """
        super().__init__(prefix, expire)
        self.id = id
        self.recursion_prefix = f'{prefix}-rec:' if prefix else 'rec:'

        """ Lock the transaction
        KEYS[1] - lock key
        KEYS[2] - current lock identifier
        ARGV[..] - possible recursive lock identifiers
        Returns 1 if lock is succesfully acquired, 0 otherwise.
        Returns other lock id if recursive lock detected. 
        """
        lock_script_data = RedisScriptData.from_data(
f"""
local lock_prefix='{self.prefix or ''}'
if redis.call('setnx', lock_prefix .. KEYS[1], KEYS[2]) == 1 then
    if #ARGV > 0 then
        redis.call('srem', lock_prefix .. KEYS[1], unpack(ARGV))
    end
    redis.call('expire', lock_prefix .. KEYS[1], {self.expire})
    return 1
else
    for n = 1, #ARGV do
        if redis.call('sismember', '{self.recursion_prefix}' .. ARGV[n], KEYS[1]) == 1 then
            return ARGV[n]
        end
    end
    return 0
end
"""
        )
        self.lock_script = RedisScript(_RedisScriptField(lock_script_data))

    def clone(self) -> '_RedisLockField':
        assert self.expire is not None
        return _RedisLockField(self.prefix, self.expire, self.id)

    def full_recursion_key(self, key: str) -> str:
        return f'{self.recursion_prefix}{key}'


class DeadlockError(RuntimeError):
    pass


class LockTimeout(RuntimeError):
    pass


class RedisLock(RedisRecordBase[_RedisLockField]):
    log = get_logger('RedisLock')
    """Redis lock
    """

    @property
    def connection(self) -> RedisConnection:
        return super().connection
    
    @connection.setter
    def connection(self, connection: RedisConnection):
        super().connection = connection
        self._record_info.lock_script.connection = connection

    class Lock():
        def __init__(self, key: str, conn: RedisConnection, info: _RedisLockField, log: Logger, recursion: set) -> None:
            """Redis real locking object

            Args:
                key (str): lock identificator
                conn (RedisConnection): current redis connection
                info (_RedisLockField): lock info data
                log (Logger): logger
                recursion (set): Optional set of recursive lock ids
            """
            self.key = key
            self.rkey = self.info.full_recursion_key(key)
            self.conn = conn
            self.info = info
            self.log = log
            self.recursion = recursion
            self.id = str(uuid4()) if self.info.id is None else self.info.id
            self.lock_timeout_future: Optional[TimerHandle] = None

        async def lock(self):
            first_pass = True
            try:
                while True:
                    result = await self.info.lock_script(
                        [self.key, self.id],
                        self.recursion
                        )
                    if result == 1:
                        break
                    if isinstance(result, str):
                        raise DeadlockError(f'Mutual lock detected: {result} -> {self.key}')
                    if first_pass:
                        first_pass = False
                        if self.recursion:
                            # adding recursive locks info
                            await self.conn.sadd(self.rkey, *self.recursion)
                    else:
                        await sleep(RETRY_LOCK_DELAY)
            except Exception:
                if not first_pass:
                    if self.recursion:
                        try:
                            # removing recursive locks info
                            await self.conn.srem(self.rkey, *self.recursion)
                        except:
                            self.log.warning(f'Error clearing recursion keys for {self.rkey}')
                raise
            assert self.info.expire is not None
            self.lock_timeout_future = self.conn.ioloop.call_later(self.info.expire, self._timeout_unlock)
        
        async def _timeout_unlock(self):
            try:
                self.log.error(f'Unlock for {self.key} failed by timeout {self.info.expire}')
                await self.unlock()
            except CancelledError:
                pass

        async def unlock(self):
            try:
                fkey = self.info.full_key(self.key)
                value = await self.conn.get(fkey)
                if value == self.id:
                    res = await self.conn.delete(fkey)
                    if res != 1:
                        self.log.warning(f'Lock for key {self.key} expired before deletion')
                elif value is None:
                    self.log.warning(f'Lock for key {self.key} expired before deletion')
                else:
                    self.log.warning(f'Lock key ID doesnt match {value} != {self.id}')
            except Exception as e:
                self.log.error(f'Error deleting key {self.key}: {e}')
            if self.lock_timeout_future:
                self.lock_timeout_future.cancel()
                self.lock_timeout_future = None


        async def __aenter__(self):
            await self.lock()

        async def __aexit__(self, *args):
            await self.unlock()

    def lock(self, key: str, recursion=set()) -> Lock:
        """Get lock with key

        Args:
            key (str): key of the lock 

        Returns:
            Lock: lock object
        """
        assert self._record_info.expire is not None
        return RedisLock.Lock(key, self.connection, self._record_info, self.log, recursion)


def RedisLockField(prefix: Optional[str] = None, expire: int = 1, id: Optional[str] = None) -> RedisLock:
    return RedisLock(_RedisLockField(prefix, expire, id))
