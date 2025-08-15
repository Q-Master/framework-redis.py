# -*- coding: utf-8 -*-
from typing import Optional
from uuid import uuid4
from asyncframework.log.log import get_logger
from .script import _RedisScriptField, RedisScriptData, RedisScript
from .connection import RedisConnection
from ._base import RedisRecordFieldBase, RedisRecordBase


__all__ = ['RedisLock', 'RedisLockError', 'RedisLockInvalidTimeoutError', 'RedisLockField']


class _RedisLockField(RedisRecordFieldBase):
    """Field for lock
    """
    def __init__(self, prefix: Optional[str] = None, expire: int = 0, id: Optional[str] = None):
        """Constructor

        Args:
            prefix (Optional[str], optional): lpck key prefix. Defaults to None.
            expire (int, optional): time in seconds when lock will expire. Defaults to 0.
            id (Optional[str], optional): lock id. Defaults to None.
        """
        super().__init__(prefix, expire)
        self.id = str(uuid4()) if id is None else id

    def clone(self) -> '_RedisLockField':
        return _RedisLockField(self.prefix, self.expire, self.id)



logger = get_logger(__name__)


class UnlockScript(RedisScriptData):
    code = """
if redis.call("get", KEYS[1]) ~= ARGV[1] then
    return 1
else
    redis.call("del", KEYS[2])
    redis.call("lpush", KEYS[2], 1)
    redis.call("del", KEYS[1])
    return 0
end
"""


class ResetScript(RedisScriptData):
    code = """
redis.call('del', KEYS[2])
redis.call('lpush', KEYS[2], 1)
return redis.call('del', KEYS[1])
"""


class RedisLockError(RuntimeError):
    pass


class RedisLockTimeoutError(RedisLockError):
    pass


class RedisLockInvalidTimeoutError(RedisLockError):
    pass


class RedisLock(RedisRecordBase[_RedisLockField]):
    """Redis lock
    """
    class RealLock():
        """Redis lock object
        """
        def __init__(self, connection: RedisConnection, id: str, prefix: Optional[str], name: str, expire: int):
            """Constructor

            Args:
                connection (RedisConnection): connection to redis
                id (str): lock id
                prefix (Optional[str], optional): lock prefix
                name (str): lock name
                expire (int): expiration timeout in seconds
            """
            super().__init__()
            self._connection = connection
            self._id: str = id
            self._name: str = f'{prefix}:{name}' if prefix else name
            self._signal: str = f'{prefix}-signal:{name}' if prefix else f'signal:{name}'
            self._expire: int = expire
            self._unlock_script = RedisScript(_RedisScriptField(UnlockScript()))
            self._unlock_script.connection = connection
            self._reset_script = RedisScript(_RedisScriptField(ResetScript()))
            self._reset_script.connection = connection
            self._acquired = False
            self._timed_out = False
        
        async def reset(self):
            """Forcibly deletes the lock. Use this with care.
            """
            await self._reset_script(self._name, self._signal)
            await self._delete_signal()
            self._timed_out = False
            self._acquired = False

        async def is_owner(self) -> bool:
            """Check if owner.

            Returns:
                bool: returns if the lock is owned.
            """
            return self._id == await self.get_owner_id()

        async def get_owner_id(self) -> str:
            """Get owner of the lock

            Returns:
                str: owner of the lock
            """
            return await self._connection.get(self._name)

        async def acquire(self, blocking: bool = True, timeout: Optional[int] = None) -> bool:
            """Acquire lock

            Args:
                blocking (bool, optional): blocking lock or not. Defaults to True.
                timeout (Optional[int], optional): maximum amount of seconds to block. Defaults to None.

            Raises:
                RedisLockError: if already acquired by someone else.
                RedisLockInvalidTimeoutError: if timeout and not blocking or if timeout < 0 or if timeout is greater then expitation
            Returns:
                bool: the status of locking
            """
            logger.debug(f'Trying to lock {self._name}')

            if await self.is_owner():
                raise RedisLockError('Already acquired from this Lock instance.')

            if not blocking and timeout is not None:
                raise RedisLockInvalidTimeoutError('Timeout cannot be used if blocking=False')

            if timeout is not None and timeout <= 0:
                raise RedisLockInvalidTimeoutError(f'Timeout {timeout} cannot be less than or equal than 0')

            if timeout and self._expire and timeout > self._expire:
                raise RedisLockInvalidTimeoutError(f'Timeout {timeout} cannot be greater than expire {self._expire}')

            busy = True
            blpop_timeout = timeout or self._expire or 0
            self._timed_out = False
            while busy:
                busy = not await self._connection.set(self._name, self._id, exist=self._connection.SET_IF_NOT_EXIST, expire=self._expire)
                if busy:
                    if self._timed_out:
                        raise RedisLockTimeoutError(f'Lock is timed out {self._name}')
                    elif blocking:
                        self._timed_out = not await self._connection.blpop(self._signal, blpop_timeout) and timeout != 0
                    else:
                        logger.debug(f'Failed to get {self._name}')
                        raise RedisLockError(f'Failed to get {self._name}')
                else:
                    self._acquired = True

            logger.debug(f'Locked {self._name}')
            return True

        async def __aenter__(self):
            await self.acquire(blocking=True)

        async def __aexit__(self, *args):
            await self.release()

        async def release(self):
            """Release the lock.

            Raises:
                RedisLockError: if already expired or not been locked or error while unlocking
            """
            self._acquired = False
            logger.debug(f'Unlocking {self._name}')
            if self._timed_out:
                await self._delete_signal()
                return
            error = await self._unlock_script(self._name, self._signal, self._id)
            if error == 1:
                raise RedisLockError(f'Lock {self._name} is not acquired')
            elif error:
                raise RedisLockError(f'Unsupported error code {error} from UNLOCK script')
            else:
                await self._delete_signal()

        async def _delete_signal(self):
            await self._connection.delete(self._signal)
    
    def __init__(self, lock_info: _RedisLockField):
        """Constructor

        Args:
            lock_info (RedisLockField): redis lock field
        """
        super().__init__(lock_info)

    def get_lock(self, name: str) -> RealLock:
        """Get lock with name

        Args:
            name (str): name of the lock 

        Returns:
            RealLock: lock object
        """
        return RedisLock.RealLock(self.connection, self.lock_info.id, self.lock_info.prefix, name, self.lock_info.expire)


def RedisLockField(prefix: Optional[str] = None, expire: int = 0, id: Optional[str] = None) -> RedisLock:
    return RedisLock(_RedisLockField(prefix, expire, id))
