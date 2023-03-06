# -*- coding: utf-8 -*-
from typing import Optional
from asyncframework.log import getLogger
from .script_field import RedisScriptField, RedisScriptData
from .script import RedisScript
from .connection import RedisConnection
from .lock_field import RedisLockField
from ._base import RedisRecordBase


__all__ = ['RedisLock', 'RedisLockError', 'RedisLockInvalidTimeoutError']


logger = getLogger(__name__)


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


class RedisLockInvalidTimeoutError(RedisLockError):
    pass


class RedisLock(RedisRecordBase):
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
            self._client: RedisConnection = connection
            self._id: str = id
            self._name: str = f'{prefix}:{name}' if prefix else name
            self._signal: str = f'{prefix}-signal:{name}' if prefix else f'signal:{name}'
            self._expire: int = expire
            self._unlock_script = RedisScript(connection, RedisScriptField(UnlockScript()))
            self._reset_script = RedisScript(connection, RedisScriptField(ResetScript()))
        
        async def reset(self):
            """Forcibly deletes the lock. Use this with care.
            """
            await self._reset_script(self._name, self._signal)
            await self._delete_signal()

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
            return await self._client.get(self._name)

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
            timed_out = False
            while busy:
                busy = not await self._client.set(self._name, self._id, exist=self._client.SET_IF_NOT_EXIST, expire=self._expire)
                if busy:
                    if timed_out:
                        return False
                    elif blocking:
                        timed_out = not await self._client.blpop(self._signal, blpop_timeout) and timeout != 0
                    else:
                        logger.debug(f'Failed to get {self._name}')
                        return False

            logger.debug(f'Locked {self._name}')
            return True

        async def __aenter__(self):
            acquired = await self.acquire(blocking=True)
            if not acquired:
                raise RedisLockError(f'Failed to ackquire lock {self._name}')

        async def __aexit__(self, *args):
            await self.release()

        async def release(self):
            """Release the lock.

            Raises:
                RedisLockError: if already expired or not been locked or error while unlocking
            """
            logger.debug(f'Unlocking {self._name}')
            error = await self._unlock_script(self._name, self._signal, self._id)
            if error == 1:
                raise RedisLockError(f'Lock {self._name} is not acquired or it already expired')
            elif error:
                raise RedisLockError(f'Unsupported error code {error} from UNLOCK script')
            else:
                await self._delete_signal()

        async def _delete_signal(self):
            await self._client.delete(self._signal)
    
    def __init__(self, connection: RedisConnection, lock_info: RedisLockField):
        """Constructor

        Args:
            connection (RedisConnection): the redis connection
            lock_info (RedisLockField): redis lock field
        """
        super().__init__(connection, lock_info)

    def get_lock(self, name: str) -> RealLock:
        """Get lock with name

        Args:
            name (str): name of the lock 

        Returns:
            RealLock: lock object
        """
        return RedisLock.RealLock(self.connection, self.lock_info.id, self.lock_info.prefix, name, self.lock_info.expire)
