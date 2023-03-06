# -*- coding:utf-8 -*-
from typing import Optional
from uuid import uuid4
from ._base import RedisRecordFieldBase


__all__ = ['RedisLockField']


class RedisLockField(RedisRecordFieldBase):
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

    def clone(self) -> 'RedisLockField':
        return RedisLockField(self.prefix, self.expire, self.id)
