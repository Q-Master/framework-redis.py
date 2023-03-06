# -*- coding:utf-8 -*-
from typing import Optional, Type, Union, Any
from packets import PacketBase
from .record_field import RedisRecordField


__all__ = ['RedisSetField']


class RedisSetField(RedisRecordField):
    """Field for the redis set
    """
    def __init__(self, record_type: Union[Any, Type[PacketBase]], prefix: Optional[str] = None, expire: int = 0):
        """Constructor

        Args:
            record_type (Union[Any, Type[PacketBase]]): the record type for set values
            prefix (Optional[str], optional): the prefix for set key. Defaults to None.
            expire (int, optional): expiration timeout in seconds. Defaults to 0.
        """
        super().__init__(record_type, prefix, expire)

    def clone(self) -> 'RedisSetField':
        return RedisSetField(self.record_type, self.prefix, self.expire)
