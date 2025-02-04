# -*- coding:utf-8 -*-
from typing import Optional, Type, Union, Any
from packets import PacketBase
from ._base import RedisRecordFieldBase


__all__ = ['RedisRecordField']


class RedisRecordField(RedisRecordFieldBase):
    """Redis record field
    """
    def __init__(self, record_type: Union[Any, Type[PacketBase]], prefix: Optional[str] = None, expire: Optional[int] = None):
        """Constructor

        Args:
            record_type (Type[PacketBase]): the packet type of the record value
            prefix (Optional[str], optional): record key prefix. Defaults to None.
            expire (int, optional): expiration in seconds (None - not expiring). Defaults to None.
        """
        super().__init__(prefix, expire)
        self.record_type: Union[Any, Type[PacketBase]] = record_type

    def clone(self) -> 'RedisRecordField':
        return RedisRecordField(self.record_type, self.prefix, self.expire)
