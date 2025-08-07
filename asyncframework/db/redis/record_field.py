# -*- coding:utf-8 -*-
from typing import Optional, Self
from ._base import RedisRecordFieldBase, RecordType, T


__all__ = ['RedisRecordField']


class RedisRecordField(RedisRecordFieldBase[T]):
    """Redis record field
    """
    def __init__(self, record_type: RecordType, prefix: Optional[str] = None, expire: Optional[int] = None):
        """Constructor

        Args:
            record_type (RecordType): the type of the record value (field processor or packet)
            prefix (Optional[str], optional): record key prefix. Defaults to None.
            expire (int, optional): expiration in seconds (None - not expiring). Defaults to None.
        """
        super().__init__(prefix, expire)
        self.set_check_record_type(record_type)

    def clone(self) -> Self:
        return self.__class__(self.record_type, self.prefix, self.expire)
