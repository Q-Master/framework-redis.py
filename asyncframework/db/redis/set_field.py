# -*- coding:utf-8 -*-
from typing import Optional, Any, Union, Iterable, List, Self
from .record_field import RedisRecordField, _T
from ._base import RecordType


__all__ = ['RedisSetField']


class RedisSetField(RedisRecordField[_T]):
    """Field for the redis set
    """
    def __init__(self, record_type: RecordType, prefix: Optional[str] = None, expire: int = 0):
        """Constructor

        Args:
            record_type (RecordType): the type of the record value (field processor or packet)
            prefix (Optional[str], optional): the prefix for set key. Defaults to None.
            expire (int, optional): expiration timeout in seconds. Defaults to 0.
        """
        super().__init__(record_type, prefix, expire)

    def clone(self) -> Self:
        return Self(self.record_type, self.prefix, self.expire)

    def dump(self, py_data: Union[_T, Iterable[_T]]) -> List[Any]:
        dumper = super().dump
        if isinstance(py_data, (list, tuple)):
            raw_data = [dumper(x) for x in py_data]
        else:
            raw_data = [dumper(py_data), ]
        return raw_data

    def load(self, raw_data: Union[Any, List[Any]]) -> Union[_T, List[_T]]:
        loader = super().load
        if isinstance(raw_data, (list, tuple)):
            return [loader(x) for x in raw_data]
        else:
            return loader(raw_data)
