# -*- coding: utf-8 -*-
from typing import Union
from hashlib import sha1
from ._base import RedisRecordFieldBase


__all__ = ['RedisScriptField', 'RedisScriptData']


class ScriptDataMeta(type):
    def __new__(cls, name, bases, namespace):
        assert 'code' in namespace.keys()
        namespace['code_sha1'] = sha1(namespace.get('code').encode('utf-8')).hexdigest()
        return super().__new__(cls, name, bases, namespace)


class RedisScriptData(metaclass=ScriptDataMeta):
    """Script data.
    Stores the lua script itself and it's sha1.
    """
    code: str = ''
    code_sha1: str = ''

    @classmethod
    def from_data(cls, script_data: str) -> type['RedisScriptData']:
        """Additional constructor.
        Will build the ScriptData object from already 

        Args:
            script_data (str): the script text

        Returns:
            ScriptData: the built class
        """
        namespace = {'code': script_data}
        return type(cls.__name__, cls.__bases__, namespace)


class RedisScriptField(RedisRecordFieldBase):
    """Script field
    """
    @property
    def code(self) -> str:
        """Returns the text of a script

        Returns:
            str: the text of a script
        """
        return self._script_data.code
    
    @property
    def code_sha1(self) -> str:
        """Returns the hex string of a sha1 of a script text

        Returns:
            str: hex digest of a sha1 of a script text
        """
        return self._script_data.code_sha1

    def __init__(self, script_or_path: Union[RedisScriptData, str]):
        """Constructor

        Args:
            script_or_path (Union[ScriptData, str]): either the `ScriptData` or the path to a text file, containing the script
        """
        super().__init__()
        if isinstance(script_or_path, RedisScriptData):
            self._script_data = script_or_path
        else:
            with open(script_or_path, 'r') as f:
                sd = f.read()
            if sd:
                self._script_data = RedisScriptData.from_data(sd)()

    def clone(self) -> 'RedisScriptField':
        return RedisScriptField(self._script_data)
