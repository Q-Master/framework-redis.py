# -*- coding: utf-8 -*-
from typing import Any, Union, Self
from hashlib import sha1
from redis.exceptions import NoScriptError
from ._base import RedisRecordBase, RedisRecordFieldBase


__all__ = ['RedisScript', 'RedisScriptField', 'RedisScriptData']


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
    def from_data(cls, script_data: str) -> Self:
        """Additional constructor.
        Will build the ScriptData object from already 

        Args:
            script_data (str): the script text

        Returns:
            ScriptData: the built class
        """
        data = cls()
        data.code = script_data
        data.code_sha1 = sha1(script_data.encode('utf-8')).hexdigest()
        return data


class _RedisScriptField(RedisRecordFieldBase):
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
                self._script_data = RedisScriptData.from_data(sd)

    def clone(self) -> '_RedisScriptField':
        return _RedisScriptField(self._script_data)


class RedisScript(RedisRecordBase[_RedisScriptField]):
    """The lua script class
    """
    def __init__(self, script_info: _RedisScriptField) -> None:
        """Constructor

        Args:
            script_info (ScriptField): the `ScriptField` info for the script
        """
        super().__init__(script_info)
    
    async def __call__(self, *args, **kwargs) -> Any:
        """Execute the script

        Returns:
            Any: the result of execution
        """
        keys_num = len(kwargs) + len(args)
        keys_args = tuple(args) + tuple(kwargs.keys()) + tuple(kwargs.values()) if keys_num > 0 else []
        try:
            result = await self.connection.evalsha(self._record_info.code_sha1, keys_num, *keys_args)
        except NoScriptError:
            result = await self.connection.eval(self._record_info.code, keys_num, *keys_args)
        return result


def RedisScriptField(script_or_path: Union[RedisScriptData, str]) -> RedisScript:
    return RedisScript(_RedisScriptField(script_or_path))
