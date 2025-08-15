# -*- coding: utf-8 -*-
from packets import Packet, makeField
from packets.processors import string_t, str_int_t
from asyncframework.db.redis import RedisDb, RedisRecord, RedisRecordField, RedisSortedSet, RedisSortedSetField
from asyncframework.app import main
from asyncframework.app.script import Script
from asyncframework.util.datetime import time
from asyncframework.log import get_logger


class TopUser(Packet):
    user_id: str = makeField(string_t, required=True)


class RedisExampleDB(RedisDb):
    start_time: RedisRecord[int] = RedisRecordField(str_int_t, 'top:')
    top: RedisSortedSet[str] = RedisSortedSetField(string_t, 'top:')
    top_user_cache: RedisRecord[TopUser] = RedisRecordField(TopUser, 'top:', 864000)


class Example(Script):
    log = get_logger('Example')
    def __init__(self, config):
        super().__init__('')

    async def __start__(self, *args, **kwargs):
        self.log.info('Starting example DB script')
        self.example_db = RedisExampleDB('redis://127.0.0.1:6379/0')
        await self.example_db.start(self.ioloop)

    async def __stop__(self, *args):
        self.log.info('Stopping example DB script')
        await self.example_db.stop()

    async def __body__(self):
        start_time = time.unixtime()
        self.log.info(f'last_time before: {start_time}')
        await self.example_db.start_time.store('start_time', start_time)
        last_leauges_start_time = await self.example_db.start_time.load_one('start_time')
        self.log.info(f'last_time after: {last_leauges_start_time}')
        league_user = TopUser(
            user_id='ExampleTopUserID'
        )
        self.log.info(f'League user before: {league_user}')
        self.log.info(f'Saving example user to cache')
        await self.example_db.top_user_cache.store('ExampleUser', league_user)
        league_user = await self.example_db.top_user_cache.load_one('ExampleUser')
        self.log.info(f'League user after: {league_user}')
        await self.example_db.top.append('current_top', [(1, 'ExampleUser'), (2, 'ExampleUser1'), (2, 'ExampleUser2')])
        top = await self.example_db.top.load('current_top', withscores=True)
        self.log.info(f'Division {top}')

if __name__ == '__main__':
    main(Example, None)
