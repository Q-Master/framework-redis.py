# -*- coding: utf-8 -*-
from .database import *
from .connection import *
from .lock_field import *
from .lock import *
from .record_field import *
from .record import *
from .script_field import *
from .script import *
from .set_field import *
from .set import *
from .sorted_set import *
from .sorted_set_field import *

__version__ = '0.5.0'

__title__ = 'asyncframework-redis'
__description__ = 'Async framework redis addon.'
__url__ = 'https://github.com/Q-Master/framework-redis.py'
__uri__ = __url__
__doc__ = f"{__description__} <{__uri__}>"

__author__ = 'Vladimir Berezenko'
__email__ = 'qmaster2000@gmail.com'

__license__ = 'MIT'
__copyright__ = 'Copyright 2019-2023 Vladimir Berezenko'
