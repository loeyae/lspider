#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-9-21 17:51:33
"""
import time
import logging
from . import BaseScheduler
from cdspider.exceptions import *

class SynctaskScheduler(BaseScheduler):
    """
    同步数据
    """
    def __init__(self, db, queue, log_level = logging.WARN):
        super(SynctaskScheduler, self).__init__(db, queue, log_level)
        self.inqueue = queue["scheduler2plan"]

    def schedule(self, message):
        obj = {
            "queuetime": message['now'],
            "plantime": message['plantime'],
            "utime": int(time.time())
        }
        self.debug("update task: %s set %s" % (message['tid'], obj))
        self.db['TaskDB'].update(message['tid'], message['pid'], obj=obj)
