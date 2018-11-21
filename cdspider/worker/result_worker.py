#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:27:08
"""
import time
from cdspider.database.base import TaskDB
from cdspider.worker import BaseWorker
from cdspider.parser import ItemParser
from cdspider.spider import Spider
from cdspider.libs import utils
from cdspider.parser.lib.time_parser import Parser as TimeParser

class ResultWorker(BaseWorker):
    """
    结果处理
    """

    def get_task(self, data):
        pass

    def on_result(self, message):
        self.proxy = self.g['proxy']
        self.debug("got message: %s" % message)
        pass

    def get_result(self, message):
        pass
