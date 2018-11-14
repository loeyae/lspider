#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-10-4 13:41:06
"""
import traceback
from cdspider.worker import BaseWorker
from cdspider.exceptions import *

class TestWorker(BaseWorker):
    """
    测试
    """
    inqueue_key = "result2kafka"

    def __init__(self, context):
        super(TestWorker, self).__init__(context)

    def on_result(self, message):
        self.info("got message: %s" % message)
        res=self.db['ArticlesDB'].get_detail(message['rid'], select = ["subdomain", "domain", "ctime", "channel", "acid", "url", "pubtime", "title", "status", "author", "content", "media_type", "result", "utime"])
        self.inqueue.put_nowait(message)
