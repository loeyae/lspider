#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-10-22 14:31:27
"""
import time
from cdspider.tools import Base
from cdspider.exceptions import *
from cdspider.libs.constants import *

class clear_attachment_task(Base):
    """
    清理过期附加任务
    """
    inqueue_key = None

    def __init__(self, context, daemon = False):
        super(clear_attachment_task, self).__init__(context, daemon)
        self.db = self.g['db']
        self.config = self.g['app_config']
        self.running = False
        self._run_once = False

    def process(self, *args):
        running_time = int(args[0]) if len(args) > 0 else None
        if running_time != None:
            h = int(time.strftime('%H'))
            if h == running_time:
                self.running = True
            else:
                self.running = False
            if h > running_time and self._run_once == True:
                self._run_once = False
        if self.running == False or self._run_once == True:
            self.debug("clear_attachment_task sleep")
            time.sleep(60)
            return
        self.debug("clear_attachment_task starting")
        comment_delete_count = self.db['SpiderTaskDB'].delete_many(HANDLER_MODE_COMMENT, where={"expire": {"$lt": int(time.time()) - 259200}})
        self.debug("clear_attachment_task comment_delete_count: %s" % comment_delete_count)
        interact_delete_count = self.db['SpiderTaskDB'].delete_many(HANDLER_MODE_INTERACT, where={"expire": {"$lt": int(time.time()) - 259200}})
        self.debug("clear_attachment_task interact_delete_count: %s" % interact_delete_count)
        extended_delete_count = self.db['SpiderTaskDB'].delete_many(HANDLER_MODE_EXTENDED, where={"expire": {"$lt": int(time.time()) - 259200}})
        self.debug("clear_attachment_task extended_delete_count: %s" % extended_delete_count)
        self.debug("clear_attachment_task end")
        self._run_once = True
