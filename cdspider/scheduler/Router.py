#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-9-21 16:10:41
"""
import time
import logging
from . import BaseScheduler
from cdspider.exceptions import *
from cdspider.libs.constants import *
from cdspider.libs.utils import get_object, run_in_thread

class Router(BaseScheduler):
    """
    路由--初级任务分发
    """

    def __init__(self, context, mode):
        super(Router, self).__init__(context)
        self.outqueue = self.queue[QUEUE_NAME_SCHEDULER_TO_TASK]
        self.mode = mode
        self.interval = 5
        self._check_time = None

    def valid(self):
        if self.outqueue.qsize() > 0:
            self.debug("scheduler2task is running")
            return False
        return True

    def schedule(self, message = None):
        self.info("%s route starting..." % self.__class__.__name__)
        def handler_schedule(key, name, mode, ctx):
            handler = get_object("cdspider.handler.%s" % name)(ctx, None)
            save = {}
            while True:
                has_item = False
                for item in handler.route(mode, save):
                    if item:
                        has_item = True
                        message = {
                            "mode": mode,
                            "h-mode": key,
                            "item": item,
                        }
                        self.outqueue.put_nowait(message)
                if not has_item:
                    break
        threads = []
        for key, name in HANDLER_MODE_HANDLER_MAPPING.items():
            threads.append(run_in_thread(handler_schedule, key, name, self.mode, self.ctx))

        for each in threads:
            if not each.is_alive():
                continue
            if hasattr(each, 'terminate'):
                each.terminate()
            each.join()

        self.info("%s route end" % self.__class__.__name__)
