#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-9-20 15:49:37
"""
import time
import logging
from . import BaseScheduler
from cdspider.exceptions import *

class PlantaskScheduler(BaseScheduler):
    """
    任务调度
    """
    DEFAULT_RATE = [7200, "每2小时一次"]

    def __init__(self, db, queue, rate_map, log_level = logging.WARN):
        super(PlantaskScheduler, self).__init__(db, queue, log_level)
        self.inqueue = queue["scheduler2task"]
        self.rate_map = rate_map

    def schedule(self, message):
        if not 'pid' in message or not message['pid']:
            raise CDSpiderError("pid is missing")
        projectid = message['pid']
        now = int(time.time())
        where = {}
        if "uid" in message and message['uid']:
            where["uid"] = message['uid']
            where["aid"] = 0
        elif "aid" in message and message['aid']:
            where["aid"] = message['aid']
        elif "kwid" in message and message['kwid']:
            where["kwid"] = message['kwid']
            where["aid"] = 0
        elif "crid" in message and message['crid']:
            where["crid"] = message['crid']
            where["aid"] = 0
        elif "sid" in message and message['sid']:
            where["sid"] = message['sid']
            where["aid"] = 0
        tid = 0
        projection = ["tid", "pid", "sid", "uid", "crid", "aid", "kwid", "rate", "plantime"]
        while True:
            where['tid'] = {"$gt": tid}
            task_list = self.db["TaskDB"].get_plan_list(pid=projectid, where=where, select=projection, sort=[("tid", 1)])
            i = 0
            for task in task_list:
                self.debug("%s check_tasks task@%s: %s " % (self.__class__.__name__, projectid, str(task)))
                obj={}
                if task['aid'] > 0:
                    obj['mode']='att'
                elif 'crid' in task and task['crid'] > 0:
                    obj['mode'] = 'channel'
                else:
                    obj['mode']='list'
                obj['pid']=task['pid']
                obj['tid']=task['tid']
                plantime = now if task['plantime'] <= 0 else now + int(self.rate_map.get(str(task['rate']), self.DEFAULT_RATE)[0])
                self.send_task(obj, now, plantime)
                i += 1
            if i == 0:
                self.debug("%s check_tasks no newtask@%s" % (self.__class__.__name__, projectid))
                break
            time.sleep(0.1)

    def send_task(self, task, now, plantime):
        if self.queue['scheduler2spider']:
            self.debug("push %s into queue: scheduler2spider" % task)
            self.queue['scheduler2spider'].put_nowait(task)
        if self.queue['scheduler2plan']:
            task['plantime'] = plantime
            task['now'] = now
            self.debug("push %s into queue: scheduler2plan" % task)
            self.queue['scheduler2plan'].put_nowait(task)
