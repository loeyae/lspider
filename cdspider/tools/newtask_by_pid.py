#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-14 14:56:32
"""
import time
from cdspider.tools import Base
from cdspider.libs.constants import *

class newtask_by_pid(Base):
    """
    newtask by site
    """
    def process(self, *args):
        pid = int(self.get_arg(args, 0, 'Pleas input pid'))
        id = int(self.get_arg(args, 1, 'Pleas input start id'))
        testing_mode = self.g.get('testing_mode', False)
        maxid = 0
        if len(args) > 2:
            maxid = int(args[2])
        mode = HANDLER_MODE_DEFAULT_LIST
        if len(args) > 3:
            mode = args[3]
        self.broken('Project not exists', pid)
        project = self.g['db']['ProjectsDB'].get_detail(pid)
        self.broken('Project: %s not exists' % pid, project)
        self.notice('Selected Site Info:', project)
        if mode == HANDLER_MODE_DEFAULT_LIST:
            UrlsDB = self.g['db']['UrlsDB']
            while True:
                i = 0
                for item in UrlsDB.get_new_list_by_pid(id, pid, where={'status': {"$in": [UrlsDB.STATUS_INIT, UrlsDB.STATUS_ACTIVE]}}):
                    task = self.g['db']['SpiderTaskDB'].get_list(mode, {"uid": item['uuid']})
                    if len(list(task)) > 0:
                        continue
                    d={
                        'mode': mode,                # handler mode
                        'pid': item['pid'],          # project id
                        'sid': item['sid'],          # site id
                        'tid': item.get('tid', 0),   # task id
                        'uid': item['uuid'],         # url id
                        'kid': 0,                    # keyword id
                        'url': item['url'],          # url
                        'status': item['status'],    # status
                    }
                    self.info("insert into newtask: %s" %  str(d))
                    if not testing_mode:
                        self.g['db']['SpiderTaskDB'].insert(d)
                    i += 1
                    if item['uuid'] > id:
                        id = item['uuid']
                    if maxid > 0 and maxid <= id:
                        return
                if i < 1:
                    return
                time.sleep(0.1)
