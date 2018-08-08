#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-8 23:23:03
"""

import time
from cdspider.tools import Base

class rebuild_result(Base):
    """
    rebuild item task
    """

    def process(self, *args, **kwargs):
        created = None if not args else args[0]
        outqueue = self.g['queue'].get('scheduler2spider')
        ArticlesDB = self.g['db'].get('ArticlesDB')
        createtime = 0
        lastcreatetime = createtime
        if not created:
            created = int(time.time())
        while True:
            if lastcreatetime == createtime:
                createtime += 1
            else:
                lastcreatetime = createtime
            self.g['logger'].debug("current createtime: %s" % createtime)
            data = ArticlesDB.get_list(created, where = [("status", ArticlesDB.STATUS_INIT), ("ctime", "$gte", createtime)], select={"rid": 1, "url": 1, "ctime": 1}, hits=100)
            data = list(data)
            self.g['logger'].debug("got result: %s" % str(data))
            i = 0
            for item in data:
                if item['url'].startswith('javascript'):
                    ArticlesDB.update(item['rid'], {"status": ArticlesDB.STATUS_DELETED})
                    continue
                message = {
                    'mode': 'item',
                    'tid': 0,
                    'pid': item['crawlinfo'].get('pid'),
                    'sid': item['crawlinfo'].get('sid', 0),
                    'rid': item['rid'],
                    'url': item('url'),
                    'save': None,
                    'unid': item['acid'],
                    'parent_url': item['crawlinfo'].get('url'),
                }
                outqueue.put_nowait(message)
                if item['ctime'] > createtime:
                    createtime = item['ctime']
                i += 1
            if i == 0:
                self.g['logger'].info("no rebuid result")
            if no_loop:
                break
            time.sleep(0.5)
