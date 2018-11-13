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
        created = None if not args else int(args[0])
        no_loop = False
        if len(args) > 1:
           no_loop = bool(args[1])
        outqueue = self.g['queue'].get('scheduler2spider')
        ArticlesDB = self.g['db'].get('ArticlesDB')
        createtime = 0
        if not created:
            created = int(time.time())
        sum = 0
        acid = '0'
        while True:
            self.debug("current createtime: %s" % createtime)
            where = {"status": ArticlesDB.STATUS_INIT, "acid": {"$gt": acid}}
            data = ArticlesDB.get_list(created, where = where, select={"rid": 1, "url": 1, "ctime": 1, "acid": 1, "ctime": 1, "crawlinfo": 1}, sort=[('acid', 1)], hits=100)
            data = list(data)
            self.debug("got result: %s" % str(data))
            i = 0
            for item in data:
                if item['url'].startswith('javascript'):
                    ArticlesDB.update(item['rid'], {"status": ArticlesDB.STATUS_DELETED})
                    continue
                message = {
                    'mode': 'item',
                    'pid': item['crawlinfo']['pid'],
                    'rid': item['rid']
                }
                self.info("message: %s" % message)
                outqueue.put_nowait(message)
                acid = item['acid']
                i += 1
            if i == 0:
                self.info("no rebuid result")
                if no_loop:
                    break
            print("totle: %s" % sum)
            time.sleep(0.5)
