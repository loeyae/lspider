#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-8 23:23:03
"""

import time
from cdspider.libs.constants import *
from cdspider.tools import Base

class rebuild_result(Base):
    """
    rebuild item task
    """
    BBS_TYPES = (MEDIA_TYPE_BBS, MEDIA_TYPE_ASK)

    def process(self, *args, **kwargs):
        created = None if not args else int(args[0])
        outqueue = self.g['queue'].get('scheduler2spider')
        ArticlesDB = self.g['db'].get('ArticlesDB')
        if not created:
            created = int(time.time())
        sum = 0
        acid = '0'
        while True:
            where = {"status": ArticlesDB.STATUS_INIT, "acid": {"$gt": acid}}
            i = 0
            for item in ArticlesDB.get_list(created, where = where, select={"rid": 1, "url": 1, "acid": 1, "mediaType": 1}, sort=[('acid', 1)], hits=100):
                if item['url'].startswith('javascript'):
                    ArticlesDB.update(item['rid'], {"status": ArticlesDB.STATUS_DELETED})
                    continue
                message = {
                    'mode': HANDLER_MODE_BBS_ITEM if item['mediaType'] in self.BBS_TYPES else HANDLER_MODE_DEFAULT_ITEM,
                    'rid': rid,
                    'mediaType': item['mediaType']
                }
                self.info("message: %s" % message)
                outqueue.put_nowait(message)
                acid = item['acid']
                i += 1
            if i == 0:
                self.info("no rebuid result")
                break
            self.debug("totle: %s" % sum)
            time.sleep(0.5)
