#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-14 14:56:32
"""
from cdspider.tools import Base

class newtask_by_sid(Base):
    """
    newtask by site
    """
    def process(self, *args):
        assert len(args) > 0, 'Please input sid'
        assert len(args) > 1, 'Please input start uid'
        maxuid = 0
        if len(args) > 2:
            maxuid = int(args[2])
        sid = int(args[0])
        uid = int(args[1])
        self.broken('Site not exists', sid)
        site = self.g['db']['SitesDB'].get_detail(sid)
        self.broken('Site: %s not exists' % sid, site)
        self.notice('Selected Site Info:', site)
        while True:
            i = 0
            for item in self.g['db']['UrlsDB'].get_new_list(uid, sid, where={'status': 0}):
                d={}
                d['uid'] = item['uid']
                self.logger.info("push newtask_queue data: %s" %  str(d))
                self.g['queue']['newtask_queue'].put_nowait(d)
                i += 1
                if item['uid'] > uid:
                    uid = item['uid']
            if i < 1 or (maxuid > 0 and maxuid <= uid):
                return
