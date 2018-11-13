#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-19 17:20:05
"""
import time
from cdspider.tools import Base

class update_rate_by_sid(Base):
    """
    update rate by sid
    """
    def process(self, *args):
        sid = int(self.get_arg(args, 0, 'Pleas input sid'))
        rate = self.get_arg(args, 1, 'Pleas input rate')
        self.broken('Site not exists', sid)
        site = self.g['db']['SitesDB'].get_detail(sid)
        self.broken('Site: %s not exists' % sid, site)
        self.notice('Selected Site Info:', site)
        child = 0
        if len(args) > 2:
            child = int(args[2])
        d={}
        d['rate'] = rate
        d['utime'] = int(time.time())
        if child:
            uid = 0
            while True:
                i = 0
                for item in self.g['db']['UrlsDB'].get_new_list(uid, sid, where={'sid': sid}):
                    self.g['db']['UrlsDB'].update(item['uid'], d)
                    self.g['queue']['status_queue'].put_nowait({'uid': item['uid'], 'rate': rate})
                    i += 1
                    if item['uid'] > uid:
                        uid = item['uid']
                if i < 1:
                    return
        else:
            self.g['db']['SitesDB'].update(sid, d)
            self.g['queue']['status_queue'].put_nowait({'sid': sid, 'rate': rate})
