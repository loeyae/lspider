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
        sid = int(self.get_arg(args, 0, 'Pleas input sid'))
        id = int(self.get_arg(args, 1, 'Pleas input start id'))
        maxid = 0
        if len(args) > 2:
            maxid = int(args[2])
        mode = 'url'
        if len(args) > 3:
            mode = args[3]
        self.broken('Site not exists', sid)
        site = self.g['db']['SitesDB'].get_detail(sid)
        self.broken('Site: %s not exists' % sid, site)
        self.notice('Selected Site Info:', site)
        UrlsDB = self.g['db']['UrlsDB']
        ChannelDB = self.g['db']['ChannelRulesDB']
        while True:
            i = 0
            if mode == 'url':
                for item in UrlsDB.get_new_list(id, sid, where={'status': {"$in": [UrlsDB.STATUS_INIT, UrlsDB.STATUS_ACTIVE]}}):
                    task = self.g['db']['TaskDB'].get_list(int(item['pid']) or 1, {"uid": item['uid'], "aid": 0})
                    if len(list(task)) > 0:
                        continue
                    d={}
                    d['uid'] = item['uid']
                    self.info("push newtask_queue data: %s" %  str(d))
                    self.g['queue']['newtask_queue'].put_nowait(d)
                    i += 1
                    if item['uid'] > id:
                        id = item['uid']
                    if maxid > 0 and maxid <= id:
                        return
            elif mode == 'channel':
                for item in ChannelDB.get_new_list(id, sid, where={'status': {"$in": [ChannelDB.STATUS_INIT, ChannelDB.STATUS_ACTIVE]}}):
                    task = self.g['db']['TaskDB'].get_list(int(item['pid']) or 1, {"crid": item['crid'], "aid": 0})
                    if len(list(task)) > 0:
                        continue
                    d={}
                    d['crid'] = item['crid']
                    self.info("push newtask_queue data: %s" %  str(d))
                    self.g['queue']['newtask_queue'].put_nowait(d)
                    i += 1
                    if item['crid'] > id:
                        id = item['crid']
                    if maxid > 0 and maxid <= id:
                        return
            if i < 1:
                return
