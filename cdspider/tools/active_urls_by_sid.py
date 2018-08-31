#-*- coding: utf-8 -*-

'''
Created on 2018年6月20日

@author: Wang Fengwei
'''
from cdspider.tools import Base

class active_urls_by_sid(Base):

    def process(self, *args):
        assert len(args) > 0, 'Please input sid'
        sid = int(args[0])
        self.broken('Site not exists', sid)
        site = self.g['db']['SitesDB'].get_detail(sid)
        self.broken('Site: %s not exists' % sid, site)
        self.notice('Selected Site Info:', site)
        UrlsDB = self.g['db']['UrlsDB']
        uid = 0
        if len(args) > 1:
            uid = int(args[1])
        while True:
            i = 0
            for item in UrlsDB.get_new_list(uid, sid, where={'status': UrlsDB.STATUS_INIT}):
                d={}
                d['status'] = UrlsDB.STATUS_ACTIVE
                d['uid'] = item['uid']
                self.logger.info("push status_queue data: %s" %  str(d))
                self.g['queue']['status_queue'].put_nowait(d)
                i += 1
                if item['uid'] > uid:
                    uid = item['uid']
            if i < 1:
                return
