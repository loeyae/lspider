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
        self.broken('Site not exists' % sid, sid)
        site = self.g['db']['SitesDB'].get_detail(sid)
        self.broken('Site: %s not exists' % sid, site)
        for item in self.g['db']['UrlsDB'].get_list(where={'sid': sid,'status': 0}):
            d={}
            d['status']=1
            d['uid'] = item['uid']
            self.logger.info("import status_queue data: %s" %  str(d))
            self.g['queue']['status_queue'].put_nowait(d)
