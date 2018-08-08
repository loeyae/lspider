#-*- coding: utf-8 -*-

'''
Created on 2018年6月20日

@author: Wang Fengwei
'''
from cdspider.tools import Base

class send_queue(Base):
    
    def process(self):
        for item in self.g['db']['UrlsDB'].get_list(where={'sid':1,'status':0}):
            d={}
            d['status']=1
            d['uid']=item['uid']
            self.logger.info("import status_queue data: %s" %  str(d))
            self.g['queue']['status_queue'].put_nowait(d)