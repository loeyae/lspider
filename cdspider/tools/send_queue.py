#-*- coding: utf-8 -*-

'''
Created on 2018年8月9日

@author: Wang Fengwei
'''
from cdspider.tools import Base
import time

class send_queue(Base):
    
    def process(self):
#         for item in self.g['db']['UrlsDB'].get_list(where={'sid':8,'uid':{'$gt':756}}):
#             d={'uid':item['uid']}
#             print(d)
#             self.g['queue']['result2kafka'].put_nowait()
#         uid=0
#         while True:
#             n=0
#             for item in self.g['db']['UrlsDB'].get_list(where={'sid':2,'status':0,'uid':{'$gt':uid}}):
#                 d={}
#                 d['status']=1
#                 d['uid']=item['uid']
#                 self.logger.info("import status_queue data: %s" %  str(d))
#                 n=n+1
#                 uid=d['uid']
#                 self.g['queue']['status_queue'].put_nowait(d)
#             if n==0:
#                 break
        
        ctime=1535558400
        sum=0
        while True:
            n=0
            for item in self.g['db']['ArticlesDB'].get_list(int(time.time()),where={'status':1,'ctime':{'$gt':ctime}}):
                n=n+1
                sum=sum+1
                d={}
                d['rid']=item['rid']
                d['on_sync']='publicOpinionCustomeNS:publicOpinionCustomeTB001'
                self.logger.info("import status_queue data: %s" %  str(d))
                self.logger.info("import status_queue data: %s" %  str(item['rid']))
                ctime=item['ctime']
                self.g['queue']['result2kafka'].put_nowait(d)
            if n==0:
                break
        print(str(sum))
