#-*- coding: utf-8 -*-

'''
Created on 2018年8月9日

@author: Wang Fengwei
'''
import re
import time
import traceback
from six.moves import queue
from cdspider.tools import Base

class kx_build_kw_newtask(Base):
    def process(self):
        while True:
            try:
                data=self.g['queue']['result2newtask'].get_nowait()
                title=data['title']
    #         for item in self.g['db']['ArticlesDB'].get_list(ctime=int(time.time()),where={'$and':[{'crawlinfo.title_to_task':{'$exists':False}},{'crawlinfo.sid':1}]}):
    #             title=item['title']
                self._insert_task(title, 2)
                self._insert_task(title, 3)
                self._insert_task(title, 9)
    #             self.g['db']['ArticlesDB'].update(item['rid'],obj={'crawlinfo.title_to_task':1})
            except queue.Empty:
                time.sleep(0.5)
            except:
                time.sleep(2)
                break

    def _insert_task(self,title,sid):
        title_list=re.split('[-_|]', title)
        title = title_list[0]
        if title==None or title=='':
            return
        if len(title)<8:
            return
        uid=0
        while True:
            u_sum=0
            for u_item in self.g['db']['UrlsDB'].get_list(where={'sid':sid,'uid':{'$gt':uid}}):
                try:
                    u_sum=u_sum+1
                    t={}
                    t['kwid']= self.g['db']['TaskDB'].get_count(1, where={'uid': u_item['uid'], 'aid': 0})
                    t['rid']=0
                    t['expire']=int(time.time()) + 2592000
                    if sid==3 or sid == 9:
                        t['expire']=int(time.time())+604800
                    t['sid']=sid
                    t['pid']=int(u_item['pid'])
                    t['uid']=u_item['uid']
                    t['utime']=int(time.time())
                    t['aid']=0
                    t['plantime']=int(time.time())
                    t['rate']=u_item['rate']
                    t['url']=u_item['url'].replace('{keyword}',title)
                    t['status']=1
                    t['newTask_by_tools']=1
                    self.g['logger'].info('insert data: %s',str(t))
                    self.g['db']['TaskDB'].insert(t)
                except:
                     self.logger(traceback.format_exc())
                finally:
                    uid = u_item['uid']
            if u_sum==0:
                break
            time.sleep(0.1)
