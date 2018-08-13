#-*- coding: utf-8 -*-

'''
Created on 2018年8月9日

@author: Wang Fengwei
'''
import time
from cdspider.tools import Base

class kx_build_kw_newtask(Base):
    def process(self):
        for item in self.g['db']['ArticlesDB'].get_list(ctime=int(time.time()),where={'$and':[{'crawlinfo.title_to_task':{'$exists':False}},{'crawlinfo.sid':1}]}):
            uid=0
            while True:
                u_sum=0
                for u_item in self.g['db']['UrlsDB'].get_list(where={'sid':2,'uid':{'$gt':uid}}):
                    u_sum=u_sum+1
                    if item['title']==None or item['title']=='':
                        continue
                    t={}
                    t['kwid']=0
                    t['rid']=0
                    t['expire']=int(time.time())+(60*60*24*30)
                    t['sid']=2
                    t['pid']=1
                    t['uid']=u_item['uid']
                    t['utime']=int(time.time())
                    t['aid']=0
                    t['plantime']=int(time.time())
                    t['rate']=u_item['rate']
                    t['url']=u_item['url'].replace('{keyword}',item['title'])
                    t['status']=1
                    t['newTask_by_tools']=1
                    try:
                        print(str(t['uid'])+'==='+item['title'])
                    except:
                        pass
                    uid=u_item['uid']
                    self.g['db']['TaskDB'].insert(t)
                if u_sum==0:
                    break
            self.g['db']['ArticlesDB'].update(item['rid'],obj={'crawlinfo.title_to_task':1})
            