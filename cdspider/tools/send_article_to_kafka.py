#-*- coding: utf-8 -*-

'''
Created on 2018年8月9日

@author: Wang Fengwei
'''
import json
from cdspider.tools import Base
import time

class send_article_to_kafka(Base):

    def process(self):
        assert len(args) > 0, 'Please input where'
        where = json.loads(args[0])
        created = int(time.time())
        if len(args) > 1:
            created = int(created)
        sum=0
        acid = '0'
        where['acid'] = {"$gt": acid}
        where['status'] = 1
        self.notice('Where Info:', where)
        while True:
            n=0
            for item in self.g['db']['ArticlesDB'].get_list(created, where=where):
                n=n+1
                sum=sum+1
                d={}
                d['rid']=item['rid']
                d['on_sync']='publicOpinionCustomeNS:publicOpinionCustomeTB001'
                self.logger.info("import status_queue data: %s" %  str(d))
                acid=item['acid']
                self.g['queue']['result2kafka'].put_nowait(d)
            if n==0:
                break
        print(str(sum))
