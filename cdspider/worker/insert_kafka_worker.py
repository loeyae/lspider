#-*- coding: utf-8 -*-

'''
Created on 2018年8月6日

@author: Wang Fengwei
'''

from cdspider.worker import BaseWorker
from cdspider.message_queue import KafkaQueue
import time
import logging
from six.moves import queue

class insert_kafka_worker(object):
    def __init__(self,db,queue,conf,log_level):
        self.db=db
        self.queue=queue
        self.conf=conf
        self.log_level=log_level
        self.logger=logging.getLogger("worker")
        self.logger.setLevel(log_level)
        self.connection()
    
    def connection(self):
        self.kafka=KafkaQueue(self.conf['topic'],self.conf['zookeeper_hosts'],host=self.conf['host'])
        self._is_loop=True
        
    def run(self):
        while self._is_loop:
            self.run_once()
    
    def run_once(self,flag=0):
        try:
            data=self.queue['result2kafka'].get_nowait()
            res=self.db['ArticlesDB'].get_detail(data['rid'])
            if 'on_sync' in data:
                res['flag']=data['on_sync']
            res.pop('rid')
            self.kafka.put_nowait(res)
        except queue.Empty:
            time.sleep(1)
            return
        except Exception as err:
            self.logger.error("InsertKafkaWorker :%s" % err)