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

class insert_kafka_worker(BaseWorker):
    def __init__(self,db,queue,conf,log_level):
        self.db=db
        self.queue=queue
        self.inqueue = queue['result2kafka']
        self.conf=conf
        self.log_level=log_level
        self.logger=logging.getLogger("worker")
        self.logger.setLevel(log_level)
        self.connection()

    def connection(self):
        self.kafka=KafkaQueue(self.conf['topic'],self.conf['zookeeper_hosts'],host=self.conf['host'])

    def on_result(self, message):
        res=self.db['ArticlesDB'].get_detail(message['rid'])
        if 'on_sync' in message:
            res['flag']=message['on_sync']
        if '_id' in res:
            res.pop('_id')
        res.pop('rid')
        res.pop('crawlinfo')
        self.kafka.put_nowait(res)
