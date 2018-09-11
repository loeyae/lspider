#-*- coding: utf-8 -*-

'''
Created on 2018年8月6日

@author: Wang Fengwei
'''

from cdspider.worker import BaseWorker
from cdspider.message_queue import KafkaQueue

class insert_kafka_worker(BaseWorker):

    inqueue_key = "result2kafka"

    def __init__(self,db,queue,conf,log_level):
        super(insert_kafka_worker, self).__init__(db, queue, proxy=None, log_level = log_level)
        self.conf=conf
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
        self.logger.info("message: " % res)
        self.kafka.put_nowait(res)
