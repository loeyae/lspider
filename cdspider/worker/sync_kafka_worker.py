#-*- coding: utf-8 -*-

'''
Created on 2018年8月6日

@author: Wang Fengwei
'''
import traceback
from cdspider.worker import BaseWorker
from cdspider.message_queue import KafkaQueue
from cdspider.exceptions import *

class SyncKafkaWorker(BaseWorker):

    inqueue_key = "result2kafka"

    def __init__(self, db, queue, kafka_cfg, log_level):
        super(SyncKafkaWorker, self).__init__(db, queue, proxy=None, log_level = log_level)
        self.conf = kafka_cfg
        self.connection()

    def connection(self):
        if not self.conf:
            raise CDSpiderSettingError("Invalid Kafka Setting")
        self.kafka=KafkaQueue(self.conf['topic'], self.conf['zookeeper_hosts'], host=self.conf['host'])

    def on_result(self, message):
        self.info("got message: %s" % message)
        res=self.db['ArticlesDB'].get_detail(message['rid'])
        if 'on_sync' in message:
            res['flag']=message['on_sync']
        if '_id' in res:
            res.pop('_id')
        res.pop('rid')
        res.pop('crawlinfo')
        self.info("message: %s " % res)
        self.kafka.put_nowait(res)
