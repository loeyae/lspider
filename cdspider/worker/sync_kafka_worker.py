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

    def __init__(self, context):
        super(SyncKafkaWorker, self).__init__(context)
        self.conf = self.g['app_config'].get('sync_kafka')
        self.connection()

    def connection(self):
        if not self.conf:
            raise CDSpiderSettingError("Invalid Kafka Setting")
        self.kafka=KafkaQueue(self.conf['topic'], self.conf['zookeeper_hosts'], host=self.conf['host'])

    def on_result(self, message):
        self.info("got message: %s" % message)
        res=self.db['ArticlesDB'].get_detail(message['rid'], select = ["subdomain", "domain", "ctime", "channel", "acid", "url", "pubtime", "title", "status", "author", "content", "media_type", "result", "utime"])
        if 'on_sync' in message:
            res['flag']=message['on_sync']
        if '_id' in res:
            res.pop('_id')
        res.pop('rid', None)
        res.pop('crawlinfo', None)
        self.info("message: %s " % res)
        self.kafka.put_nowait(res)
