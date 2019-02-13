#-*- coding: utf-8 -*-
'''
@Author: wuhongchao
@Date: 2018-12-17 19:09:24
@LastEditors: wuhongchao
@LastEditTime: 2019-01-23 19:14:07
@Description: 互动数同步到kafka
'''
import sys
import traceback
import random
import time
import datetime
from cdspider.libs.constants import *
from cdspider.worker import BaseWorker
from cdspider.message_queue import KafkaQueue
from cdspider.exceptions import *

class AttachSyncKafkaWorker(BaseWorker):

    inqueue_key = QUEUE_NAME_ATTACH_TO_KAFKA

    def __init__(self, context):
        super(AttachSyncKafkaWorker, self).__init__(context)
        self.conf = self.g['app_config'].get('sync_kafka')
        self.connection()

    def connection(self):
        if not self.conf:
            raise CDSpiderSettingError("Invalid Kafka Setting")
        self.kafka=KafkaQueue(self.conf['attach_topic'], host=self.conf['host'])

    def on_result(self, message):
        if 'rid' in message:
            self.info("got message: %s" % message)
            #res = self.db['AttachDataDB'].get_detail(message['rid'], ['acid', 'viewNum', 'ctime', 'utime', 'spreadNum', 'mediaType', 'commentNum', 'likeNum', 'crawlinfo.list_url'])
            res = self.db['AttachDataDB'].get_detail(message['rid'])
            if 'mediaType' not in res:
                res['mediaType'] = 99 #mediaType = 99 其他类型数据
            self.info("message: %s " % res)
            try:
                print(res)
                self.kafka.put_nowait(res)
            except Exception as e:
                print(e)
