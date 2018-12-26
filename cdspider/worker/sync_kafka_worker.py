#-*- coding: utf-8 -*-

'''
Created on 2018年8月6日

@author: Wang Fengwei
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

class SyncKafkaWorker(BaseWorker):

    inqueue_key = QUEUE_NAME_RESULT_TO_KAFKA

    def __init__(self, context):
        super(SyncKafkaWorker, self).__init__(context)
        self.conf = self.g['app_config'].get('sync_kafka')
        self.connection()

    def connection(self):
        if not self.conf:
            raise CDSpiderSettingError("Invalid Kafka Setting")
        self.kafka=KafkaQueue(self.conf['topic'], host=self.conf['host'])

    def on_result(self, message):
        #message['db'] = 'ArticlesDB'
        print(message)
        if 'rid' in message and 'db' in message:
            self.info("got message: %s" % message)
            res = self.db[message['db']].get_detail(message['rid'])
            if '_id' in res:
                res.pop('_id')
            if 'mediaType' not in res:
                res['mediaType'] = 0
            self.info("message: %s " % res)
            if 'rowkey' not in res:
                print(res)
                rowkey = self.generate_rowkey(res)
                res['rowkey'] = rowkey
                try:
                    print(res)
                    self.kafka.put_nowait(res)
                    self.db[message['db']].update(message['rid'], {'rowkey': rowkey})
                except Exception as e:
                    print(e)
            else:
                print(res)
                try:
                    self.kafka.put_nowait(res)
                except Exception as e:
                    print(e)


    def generate_rowkey(self, res):
        """
        生成唯一key，生成规则：三位随机数（100-999）+mediatype（2位）+puime（年月日，8位）+时间戳（13位)
        """
        rand = random.randint(100,999)

        #mediatype = res['mediaType'] if 'mediaType' in res else 0

        timeArray = time.localtime(res['pubtime'])
        otherStyleTime = time.strftime("%Y%m%d", timeArray)

        nowtime = lambda:int(round(time.time() * 1000))
        
        #print(str(rand) + str(mediatype).zfill(2) + str(otherStyleTime) + str(nowtime()))

        rowkey = str(rand) + str(res['mediaType']).zfill(2) + str(otherStyleTime) + str(nowtime())
        return rowkey