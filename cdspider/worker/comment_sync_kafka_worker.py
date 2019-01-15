#-*- coding: utf-8 -*-
'''
@Author: wuhongchao
@Date: 2018-12-17 19:09:24
@LastEditors: wuhongchao
@LastEditTime: 2019-01-15 10:15:43
@Description: 评论内容同步到kafka
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

class CommentSyncKafkaWorker(BaseWorker):

    inqueue_key = QUEUE_NAME_COMMENT_TO_KAFKA

    def __init__(self, context):
        super(CommentSyncKafkaWorker, self).__init__(context)
        self.conf = self.g['app_config'].get('sync_kafka')
        self.connection()

    def connection(self):
        if not self.conf:
            raise CDSpiderSettingError("Invalid Kafka Setting")
        self.kafka=KafkaQueue(self.conf['comment_topic'], host=self.conf['host'])

    def on_result(self, message):
        if 'id' in message and 'rid' in message:
            self.info("got message: %s" % message)
            res = self.db['CommentsDB'].get_detail(message['id'], message['rid'], ['author', 'ctime', 'comment', 'pubtime', 'acid', 'rowkey', 'crawlinfo.list_url'])
            if 'mediaType' not in res:
                res['mediaType'] = 99 #mediaType = 99 其他类型数据
            self.info("message: %s " % res)
            if 'rowkey' not in res:
                rowkey = self.generate_rowkey(res)
                res['rowkey'] = rowkey
                res['flag'] = 0
                #try:
                print(res)
                self.kafka.put_nowait(res)
                #self.db['CommentsDB'].update(message['id'], message['rid'], {'rowkey': rowkey})
                #except Exception as e:
                #    print(e)
            else:
                print(res)
                self.kafka.put_nowait(res)
                try:
                    res['flag'] = 1
                    self.kafka.put_nowait(res)
                except Exception as e:
                    print(e)

    def generate_rowkey(self, res):
        """
        生成唯一key，生成规则：三位随机数（100-999）+mediatype（2位）+puime（年月日，8位）+时间戳（13位)
        """
        #rand = random.randint(100,999)

        timeArray = time.localtime(res['pubtime'])
        otherStyleTime = time.strftime("%Y%m%d", timeArray)
        nowtime = lambda:int(round(time.time() * 1000))
        rowkey = str(res['mediaType']).zfill(2) + str(otherStyleTime) + str(nowtime())
        return rowkey
