#-*- coding: utf-8 -*-
'''
@Author: wuhongchao
@Date: 2018-12-17 19:09:24
@LastEditors: wuhongchao
@LastEditTime: 2019-01-16 11:13:18
@Description: 文章、微博内容数据同步到kafka
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

class ArticleSyncKafkaWorker(BaseWorker):

    inqueue_key = QUEUE_NAME_ARTICLE_TO_KAFKA

    fields = ['acid','title','author','mediaType','result.viewUrl','url','domain','subdomain','pubtime','ctime',\
    'utime','channel','status','industry','territory','content','crawlinfo.pid','crawlinfo.sid',\
    'crawlinfo.tid','crawlinfo.uid','crawlinfo.list_url','uid','id','praise','profile_image_url','gender','follow_count',\
    'statuses_count','reposts','verified','verified_type','comment','retweeted_statuses_count',\
    'retweeted_gender','retweeted_comment','retweeted_url','retweeted_profile_image_url','retweeted_reposts',\
    'followers_count','retweeted_followers_count','retweeted_follow_count','retweeted_uid','retweeted_verified_type',\
    'retweeted_id','retweeted_pubtime','retweeted_verified','retweeted_name','retweeted_praise','retweeted_content',\
    'rowkey']

    def __init__(self, context):
        super(ArticleSyncKafkaWorker, self).__init__(context)
        self.conf = self.g['app_config'].get('sync_kafka')
        self.connection()

    def connection(self):
        if not self.conf:
            raise CDSpiderSettingError("Invalid Kafka Setting")
        self.kafka=KafkaQueue(self.conf['article_topic'], host=self.conf['host'])

    def on_result(self, message):
        #message['db'] = 'ArticlesDB'
        if 'rid' in message and 'db' in message:
            self.info("got message: %s" % message)
            res = self.db[message['db']].get_detail(message['rid'], self.fields)
            print(res)
            if res:
                siteInfo = self.db['SitesDB'].get_detail(res['crawlinfo']['sid'], ['industry', 'territory'])
                res['industry'] = int(siteInfo['industry'])
                res['territory'] = int(siteInfo['territory'])
            if message['db'] == 'WeiboInfoDB':
                res['mediaType'] = 12 #mediaType = 12 微博类型数据
            if 'mediaType' not in res:
                res['mediaType'] = 99 #mediaType = 99 其他类型数据
            if 'result' in res and 'viewUrl' in res['result']:      #如果viewUrl存在，删除url元素
                #res.pop('url')
                res['url'] = res['result']['viewUrl']
                res.pop('result')
            self.info("message: %s " % res)
            if 'rowkey' not in res:
                rowkey = self.generate_rowkey(res)
                res['rowkey'] = rowkey
                res['flag'] = 0
                try:
                    self.kafka.put_nowait(res)
                    self.db[message['db']].update(message['rid'], {'rowkey': rowkey})
                except Exception as e:
                    print(e)
            else:
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
