#-*- coding: utf-8 -*-

'''
Created on 2018年8月5日

@author: Wang Fengwei
'''

from pykafka import KafkaClient
from pykafka import exceptions
from six.moves import queue as BaseQueue
from cdspider.message_queue import BaseQueue as CDBaseQueue
import time
import logging
import socket
import umsgpack
import json

connection_pool = {}

class KafkaQueue(CDBaseQueue):

    def __init__(self, name, zookeeper_hosts,user=None, password=None, host="localhost", port=6379, path='0',
                 maxsize=0, lazy_limit=True, log_level = logging.WARN):
        """
        init
        """
        self.zookeeper_hosts=zookeeper_hosts
        super(KafkaQueue, self).__init__(name=name, user=user, password=password, host=host, port=port, path=path,
                 maxsize=maxsize, lazy_limit=lazy_limit, log_level=log_level)

    def connect(self):
        """
        连接queue
        """

        k = self.symbol()
        if not k in connection_pool:
            self.connect = KafkaClient(hosts=self.host)
            self.connect=self.connect.topics[self.queuename.encode()]
            connection_pool[k] = self.connect
        else:
            self.connect = connection_pool[k]


    def get(self, block=True, timeout=None, ack=False):
        """
        获取queue
        """
        if not block:
            return self.get_nowait()

        start_time = time.time()
        while True:
            try:
                return self.get_nowait(ack)
            except BaseQueue.Empty:
                if timeout:
                    lasted = time.time() - start_time
                    if timeout > lasted:
                        time.sleep(min(self.max_timeout, timeout - lasted))
                    else:
                        raise
                else:
                    time.sleep(self.max_timeout)

    def put(self, obj, block=True, timeout=None):
        if not block:
            return self.put_nowait()

        start_time = time.time()
        while True:
            try:
                return self.put_nowait(obj)
            except BaseQueue.Full:
                if timeout:
                    lasted = time.time() - start_time
                    if timeout > lasted:
                        time.sleep(min(self.max_timeout, timeout - lasted))
                    else:
                        raise
                else:
                    time.sleep(self.max_timeout)

    def get_nowait(self, ack=False):
        consumer = self.connect.get_simple_consumer(b'cdspider',auto_commit_enable=True,auto_commit_interval_ms=1)
#         sum=self.connect.latest_available_offsets()[0][0][0]
        c=consumer.consume()
#         self.qsize=sum-c.offset
        msg=c.value.decode('utf-8')
        msg=json.loads(msg)
        if msg is None:
            raise BaseQueue.Empty
        return msg

    def put_nowait(self, obj):
        """
        直接发送
        （obj>>json格式）
        """
        self.logger.info('send kafka starting ....')
        producer = self.connect.get_producer(linger_ms=0.1,sync=False)
        obj=json.dumps(obj)
        obj=obj.encode(encoding='utf_8')
        producer.produce(obj)
        producer.stop()
        self.logger.info('send kafka end data: %s' % obj)

    def empty(self):
        pass

    def full(self):
        pass

    def qsize(self):
        pass


    def close(self):
        pass

if __name__=='__main__':
    k=KafkaQueue('test2_queue',host='114.112.86.135:6667,114.112.86.136:6667,114.112.86.137:6667')
    for i in range(5):
        k.put_nowait({'test':i})
#     print(k.get_nowait())
