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
def catch_error(func):
    """
    Catch errors of kafka then reconnect
    """
    logger = logging.getLogger('queue')
    try:
        connect_exceptions = (
            exceptions.KafkaException,
        )
    except ImportError:
        connect_exceptions = ()

    connect_exceptions += (
        socket.error,
    )

    def wrap(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except connect_exceptions as e:
            logger.error('Kafka error: %r, reconnect.', e)
            self.connect()
            return func(self, *args, **kwargs)
    return wrap

class KafkaQueue(CDBaseQueue):

    def __init__(self, name, zookeeper_hosts,user=None, password=None, host="localhost", port=6379, path='0',
                 maxsize=0, lazy_limit=True, log_level = logging.WARN):
        """
        init
        """
        self.zookeeper_hosts=zookeeper_hosts
        super(KafkaQueue, self).__init__(name=name, user=user, password=password, host=host, port=port, path=path,
                 maxsize=maxsize, lazy_limit=lazy_limit, log_level=log_level)
        self.qsize=self.qsize()
        if self.lazy_limit and self.maxsize:
            self.qsize_diff_limit = int(self.maxsize * 0.1)
        else:
            self.qsize_diff_limit = 0
        self.qsize_diff = self.qsize - self.maxsize

    def connect(self):
        """
        连接queue
        """

        k = self.symbol()
        if not k in connection_pool:
            self.connect = KafkaClient(hosts=self.host,zookeeper_hosts=self.zookeeper_hosts)
            self.connect=self.connect.topics[self.queuename.encode()]
            connection_pool[k] = self.connect
        else:
            self.connect = connection_pool[k]

    def empty(self):
        if self.qsize() == 0:
            return True
        else:
            return False

    def full(self):
        if self.maxsize and self.qsize() >= self.maxsize:
            return True
        else:
            return False

    @catch_error
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

    @catch_error
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

    @catch_error
    def get_nowait(self, ack=False):
        consumer = self.connect.get_simple_consumer(b'cdspider',auto_commit_enable=True,auto_commit_interval_ms=1)
#         sum=self.connect.latest_available_offsets()[0][0][0]
        c=consumer.consume()
        print(c.offset)
#         self.qsize=sum-c.offset
        msg=c.value.decode('utf-8')
        msg=json.loads(msg)
        if msg is None:
            raise BaseQueue.Empty
        return msg

    @catch_error
    def put_nowait(self, obj):
        """
        直接发送
        （obj>>json格式）
        """
        self.logger.info('send kafka starting ....')
        producer = self.connect.get_producer(linger_ms=1,sync=False)
        obj=json.dumps(obj)
        obj=obj.encode(encoding='utf_8')
        producer.produce(obj)
        producer.stop()
        self.logger.info('send kafka end data: %s' % obj)


    @catch_error
    def qsize(self):
        """
        queue的条数
        """
#         if self.qsize:
#             consumer = self.connect.get_simple_consumer(b'cdspider')
#             sum=self.connect.latest_available_offsets()[0][0][0]
#             if sum==0:
#                 self.qsize=0
#                 return self.qsize 
#             c=consumer.consume()
#             self.qsize=sum-c.offset
#             producer = self.connect.get_producer()
#             producer.produce(c.value)
#             producer.stop()
        self.qsize=100
        return self.qsize


    def close(self):
        pass
    
if __name__=='__main__':
    k=KafkaQueue('test2_queue',host='114.112.86.135:6667,114.112.86.136:6667,114.112.86.137:6667')
    for i in range(5):
        k.put_nowait({'test':i})
#     print(k.get_nowait())
