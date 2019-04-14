# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-29 11:23:19
"""
import time
import logging
import socket
import umsgpack
import json
import redis
from six.moves import queue as BaseQueue
from cdspider.message_queue import BaseQueue as CDBaseQueue

connection_pool = {}

def catch_error(func):
    """
    Catch errors of redis then reconnect
    """
    logger = logging.getLogger('queue')
    try:
        connect_exceptions = (
            redis.ConnectionError,
            redis.TimeoutError,
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
            logger.error('Redis error: %r, reconnect.', e)
            k = self.symbol()
            if k in connection_pool:
                del connection_pool[k]
            self.connect()
            return func(self, *args, **kwargs)
    return wrap

class RedisQueue(CDBaseQueue):

    def __init__(self, name, user=None, password=None, host="localhost", port=6379, path='0',
                 maxsize=0, lazy_limit=True, log_level = logging.WARN):
        """
        init
        """
        super(RedisQueue, self).__init__(name=name, user=user, password=password, host=host, port=port, path=path,
                 maxsize=maxsize, lazy_limit=lazy_limit, log_level=log_level)
        if self.lazy_limit and self.maxsize:
            self.qsize_diff_limit = int(self.maxsize * 0.1)
        else:
            self.qsize_diff_limit = 0
        self.qsize_diff = self.qsize() - self.maxsize

    def connect(self):
        """
        连接queue
        """

        k = self.symbol()
        if k not in connection_pool:
            connectionPool = redis.ConnectionPool(host=self.host, port=self.port, db=self.path, password=self.password)
            self.connect = redis.Redis(connection_pool=connectionPool)
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
        body = self.connect.lpop(self.queuename)
        if body is None:
            raise BaseQueue.Empty
        try:
            s=umsgpack.unpackb(body)
        except:
            s=json.loads(body.decode())
        return s

    @catch_error
    def put_nowait(self, obj, pack = True):
        """
        直接发送
        """
        if self.lazy_limit and self.qsize_diff < self.qsize_diff_limit:
            pass
        elif self.full():
            raise BaseQueue.Full
        else:
            self.qsize_diff = 0
        self.qsize_diff += 1
        if pack:
            return self.connect.rpush(self.queuename, umsgpack.packb(obj))
        return self.connect.rpush(self.queuename, json.dumps(obj))

    @catch_error
    def qsize(self):
        """
        queue的条数
        """
        return self.connect.llen(self.queuename)


    def close(self):
        pass
