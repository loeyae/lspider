# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

import abc
import six
import logging
from cdspider.libs.utils import md5

@six.add_metaclass(abc.ABCMeta)
class BaseQueue(object):
    """
    queue操作基类
    """

    def __init__(self, *args, **kwargs):
        """
        init
        """
        self.queuename = kwargs.pop('name')
        self.exchange= kwargs.pop('exchange', None)
        self.username = kwargs.pop('user', None)
        self.password = kwargs.pop('password', None)
        self.host = kwargs.pop('host', 'localhost')
        self.port = kwargs.pop('port', 5672)
        self.path = kwargs.pop('path', '%2F')
        self.maxsize = kwargs.pop('maxsize', 0)
        self.lazy_limit = kwargs.pop('lazy_limit', True)
        self.logger = kwargs.pop('logger', logging.getLogger('parser'))
        log_level = kwargs.pop('log_level', logging.WARN)
        self.logger.setLevel(log_level)
        self.connect()

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def symbol(self):
        return md5("%s:%s@%s:%s/%s" % (self.username, self.password, self.host, self.port, self.path))

    @abc.abstractmethod
    def connect(self):
        """
        连接queue
        """
        pass

    @abc.abstractmethod
    def empty(self):
        """
        queue是否为空
        """
        pass

    @abc.abstractmethod
    def full(self):
        """
        queue是否已满
        """
        pass

    @abc.abstractmethod
    def get(self, block=True, timeout=None, ack=False):
        """
        获取queue
        """

    @abc.abstractmethod
    def put(self, obj, block=True, timeout=None):
        """
        发送queue
        """
        pass

    @abc.abstractmethod
    def get_nowait(self, ack=False):
        """
        直接获取
        """
        pass

    @abc.abstractmethod
    def put_nowait(self, obj, pack = True):
        """
        直接发送
        """
        pass

    @abc.abstractmethod
    def qsize(self):
        """
        queue的条数
        """
        pass

    @abc.abstractmethod
    def close(self):
        pass

from .MessageQueue import PikaQueue, AmqpQueue
from .RedisQueue import RedisQueue
from .KafkaQueue import  KafkaQueue
