#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
import abc
import six
import logging

@six.add_metaclass(abc.ABCMeta)
class Base(object):
    """
    数据库连接基类
    """

    def __init__(self, *args, **kwargs):
        self.username = kwargs.pop('user', None)
        self.password = kwargs.pop('password', None)
        self.host = kwargs.pop('host', 'localhost')
        self.port = kwargs.pop('port', 0)
        self.db = kwargs.pop('db')
        self.logger = kwargs.pop('logger', logging.getLogger('db'))
        log_level = kwargs.pop('log_level', logging.WARN)
        self.logger.setLevel(log_level)
        self.setting = kwargs
        self.connect()

    def __del__(self):
        """
        关闭数据库连接
        """
        self.close()

    def symbol(self):
        return md5("%s:%s@%s:%s/%s" % (self.username, self.password, self.host, self.port, self.db))

    @abc.abstractmethod
    def connect(self):
        """
        连接数据库
        """
        pass

    @abc.abstractmethod
    def close(self):
        """
        关闭数据库连接
        """
        pass

    @property
    def cursor(self):
        raise NotImplementedError

    from .Mongo import Mongo