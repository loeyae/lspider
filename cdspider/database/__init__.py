#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
#version: SVN: $Id: __init__.py 1377 2018-06-21 12:37:54Z zhangyi $
import abc
import six
import logging

from .Basedb import BaseDB
from cdspider.libs.utils import md5

@six.add_metaclass(abc.ABCMeta)
class BaseDataBase(object):
    """
    数据库操作基类
    """

    def __init__(self, *args, **kwargs):
        self.username = kwargs.pop('user', None)
        self.password = kwargs.pop('password', None)
        self.host = kwargs.pop('host', 'localhost')
        self.port = kwargs.pop('port', 0)
        self.db = kwargs.pop('db')
        self.table = kwargs.pop('table', None)
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
    def find(self, where, table = None, select = None, sort = None, offset = 0, hits = 10):
        """
        多行查询
        """
        pass

    @abc.abstractmethod
    def get(self, where, table = None, select = None, sort = None):
        """
        单行查询
        """
        pass

    @abc.abstractmethod
    def insert(self, setting, table = None):
        """
        插入数据
        """
        pass

    @abc.abstractmethod
    def update(self, setting, where, table = None, multi = False):
        """
        修改数据
        """
        pass

    @abc.abstractmethod
    def delete(self, where, table = None, multi = False):
        """
        删除数据
        """
        pass

    @abc.abstractmethod
    def count(self, where, select = None, table = None):
        """
        count
        """
        pass

    @abc.abstractmethod
    def close(self):
        """
        关闭数据库连接
        """
        pass
