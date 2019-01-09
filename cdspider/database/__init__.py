#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
import abc
import six
import logging

from cdspider.libs.utils import md5

@six.add_metaclass(abc.ABCMeta)
class BaseDataBase(object):
    """
    数据库操作基类
    """

    __tablename__ = None

    def __init__(self, connector, *args, **kwargs):
        self.conn = connector
        self.table = kwargs.pop('table', None)
        self.logger = kwargs.pop('logger', logging.getLogger('db'))
        log_level = kwargs.pop('log_level', logging.WARN)
        self.logger.setLevel(log_level)
        self.setting = kwargs

    def __del__(self):
        for h in self.logger.handlers:
            h.flush()

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
