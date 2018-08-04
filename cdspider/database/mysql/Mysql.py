#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:47:29
:version: SVN: $Id: Mysql.py 1723 2018-06-27 01:46:07Z zhangyi $
"""
import mysql.connector

from cdspider.database import BaseDataBase
from cdspider.database import BaseDB
from cdspider.exceptions import *
connection_pool = {}

class Mysql(BaseDataBase, BaseDB):
    """
    Mysql 操作类
    """

    def __init__(self, host='localhost', port=3306, db = None, user=None,
                password=None, table=None, **kwargs):
        if table:
            self.__tablename__ = table
        super(Mysql, self).__init__(host = host, port = port, db = db,
            user = user, password = password, table = table, **kwargs)

    def connect(self):
        """
        连接数据库
        """
        try:
            k = self.symbol()
            if not k in connection_pool:
                self.conn = mysql.connector.connect(user=self.username, password=self.password,
                                                host=self.host, port=self.port, autocommit=True)
                if self.db not in [x[0] for x in self._execute('SHOW DATABASES')]:
                    self._execute('CREATE DATABASE %s CHARSET utf8' % self.escape(self.db))
                self.conn.database = self.db
                connection_pool[k] = self.conn
            else:
                self.conn = connection_pool[k]
        except Exception as e:
            raise CDSpiderDBError(e, host=self.host, prot=self.port, db=self.db)

    @property
    def cursor(self):
        try:
            if self.conn.unread_result:
                self.conn.get_rows()
            return self.conn.cursor()
        except (mysql.connector.OperationalError, mysql.connector.InterfaceError):
            self.conn.ping(reconnect=True)
            self.conn.database = self.db
            return self.conn.cursor()

    @property
    def archive_fields(self):
        return None

    def have_table(self, table):
        if table in [x[0] for x in self._execute('SHOW TABLES')]:
            return True
        return False

    def close(self):
        """
        关闭数据库连接
        """
        try:
            self.conn.close()
        except:
            pass

    def find(self, where, table = None, select = None, group=None, having=None, sort = None, offset = 0, hits = 10):
        """
        多行查询
        """
        result = self._select2dic(where=where, table=table, select=select, group=group, having=having, sort=sort, offset=offset, hits=hits)
        for item in result:
            yield self.archive_unzip(item)

    def get(self, where, table = None, select = None, group=None, having=None, sort = None):
        """
        单行查询
        """
        result = self._select2dic(where=where, table=table, select=select, group=group, having=having, sort=sort, offset=0, hits=1)
        for item in result:
            return self.archive_unzip(item)

    def insert(self, setting, table = None):
        """
        插入数据
        """
        setting = self.archive_zip(setting)
        return self._insert(setting=setting, table=table)

    def update(self, setting, where, table = None, multi = False):
        """
        修改数据
        """
        setting = self.archive_zip(setting)
        return self._update(setting=setting, where=where, table=table, multi=multi)

    def delete(self, where, table = None, multi = False):
        """
        删除数据
        """
        return self._delete(where=where, table=table, multi=multi)

    def count(self, where, select = None, table = None, group=None, having=None, sort=None):
        """
        count
        """
        if not select:
            select = "COUNT(*) AS count"
        if group:
            return self.find(where=where, select=select, table=table, group=group, having=having, sort=sort, hits=0)
        result = self.get(where=where, select=select, table=table, having=having, sort=sort)
        if result:
            return result['count']
        return 0

class SplitTableMixin(object):

    __tablename__ = None

    def _table_name(self, suffix):
        if not suffix:
            raise NotImplementedError
        return "%s.%s" % (self.__tablename__, suffix)

    def _list_table(self):
        self._tables = set()
        prefix = "%s." % self.__tablename__
        for each in [x[0] for x in self._execute('SHOW TABLES')]:
            if each.startswith(prefix):
                self._tables.add(each)
