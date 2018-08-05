#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-4 21:00:32
"""

from pymongo import MongoClient
from cdspider.exceptions import *
from . import Base

connection_pool = {}

class Mongo(Base):
    """
    Mongo 连接类
    """

    def __init__(self, host='localhost', port=27017, db = None, user=None,
                password=None, **kwargs):
        super(Mongo, self).__init__(host = host, port = port, db = db,
            user = user, password = password, **kwargs)

    def connect(self):
        """
        连接数据库
        """
        try:
            k = self.symbol()
            if not k in connection_pool:
                self.conn = MongoClient(host = self.host, port = self.port, **self.setting)
                #登录认证
                if self.db:
                    self._db = self.conn[self.db]
                else:
                    self._db = self.conn.get_default_database();
                #登录认证
                if self.username:
                    self._db.authenticate(self.username, self.password)
                connection_pool[k] = {"c": self.conn, 'd': self._db}
            else:
                self.conn = connection_pool[k]["c"]
                self._db = connection_pool[k]["d"]
        except Exception as e:
            raise CDSpiderDBError(e, host=self.host, prot=self.port, db=self.db)

    def close(self):
        """
        关闭数据库连接
        """
        if hasattr(self, 'conn'):
            self.conn.close()
            del self.conn

    @property
    def cursor(self):
        return self._db