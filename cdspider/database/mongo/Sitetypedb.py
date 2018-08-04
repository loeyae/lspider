#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-13 12:48:16
:version: SVN: $Id: Sitetypedb.py 1052 2018-06-08 03:55:04Z zhangyi $
"""
import time
import pymongo
from cdspider.database.base import SitetypeDB
from .Mongo import Mongo

class SitetypeDB(Mongo, SitetypeDB):
    """
    站点类型
    """

    __tablename = 'sitetype'

    def __init__(self, host='localhost', port=27017, db = None, user=None,
            password=None, table=None, **kwargs):
        super(SitetypeDB, self).__init__(host = host, port = port, db = db,
            user = user, password = password, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if not 'stid' in indexes:
            collection.create_index('stid', unique=True, name='stid')
        if not 'type' in indexes:
            collection.create_index('type', name='type')
        if not 'domain' in indexes:
            collection.create_index([('domain', pymongo.ASCENDING), ('subdomain', pymongo.ASCENDING)], unique=True, name='domain')
        if not 'createtime' in indexes:
            collection.create_index('createtime', name='createtime')

    def insert(self, obj={}):
        obj['stid'] = self._get_increment(self.table)
        obj.setdefault('subdomain', '')
        obj.setdefault('createtime', int(time.time()))
        obj.setdefault('updatetime', 0)
        obj.setdefault('updatetime', 0)
        obj.setdefault('status', self.SITETYPE_STATUS_NORMAl)
        _id = super(SitetypeDB, self).insert(setting=obj)
        return obj['stid']

    def update(self, id, obj = {}):
        obj['updatetime'] = int(time.time())
        return super(SitetypeDB, self).update(setting=obj, where={'stid': int(id)}, multi=False)

    def delete(self, id):
        return super(SitetypeDB, self).update(setting={"status": self.SITETYPE_STATUS_DELETED},
                where={'stid': int(id)}, multi=False)

    def get_detail(self, id):
        return self.get(where={'stid': int(id)})

    def get_detail_by_domain(self, domain, subdomain=None):
        res = None
        if subdomain:
            return self.get(where={'subdomain':subdomain, 'domain': domain})
        return self.get(where={'domain': domain})

    def get_list(self, where = {}, select=None, **kwargs):
        kwargs.setdefault('sort', [('stid', 1)])
        return self.find(where=where, select=select, **kwargs)
