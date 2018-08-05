#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-5 0:04:09
"""
import time
import pymongo
from cdspider.database.base import CrawlLogDB as BaseCrawlLogDB
from .Mongo import Mongo

class CrawlLogDB(Mongo, BaseCrawlLogDB):
    """
    crawl_log data object
    """

    __tablename__ = 'crawl_log'

    def __init__(self, connector, table=None, **kwargs):
        super(CrawlLogDB, self).__init__(connector, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if not 'id' in indexes:
            collection.create_index('id', unique=True, name='id')
        if not 'tid' in indexes:
            collection.create_index('tid', name='tid')
        if not 'ctime' in indexes:
            collection.create_index('ctime', name='ctime')

    def insert(self, obj):
        obj['id'] = self._get_increment(self.table)
        obj.setdefault('status', self.STATUS_INIT)
        obj.setdefault('ctime', int(time.time()))
        obj.setdefault('utime', 0)
        _id = super(CrawlLogDB, self).insert(setting=obj)
        return obj['id']

    def update(self, id, obj = {}):
        obj['utime'] = int(time.time())
        return super(CrawlLogDB, self).update(setting=obj, where={"prid": int(id)}, multi=False)

    def delete(self, id, where = {}):
        if not where:
            where = {'id': int(id)}
        else:
            where.update({'id': int(id)})
        return super(CrawlLogDB, self).update(setting={"status": self.STATUS_DELETED},
                where=where, multi=False)
