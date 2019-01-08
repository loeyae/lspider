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
from .Mongo import Mongo, SplitTableMixin

class CrawlLogDB(Mongo, BaseCrawlLogDB, SplitTableMixin):
    """
    crawl_log data object
    """

    __tablename__ = 'crawl_log'

    def __init__(self, connector, table=None, **kwargs):
        super(CrawlLogDB, self).__init__(connector, table = table, **kwargs)
        self._check_collection()
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if not 'uuid' in indexes:
            collection.create_index('uuid', unique=True, name='uuid')
        if not 'stid' in indexes:
            collection.create_index('stid', name='stid')
        if not 'ctime' in indexes:
            collection.create_index('ctime', name='ctime')

    def insert(self, obj):
        obj['uuid'] = self._get_increment(self.table)
        obj.setdefault('status', self.STATUS_INIT)
        obj.setdefault('ctime', int(time.time()))
        table = self._get_collection(obj['ctime'])
        obj['lid'] = BaseCrawlLogDB.build_id(obj['ctime'], obj['uuid'])
        obj.setdefault('utime', 0)
        obj.setdefault('errid', 0)
        super(CrawlLogDB, self).insert(setting=obj, table=table)
        return obj['lid']

    def update(self, id, obj = {}):
        table = self._table_name(id)
        obj['utime'] = int(time.time())
        return super(CrawlLogDB, self).update(setting=obj, where={"lid": id}, table=table, multi=False)

    def delete(self, id, where = {}):
        table = self._table_name(id)
        if not where:
            where = {'lid': id}
        else:
            where.update({'lid': id})
        return super(CrawlLogDB, self).update(setting={"status": self.STATUS_DELETED},
                table=table, where=where, multi=False)

    def _get_collection(self, ctime):
        suffix = time.strftime("%Y%m", time.localtime(ctime))
        name = super(CrawlLogDB, self)._collection_name(suffix)
        if not name in self._collections:
            self._create_collection(name)
        return name

    def _table_name(self, id):
        suffix, _ = BaseCrawlLogDB.unbuild_id(id)
        name = super(CrawlLogDB, self)._collection_name(suffix)
        if not name in self._collections:
            self._create_collection(name)
        return name


    def _check_collection(self):
        self._list_collection()
        suffix = time.strftime("%Y%m")
        name = super(ArticlesDB, self)._collection_name(suffix)
        if not name in self._collections:
            self._create_collection(name)

    def _create_collection(self, table):
        collection = self._db.get_collection(table)
        indexes = collection.index_information()
        if not 'rid' in indexes:
            collection.create_index('rid', unique=True, name='rid')
        if not 'acid' in indexes:
            collection.create_index('acid', unique=True, name='acid')
        if not 'domain' in indexes:
            collection.create_index('domain', name='domain')
        if not 'subdomain' in indexes:
            collection.create_index('subdomain', name='subdomain')
        if not 'status' in indexes:
            collection.create_index('status', name='status')
        if not 'pubtime' in indexes:
            collection.create_index('pubtime', name='pubtime')
        if not 'ctime' in indexes:
            collection.create_index('ctime', name='ctime')
        if not 'pid' in indexes:
            collection.create_index('crawlinfo.pid', name='pid')
        if not 'sid' in indexes:
            collection.create_index('crawlinfo.sid', name='sid')
        if not 'tid' in indexes:
            collection.create_index('crawlinfo.tid', name='tid')
        if not 'uid' in indexes:
            collection.create_index('crawlinfo.uid', name='uid')
        if not 'kid' in indexes:
            collection.create_index('crawlinfo.kid', name='kid')
        self._collections.add(table)
