#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-5 0:04:09
"""
import time
import pymongo
from cdspider.database.base import ErrorLogDB as BaseErrorLogDB
from .Mongo import Mongo

class ErrorLogDB(Mongo, BaseErrorLogDB):
    """
    crawl_log data object
    """

    __tablename__ = 'error'

    def __init__(self, connector, table=None, **kwargs):
        super(ErrorLogDB, self).__init__(connector, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if not 'uuid' in indexes:
            collection.create_index('uuid', unique=True, name='uuid')
        if not 'tid' in indexes:
            collection.create_index('tid', name='tid')
        if not 'ctime' in indexes:
            collection.create_index('ctime', name='ctime')

    def insert(self, obj):
        obj['uuid'] = self._get_increment(self.table)
        obj.setdefault('status', self.STATUS_INIT)
        obj.setdefault('create_at', int(time.time()))
        _id = super(ErrorLogDB, self).insert(setting=obj)
        return obj['uuid']

    def update(self, id, obj = {}):
        obj['utime'] = int(time.time())
        return super(ErrorLogDB, self).update(setting=obj, where={"uuid": int(id)}, multi=False)

    def delete(self, id, where = {}):
        if not where:
            where = {'uuid': int(id)}
        else:
            where.update({'uuid': int(id)})
        return super(ErrorLogDB, self).update(setting={"status": self.STATUS_DELETED},
                where=where, multi=False)
