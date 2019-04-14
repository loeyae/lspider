# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-5 0:04:09
"""
import time
from cdspider.database.base import ErrorLogDB as BaseErrorLogDB
from .Mongo import Mongo, SplitTableMixin


class ErrorLogDB(Mongo, BaseErrorLogDB, SplitTableMixin):
    """
    crawl_log data object
    """

    __tablename__ = 'error'

    def __init__(self, connector, table=None, **kwargs):
        super(ErrorLogDB, self).__init__(connector, table = table, **kwargs)
        self._check_collection()

    def insert(self, obj):
        obj['uuid'] = self._get_increment(self.table)
        obj.setdefault('status', self.STATUS_INIT)
        obj.setdefault('create_at', int(time.time()))
        table = self._get_collection(obj['create_at'])
        obj['lid'] = BaseErrorLogDB.build_id(obj['create_at'], obj['uuid'])
        obj.setdefault('status', self.STATUS_INIT)
        super(ErrorLogDB, self).insert(setting=obj, table=table)
        return obj['lid']

    def update(self, id, obj={}):
        table = self._table_name(id)
        obj['utime'] = int(time.time())
        return super(ErrorLogDB, self).update(setting=obj, where={"lid": id}, table=table, multi=False)

    def delete(self, id, where={}):
        table = self._table_name(id)
        if not where:
            where = {'lid': id}
        else:
            where.update({'lid': id})
        return super(ErrorLogDB, self).update(setting={"status": self.STATUS_DELETED},
                table=table, where=where, multi=False)

    def _get_collection(self, ctime):
        suffix = time.strftime("%Y%m", time.localtime(ctime))
        name = super(ErrorLogDB, self)._collection_name(suffix)
        if name not in self._collections:
            self._create_collection(name)
        return name

    def _table_name(self, id):
        suffix, _ = BaseErrorLogDB.unbuild_id(id)
        name = super(ErrorLogDB, self)._collection_name(suffix)
        if name not in self._collections:
            self._create_collection(name)
        return name

    def _check_collection(self):
        self._list_collection()
        suffix = time.strftime("%Y%m")
        name = super(ErrorLogDB, self)._collection_name(suffix)
        if name not in self._collections:
            self._create_collection(name)

    def _create_collection(self, table):
        collection = self._db.get_collection(table)
        indexes = collection.index_information()
        if 'uuid' not in indexes:
            collection.create_index('uuid', unique=True, name='uuid')
        if 'lid' not in indexes:
            collection.create_index('lid', unique=True, name='lid')
        if 'create_ad' not in indexes:
            collection.create_index('create_ad', name='create_ad')
        if 'tid' not in indexes:
            collection.create_index('tid', name='tid')
        self._collections.add(table)
