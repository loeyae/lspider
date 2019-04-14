# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 21:33:44
:version: SVN: $Id: Taskdb.py 2141 2018-07-04 06:43:11Z zhangyi $
"""
import time
from cdspider.database.base import TaskDB as BaseTaskDB
from .Mongo import Mongo, SplitTableMixin


class TaskDB(Mongo, BaseTaskDB, SplitTableMixin):

    __tablename__ = 'task'

    def __init__(self, connector, table=None, **kwargs):
        super(TaskDB, self).__init__(connector, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if 'uuid' not in indexes:
            collection.create_index('uuid', unique=True, name='uuid')
        if 'pid' not in indexes:
            collection.create_index('pid', name='pid')
        if 'sid' not in indexes:
            collection.create_index('sid', name='sid')
        if 'status' not in indexes:
            collection.create_index('status', name='status')

    def insert(self, obj):
        table = self._table_name(obj['pid'])
        obj['uuid'] = self._get_increment(table)
        obj.setdefault('status', self.STATUS_INIT)
        obj.setdefault('ctime', int(time.time()))
        obj.setdefault('utime', 0)
        _id = super(TaskDB, self).insert(setting=obj)
        return obj['uuid']

    def update(self, id, obj):
        obj['utime'] = int(time.time())
        return super(TaskDB, self).update(setting=obj, where={'uuid': int(id)}, multi=False)

    def update_many(self , obj, where=None):
        if where=={} or where==None:
            return
        obj['utime'] = int(time.time())
        return super(TaskDB, self).update(setting=obj, where=where, multi=True)

    def disable(self, id, where={}):
        obj={"status": self.STATUS_INIT}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'uuid':int(id)}
        else:
            where.update({'uuid':int(id)})
        return super(TaskDB, self).update(setting=obj, where=where, multi=False)

    def active(self, id, where={}):
        obj={"status": self.STATUS_ACTIVE}
        obj['utime'] = int(time.time())
        if not where:
            where = {'uuid':int(id)}
        else:
            where.update({'uuid':int(id)})
        return super(TaskDB, self).update(setting=obj, where=where, multi=False)

    def delete(self, id, obj, where={}):
        obj={"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'uuid':int(id)}
        else:
            where.update({'uuid':int(id)})
        return super(TaskDB, self).update(setting=obj, where=where, multi=False)

    def get_detail(self, id, select=None):
        return self.get(where={'uuid': int(id)}, select=select)

    def get_count(self, where={}):
        return self.count(where=where)

    def get_list(self, where={}, select=None, **kwargs):
        kwargs.setdefault('sort', [('uuid', 1)])
        return self.find(where=where, select=select, **kwargs)

    def get_new_list(self, id, where={}, select=None, **kwargs):
        if not where:
            where == {}
        where['uuid'] = {'$gt': int(id)}
        kwargs.setdefault('sort', [('uuid', 1)])
        return self.find(where=where, select=select, **kwargs)
