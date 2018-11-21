#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-11-21 16:02:24
"""
import time
import pymongo
from cdspider.database.base import SpiderTaskDB as BaseTaskDB
from .Mongo import Mongo, SplitTableMixin

class SpiderTaskDB(Mongo, BaseTaskDB, SplitTableMixin):

    __tablename__ = 'spider_task'

    def __init__(self, connector, table=None, **kwargs):
        super(TaskDB, self).__init__(connector, table = table, **kwargs)
        self._list_collection()

    def insert(self, obj):
        table = self._table_name(obj['mode'])
        obj['uuid'] = self._get_increment(table)
        obj.setdefault('status', self.STATUS_INIT)
        obj.setdefault('ctime', int(time.time()))
        obj.setdefault('utime', 0)
        obj.setdefault('pid', 0)
        _id = super(TaskDB, self).insert(setting=obj, table=table)
        return obj['uuid']

    def update(self, id, mode, obj):
        table = self._table_name(mode)
        obj['utime'] = int(time.time())
        return super(TaskDB, self).update(setting=obj, where={"uuid": int(id)}, table=table, multi=False)

    def update_many(self , mode, obj, where=None):
        obj['plantime']=int(time.time())
        if where=={} or where==None:
            return
        table = self._table_name(mode)
        obj['utime'] = int(time.time())
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def disable(self, id, mode, where = {}):
        table = self._table_name(mode)
        obj={"status": self.STATUS_INIT}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'uuid':int(id)}
        else:
            where.update({'uuid':int(id)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=False)

    def disable_by_param(self, pid, mode, where = {}):
        table = self._table_name(mode)
        obj={"status": self.STATUS_INIT}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"pid": int(pid)}
        else:
            where.update({"pid": int(pid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def disable_by_url(self, uid, mode, where = {}):
        table = self._table_name(mode)
        obj={"status": self.STATUS_INIT}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"uid": int(uid)}
        else:
            where.update({"uid": int(uid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def active(self, id, mode, where = {}):
        table = self._table_name(mode)
        obj={"status": self.STATUS_ACTIVE}
        obj['utime'] = int(time.time())
        if not where:
            where = {'uuid':int(id)}
        else:
            where.update({'uuid':int(id)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=False)

    def active_by_param(self, pid, mode, where = {}):
        table = self._table_name(mode)
        obj={"status": self.STATUS_ACTIVE}
        obj['utime'] = int(time.time())
        if not where:
            where = {"pid": int(pid)}
        else:
            where.update({"pid": int(pid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def active_by_url(self, uid, mode, where = {}):
        table = self._table_name(mode)
        obj={"status": self.STATUS_ACTIVE}
        obj['utime'] = int(time.time())
        obj['plantime'] = int(time.time())
        if not where:
            where = {"uid": int(uid)}
        else:
            where.update({"uid": int(uid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def delete(self, id, mode, obj, where = {}):
        table = self._table_name(mode)
        obj={"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'uuid':int(id)}
        else:
            where.update({'uuid':int(id)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=False)

    def delete_by_param(self, pid, mode, where = {}):
        table = self._table_name(mode)
        obj={"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"pid": int(pid)}
        else:
            where.update({"pid": int(pid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def delete_by_url(self, uid, mode, where = {}):
        table = self._table_name(mode)
        obj={"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"uid": int(uid)}
        else:
            where.update({"uid": int(uid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def get_detail(self, id, mode, crawlinfo=False):
        table = self._table_name(mode)
        if crawlinfo:
            select=None
        else:
            select={"crawlinfo": False}
        return self.get(where={"uuid": int(id)}, select=select, table=table)

    def get_count(self, mode, where = {}):
        table = self._table_name(mode)
        return self.count(where=where, table=table)

    def get_list(self, pid, where = {}, select=None, **kwargs):
        table = self._table_name(pid)
        kwargs.setdefault('sort', [('uuid', 1)])
        return self.find(where=where, table=table, select=select, **kwargs)

    def get_plan_list(self, mode, plantime = None, where = {}, select=None, **kwargs):
        table = self._table_name(mode)
        now = int(time.time())
        if not plantime:
            plantime = now
        where = self._build_where(where)
        _where = {'$and':[{'status':self.STATUS_ACTIVE},{'plantime':{'$lte': plantime}},{'$or':[{'expire':0},{'expire':{'$gt': now}}]}]}
        for k, v in where.items():
            _where['$and'].extend([{k: v}])
        kwargs.setdefault('sort', [('uuid', 1)])
        return self.find(where=_where, table=table, select=select, **kwargs)

    def _table_name(self, mode):
        table = super(TaskDB, self)._collection_name(mode)
        if not table in self._collections:
            self._create_collection(table)
        return table

    def _create_collection(self, table):
        collection = self._db.get_collection(table)
        indexes = collection.index_information()
        if not 'uuid' in indexes:
            collection.create_index('uuid', unique=True, name='uuid')
        if not 'u_p' in indexes:
            collection.create_index([("uid", pymongo.ASCENDING), ("pid", pymongo.ASCENDING)], unique=True, name='u_p')
        if not 'pid' in indexes:
            collection.create_index('pid', name='pid')
        if not 'uid' in indexes:
            collection.create_index('uid', name='uid')
        if not 'status' in indexes:
            collection.create_index('status', name='status')
        if not 'expire' in indexes:
            collection.create_index('expire', name='expire')
        if not 'plantime' in indexes:
            collection.create_index('plantime', name='plantime')
        if not 'ctime' in indexes:
            collection.create_index('ctime', name='ctime')
        self._collections.add(table)
