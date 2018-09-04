#-*- coding: utf-8 -*-

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
        self._list_collection()

    def insert(self, obj):
        table = self._table_name(obj['pid'])
        obj['tid'] = self._get_increment(table)
        obj.setdefault('status', self.STATUS_INIT)
        obj.setdefault('ctime', int(time.time()))
        obj.setdefault('utime', 0)
        obj.setdefault('uid', 0)
        obj.setdefault('aid', 0)
        obj.setdefault('kwid', 0)
        _id = super(TaskDB, self).insert(setting=obj, table=table)
        return obj['tid']

    def update(self, id, pid, obj):
        table = self._table_name(pid)
        obj['utime'] = int(time.time())
        return super(TaskDB, self).update(setting=obj, where={"tid": int(id)}, table=table, multi=False)

    def update_many(self , pid, obj, where=None):
        obj['plantime']=int(time.time())
        if where=={} or where==None:
            return
        table = self._table_name(pid)
        obj['utime'] = int(time.time())
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def update_url_by_site(self, id, pid, url):
        table = self._table_name(pid)
        obj={"url": url}
        obj['utime'] = int(time.time())
        return super(TaskDB, self).update(setting=obj, where={"sid": int(id), "aid": 0, "uid": 0}, table=table, multi=False)

    def update_url_by_urls(self, id, pid, url):
        table = self._table_name(pid)
        obj={"url": url}
        obj['utime'] = int(time.time())
        return super(TaskDB, self).update(setting=obj, where={"uid": int(id), "aid": 0}, table=table, multi=False)

    def disable(self, id, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.STATUS_INIT}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'tid':int(id)}
        else:
            where.update({'tid':int(id)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=False)

    def disable_by_project(self, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.STATUS_INIT}
        obj['utime'] = int(time.time())
        obj['save'] = None
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def disable_by_site(self, sid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.STATUS_INIT}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"sid": int(sid)}
        else:
            where.update({"sid": int(sid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def disable_by_urls(self, uid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.STATUS_INIT}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"uid": int(uid)}
        else:
            where.update({"uid": int(uid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def disable_by_attachment(self, aid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.STATUS_INIT}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"aid": int(aid)}
        else:
            where.update({"aid": int(aid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def disable_by_keyword(self, kwid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.STATUS_INIT}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"kwid": int(kwid)}
        else:
            where.update({"kwid": int(kwid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def active(self, id, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.STATUS_ACTIVE}
        obj['utime'] = int(time.time())
        if not where:
            where = {'tid':int(id)}
        else:
            where.update({'tid':int(id)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=False)

    def active_by_site(self, sid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.STATUS_ACTIVE}
        obj['utime'] = int(time.time())
        if not where:
            where = {"sid": int(sid)}
        else:
            where.update({"sid": int(sid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def active_by_urls(self, uid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.STATUS_ACTIVE}
        obj['utime'] = int(time.time())
        obj['plantime'] = int(time.time())
        if not where:
            where = {"uid": int(uid)}
        else:
            where.update({"uid": int(uid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def active_by_attachment(self, aid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.STATUS_ACTIVE}
        obj['utime'] = int(time.time())
        obj['plantime'] = int(time.time())
        if not where:
            where = {"aid": int(aid)}
        else:
            where.update({"aid": int(aid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def active_by_keyword(self, kwid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.STATUS_ACTIVE}
        obj['utime'] = int(time.time())
        obj['plantime'] = int(time.time())
        if not where:
            where = {"kwid": int(kwid)}
        else:
            where.update({"kwid": int(kwid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def delete(self, id, pid, obj, where = {}):
        table = self._table_name(pid)
        obj={"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'tid':int(id)}
        else:
            where.update({'tid':int(id)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=False)

    def delete_by_project(self, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        obj['save'] = None
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def delete_by_site(self, sid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"sid": int(sid)}
        else:
            where.update({"sid": int(sid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def delete_by_urls(self, uid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"uid": int(uid)}
        else:
            where.update({"uid": int(uid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def delete_by_attachment(self, uid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"aid": int(aid)}
        else:
            where.update({"aid": int(aid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def delete_by_keyword(self, kwid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"kwid": int(kwid)}
        else:
            where.update({"kwid": int(kwid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def get_detail(self, id, pid, crawlinfo=False):
        table = self._table_name(pid)
        if crawlinfo:
            select=None
        else:
            select={"crawlinfo": False}
        return self.get(where={"tid": int(id)}, select=select, table=table)

    def get_count(self, pid, where = {}):
        table = self._table_name(pid)
        return self.count(where=where, table=table)

    def get_list(self, pid, where = {}, select=None, **kwargs):
        table = self._table_name(pid)
        kwargs.setdefault('sort', [('tid', 1)])
        return self.find(where=where, table=table, select=select, **kwargs)

    def get_plan_list(self, pid, plantime, where = {}, select=None, **kwargs):
        table = self._table_name(pid)
        where['status'] = TaskDB.STATUS_ACTIVE
        where['plantime'] = {"$lte": plantime}
        kwargs.setdefault('sort', [('tid', 1)])
        return self.find(where=where, table=table, select=select, **kwargs)

    def _table_name(self, pid):
        table = super(TaskDB, self)._collection_name(pid)
        if not table in self._collections:
            self._create_collection(table)
        return table

    def _create_collection(self, table):
        collection = self._db.get_collection(table)
        indexes = collection.index_information()
        if not 'tid' in indexes:
            collection.create_index('tid', unique=True, name='tid')
        if not 'p_s_u_kw_a' in indexes:
            collection.create_index({"pid": 1, "sid": 1, "uid": 1, "kwid": 1, "aid": 1}, unique=True, name='p_s_u_kw_a')
        if not 'pid' in indexes:
            collection.create_index('pid', name='pid')
        if not 'sid' in indexes:
            collection.create_index('sid', name='sid')
        if not 'uid' in indexes:
            collection.create_index('uid', name='uid')
        if not 'kwid' in indexes:
            collection.create_index('kwid', name='kwid')
        if not 'aid' in indexes:
            collection.create_index('aid', name='aid')
        if not 'status' in indexes:
            collection.create_index('status', name='status')
        if not 'expire' in indexes:
            collection.create_index('expire', name='expire')
        if not 'plantime' in indexes:
            collection.create_index('plantime', name='plantime')
        if not 'ctime' in indexes:
            collection.create_index('ctime', name='ctime')
        self._collections.add(table)
