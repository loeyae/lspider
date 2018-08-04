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

    def __init__(self, host='localhost', port=27017, db = None, user=None,
            password=None, table=None, **kwargs):
        super(TaskDB, self).__init__(host = host, port = port, db = db,
            user = user, password = password, table = table, **kwargs)
        self._list_collection()

    def insert(self, obj):
        table = self._table_name(obj['projectid'])
        obj['tid'] = self._get_increment(table)
        obj.setdefault('status', self.TASK_STATUS_INIT)
        obj.setdefault('createtime', int(time.time()))
        obj.setdefault('updatetime', 0)
        obj.setdefault('urlid', 0)
        obj.setdefault('atid', 0)
        obj.setdefault('kwid', 0)
        _id = super(TaskDB, self).insert(setting=obj, table=table)
        return obj['tid']

    def update(self, id, pid, obj):
        table = self._table_name(pid)
        obj['updatetime'] = int(time.time())
        return super(TaskDB, self).update(setting=obj, where={"tid": int(id)}, table=table, multi=False)

    def update_url_by_site(self, id, pid, url):
        table = self._table_name(pid)
        obj={"url": url}
        obj['updatetime'] = int(time.time())
        return super(TaskDB, self).update(setting=obj, where={"siteid": int(id), "atid": 0, "urlid": 0}, table=table, multi=False)

    def update_url_by_urls(self, id, pid, url):
        table = self._table_name(pid)
        obj={"url": url}
        obj['updatetime'] = int(time.time())
        return super(TaskDB, self).update(setting=obj, where={"urlid": int(id), "atid": 0}, table=table, multi=False)

    def enable(self, id, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = 0
        obj['save'] = None
        if not where:
            where = {'tid':int(id)}
        else:
            where.update({'tid':int(id)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=False)

    def enable_by_project(self, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = 0
        obj['save'] = None
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def enable_by_site(self, sid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = 0
        obj['save'] = None
        if not where:
            where = {"siteid": int(sid)}
        else:
            where.update({"siteid": int(sid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def enable_by_urls(self, uid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = 0
        obj['save'] = None
        if not where:
            where = {"urlid": int(uid)}
        else:
            where.update({"urlid": int(uid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def enable_by_attachment(self, aid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = 0
        obj['save'] = None
        if not where:
            where = {"atid": int(aid)}
        else:
            where.update({"atid": int(aid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def enable_by_keyword(self, kid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = 0
        obj['save'] = None
        if not where:
            where = {"kwid": int(kid)}
        else:
            where.update({"kwid": int(kid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def disable(self, id, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'tid':int(id)}
        else:
            where.update({'tid':int(id)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=False)

    def disable_by_project(self, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def disable_by_site(self, sid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"siteid": int(sid)}
        else:
            where.update({"siteid": int(sid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def disable_by_urls(self, uid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"urlid": int(uid)}
        else:
            where.update({"urlid": int(uid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def disable_by_attachment(self, aid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"atid": int(aid)}
        else:
            where.update({"atid": int(aid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def disable_by_keyword(self, kid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"kwid": int(kid)}
        else:
            where.update({"kwid": int(kid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def active(self, id, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_ACTIVE}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'tid':int(id)}
        else:
            where.update({'tid':int(id)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=False)

    def active_by_site(self, sid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_ACTIVE}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {"siteid": int(sid)}
        else:
            where.update({"siteid": int(sid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def active_by_urls(self, uid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_ACTIVE}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = int(time.time())
        if not where:
            where = {"urlid": int(uid)}
        else:
            where.update({"urlid": int(uid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def active_by_attachment(self, aid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_ACTIVE}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = int(time.time())
        if not where:
            where = {"atid": int(aid)}
        else:
            where.update({"atid": int(aid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def active_by_keyword(self, kid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_ACTIVE}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = int(time.time())
        if not where:
            where = {"kwid": int(kid)}
        else:
            where.update({"kwid": int(kid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def delete(self, id, pid, obj, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'tid':int(id)}
        else:
            where.update({'tid':int(id)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=False)

    def delete_by_project(self, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def delete_by_site(self, sid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"siteid": int(sid)}
        else:
            where.update({"siteid": int(sid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def delete_by_urls(self, uid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"urlid": int(uid)}
        else:
            where.update({"urlid": int(uid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def delete_by_attachment(self, uid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"atid": int(aid)}
        else:
            where.update({"atid": int(aid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def delete_by_keyword(self, kid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {"kwid": int(kid)}
        else:
            where.update({"kwid": int(kid)})
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

    def get_init_list(self, pid, where = {}, select=None, **kwargs):
        table = self._table_name(pid)
        where['status'] = self.TASK_STATUS_ACTIVE
        where['plantime'] = 0
        kwargs.setdefault('sort', [('tid', 1)])
        return self.find(where=where, table=table, select=select, **kwargs)

    def get_plan_list(self, pid, plantime, where = {}, select=None, **kwargs):
        table = self._table_name(pid)
        where['status'] = TaskDB.TASK_STATUS_ACTIVE
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
        if not 's_u_a_k' in indexes:
            collection.create_index([('siteid', 1), ('urlid', 1), ('atid', 1), ('kwid', 1)], unique=True, name='s_u_k')
        if not 'status' in indexes:
            collection.create_index('status', name='status')
        if not 'plantime' in indexes:
            collection.create_index('plantime', name='plantime')
        if not 'createtime' in indexes:
            collection.create_index('createtime', name='createtime')
        self._collections.add(table)
