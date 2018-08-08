#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 21:32:22
:version: SVN: $Id: Sitedb.py 2115 2018-07-04 03:56:07Z zhangyi $
"""
import time
import pymongo
from cdspider.database.base import SitesDB as BaseSitesDB
from .Mongo import Mongo

class SitesDB(Mongo, BaseSitesDB):

    __tablename__ = 'sites'

    def __init__(self, connector, table=None, **kwargs):
        super(SitesDB, self).__init__(connector, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if not 'sid' in indexes:
            collection.create_index('sid', unique=True, name='sid')
        if not 'p_s' in indexes:
            collection.create_index([('pid', pymongo.ASCENDING),('status', pymongo.ASCENDING)], name='p_s')
        if not 'type' in indexes:
            collection.create_index('type', name='type')
        if not 'ctime' in indexes:
            collection.create_index('ctime', name='ctime')

    def insert(self, obj={}):
        obj['sid'] = self._get_increment(self.table)
        obj.setdefault('status', self.STATUS_INIT)
        obj.setdefault('ctime', int(time.time()))
        obj.setdefault('utime', 0)
        _id = super(SitesDB, self).insert(setting=obj)
        return obj['sid']

    def disable(self, id, where = {}):
        if not where:
            where = {'sid': int(id)}
        else:
            where.update({'sid': int(id)})
        return super(SitesDB, self).update(setting={"status": self.STATUS_INIT, 'utime': int(time.time())},
                where=where, multi=False)

    def disable_by_project(self, pid, where = {}):
        if not where:
            where = {'pid': int(pid)}
        else:
            where.update({'pid': int(pid)})
        return super(SitesDB, self).update(setting={"status": self.STATUS_INIT, 'utime': int(time.time())},
                where=where, multi=True)

    def active(self, id, where = {}):
        if not where:
            where = {'sid': int(id)}
        else:
            where.update({'sid': int(id)})
        return super(SitesDB, self).update(setting={"status": self.STATUS_ACTIVE, 'utime': int(time.time())},
                where=where, multi=False)

    def update(self, id, obj = {}):
        obj['utime'] = int(time.time())
        return super(SitesDB, self).update(setting=obj, where={'sid': int(id)}, multi=False)

    def update_many(self, obj = {},where=None):
        if where=={} or where==None:
            return
        obj['utime'] = int(time.time())
        return super(SitesDB, self).update(setting=obj, where=where, multi=True)

    def delete(self, id, where = {}):
        if not where:
            where = {'sid': int(id)}
        else:
            where.update({'sid': int(id)})
        return super(SitesDB, self).update(setting={"status": self.STATUS_DELETED, 'utime': int(time.time())},
                where=where, multi=False)

    def delete_by_project(self, pid, where = {}):
        if not where:
            where = {'pid': int(pid)}
        else:
            where.update({'pid': int(pid)})
        return super(SitesDB, self).update(setting={"status": self.STATUS_DELETED, 'utime': int(time.time())},
                where=where, multi=True)

    def get_detail(self, id):
        return self.get(where={'sid': int(id)})

    def get_list(self, where, select=None, **kwargs):
        kwargs.setdefault('sort', [('sid', 1)])
        return self.find(where=where, select=select, **kwargs)

    def get_new_list(self, id, pid, where = {}, select=None, **kwargs):
        kwargs.setdefault('sort', [('sid', 1)])
        if not where:
            where == {}
        where['sid'] = {'$gt': int(id)}
        where['pid'] = pid
        return self.find(where=where, select=select, **kwargs)
