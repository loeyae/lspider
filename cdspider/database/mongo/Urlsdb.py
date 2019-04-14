# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 21:58:31
:version: SVN: $Id: Urlsdb.py 2116 2018-07-04 03:56:12Z zhangyi $
"""
import time
import pymongo
from cdspider.database.base import UrlsDB as BaseUrlsDB
from .Mongo import Mongo

class UrlsDB(Mongo, BaseUrlsDB):

    __tablename__ = 'urls'

    incr_key = 'url'

    def __init__(self, connector, table=None, **kwargs):
        super(UrlsDB, self).__init__(connector, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if 'uuid' not in indexes:
            collection.create_index('uuid', unique=True, name='uuid')
        if 'pid' not in indexes:
            collection.create_index('pid', name='pid')
        if 'sid' not in indexes:
            collection.create_index('sid', name='sid')
        if 'tid' not in indexes:
            collection.create_index('tid', name='tid')
        if 'typeChannel' not in indexes:
            collection.create_index('typeChannel', name='typeChannel')
        if 'typeList' not in indexes:
            collection.create_index('typeList', name='typeList')
        if 'typeDetail' not in indexes:
            collection.create_index('typeDetail', name='typeDetail')
        if 'typeOther' not in indexes:
            collection.create_index('typeOther', name='typeOther')
        if 'linkText' not in indexes:
            collection.create_index('linkText', name='linkText')
        if 'url' not in indexes:
            collection.create_index([('url', pymongo.TEXT)], name='url')
        if 'cluster' not in indexes:
            collection.create_index('cluster', name='cluster')
        if 'dataNum' not in indexes:
            collection.create_index('dataNum', name='dataNum')
        if 'addAuthor' not in indexes:
            collection.create_index('addAuthor', name='addAuthor')
        if 'status' not in indexes:
            collection.create_index('status', name='status')
        if 'ruleStatus' not in indexes:
            collection.create_index('ruleStatus', name='ruleStatus')

    def insert(self, obj = {}):
        obj['uuid'] = self._get_increment(self.incr_key)
        obj.setdefault('status', self.STATUS_INIT)
        obj.setdefault('ctime', int(time.time()))
        obj.setdefault('dataNum', 0)
        _id = super(UrlsDB, self).insert(setting=obj)
        return obj['uuid']

    def update(self, id, obj = {}):
        obj['utime'] = int(time.time())
        return super(UrlsDB, self).update(setting=obj, where={'uuid': int(id)}, multi=False)

    def update_many(self, obj = {},where=None):
        if where=={} or where==None:
            return
        obj['utime'] = int(time.time())
        return super(UrlsDB, self).update(setting=obj, where=where, multi=True)

    def delete(self, id, where = {}):
        obj = {"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        if not where:
            where = {'uuid': int(id)}
        else:
            where.update({'uuid': int(id)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=False)

    def delete_by_site(self, sid, where = {}):
        obj = {"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        if not where:
            where = {"sid": int(sid)}
        else:
            where.update({"sid": int(sid)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=True)

    def delete_by_project(self, pid, where = {}):
        obj = {"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        if not where:
            where = {'pid': int(pid)}
        else:
            where.update({'pid': int(pid)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=True)

    def active(self, id, where = {}):
        obj = {"status": self.STATUS_ACTIVE}
        obj['utime'] = int(time.time())
        if not where:
            where = {'uuid': int(id)}
        else:
            where.update({'uuid': int(id)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=False)

    def disable(self, id, where = {}):
        obj = {"status": self.STATUS_INIT}
        obj['utime'] = int(time.time())
        if not where:
            where = {'uuid': int(id)}
        else:
            where.update({'uuid': int(id)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=False)

    def disable_by_site(self, sid, where = {}):
        obj = {"status": self.STATUS_INIT}
        obj['utime'] = int(time.time())
        if not where:
            where = {"sid": int(sid)}
        else:
            where.update({"sid": int(sid)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=True)

    def disable_by_project(self, pid, where = {}):
        obj = {"status": self.STATUS_INIT}
        obj['utime'] = int(time.time())
        if not where:
            where = {'pid': int(pid)}
        else:
            where.update({'pid': int(pid)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=True)

    def get_detail(self, id):
        return self.get(where={'uuid': int(id)})

    def get_list(self, where = {}, select=None, **kwargs):
        kwargs.setdefault('sort', [('uuid', 1)])
        return self.find(where=where, select=select, **kwargs)

    def get_new_list(self, id, where = {}, select=None, **kwargs):
        kwargs.setdefault('sort', [('uuid', 1)])
        if not where:
            where = {}
        where = self._build_where(where)
        _where = {'$and':[{"$or": [{"typeChannelList": UrlsDB.IS_TYPE_CHANNEL_LIST}, {"typeChannel": UrlsDB.IS_TYPE_CHANNEL}, {"typeList": UrlsDB.IS_TYPE_LIST}]}, {"uuid": {"$gt": id}}]}
        for k, v in where.items():
            _where['$and'].extend([{k: v}])
        return self.find(where = _where, select=select, **kwargs)

    def get_new_list_by_pid(self, id, pid, where = {}, select=None, **kwargs):
        kwargs.setdefault('sort', [('uuid', 1)])
        if not where:
            where = {}
        where = self._build_where(where)
        _where = {'$and':[{"$or": [{"typeChannelList": UrlsDB.IS_TYPE_CHANNEL_LIST}, {"typeChannel": UrlsDB.IS_TYPE_CHANNEL}, {"typeList": UrlsDB.IS_TYPE_LIST}]}, {"uuid": {"$gt": id}}, {"pid": pid}]}
        for k, v in where.items():
            _where['$and'].extend([{k: v}])
        return self.find(where = _where, select=select, **kwargs)

    def get_count(self, where = {}):
        return self.count(where = where)