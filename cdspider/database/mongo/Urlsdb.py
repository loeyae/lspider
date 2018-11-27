#-*- coding: utf-8 -*-

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
        if not 'uuid' in indexes:
            collection.create_index('uuid', unique=True, name='uuid')
        if not 'pid' in indexes:
            collection.create_index('pid', name='pid')
        if not 'sid' in indexes:
            collection.create_index('sid', name='sid')
        if not 'typeChannel' in indexes:
            collection.create_index('typeChannel', name='typeChannel')
        if not 'typeList' in indexes:
            collection.create_index('typeList', name='typeList')
        if not 'typeDetail' in indexes:
            collection.create_index('typeDetail', name='typeDetail')
        if not 'typeOther' in indexes:
            collection.create_index('typeOther', name='typeOther')
        if not 'linkText' in indexes:
            collection.create_index('linkText', name='linkText')
        if not 'url' in indexes:
            collection.create_index([('url', pymongo.TEXT)], name='url')
        if not 'cluster' in indexes:
            collection.create_index('cluster', name='cluster')
        if not 'dataNum' in indexes:
            collection.create_index('dataNum', name='dataNum')
        if not 'addAuthor' in indexes:
            collection.create_index('addAuthor', name='addAuthor')
        if not 'status' in indexes:
            collection.create_index('status', name='status')

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

    def get_new_list(self, id, sid, where = {}, select=None, **kwargs):
        kwargs.setdefault('sort', [('uuid', 1)])
        if not where:
            where = {}
        where['uuid'] = {'$gt': int(id)}
        where['sid'] = int(sid)
        return self.find(where = where, select=select, **kwargs)

    def get_new_list_by_pid(self, id, pid, where = {}, select=None, **kwargs):
        kwargs.setdefault('sort', [('uuid', 1)])
        if not where:
            where = {}
        where['uuid'] = {'$gt': int(id)}
        where['pid'] = pid
        return self.find(where = where, select=select, **kwargs)
