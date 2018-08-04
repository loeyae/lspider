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

    def __init__(self, host='localhost', port=27017, db = None, user=None,
            password=None, table=None, **kwargs):
        super(UrlsDB, self).__init__(host = host, port = port, db = db,
            user = user, password = password, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if not 'uid' in indexes:
            collection.create_index('uid', unique=True, name='uid')
        if not 'title' in indexes:
            collection.create_index('title', name='title')
        if not 's_s' in indexes:
            collection.create_index([('siteid', pymongo.ASCENDING),('status', pymongo.ASCENDING)], name='s_s')
        if not 'url' in indexes:
            collection.create_index([('url', pymongo.TEXT)], name='url')
        if not 'createtime' in indexes:
            collection.create_index('createtime', name='createtime')

    def insert(self, obj = {}):
        obj['uid'] = self._get_increment(self.table)
        obj.setdefault('status', self.URLS_STATUS_INIT)
        obj.setdefault('createtime', int(time.time()))
        obj.setdefault('updatetime', 0)
        _id = super(UrlsDB, self).insert(setting=obj)
        return obj['uid']

    def update(self, id, obj = {}):
        obj['updatetime'] = int(time.time())
        return super(UrlsDB, self).update(setting=obj, where={'uid': int(id)}, multi=False)

    def enable(self, id, where = {}):
        obj = {"status": self.URLS_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'uid': int(id)}
        else:
            where.update({'uid': int(id)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=False)

    def enable_by_site(self, sid, where = {}):
        obj = {"status": self.URLS_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {"siteid": int(sid)}
        else:
            where.update({"siteid": int(sid)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=True)

    def enable_by_project(self, pid, where = {}):
        obj = {"status": self.URLS_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'projectid': int(pid)}
        else:
            where.update({'projectid': int(pid)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=True)

    def delete(self, id, where = {}):
        obj = {"status": self.URLS_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'uid': int(id)}
        else:
            where.update({'uid': int(id)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=False)

    def delete_by_site(self, sid, where = {}):
        obj = {"status": self.URLS_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {"siteid": int(sid)}
        else:
            where.update({"siteid": int(sid)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=True)

    def delete_by_project(self, pid, where = {}):
        obj = {"status": self.URLS_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'projectid': int(pid)}
        else:
            where.update({'projectid': int(pid)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=True)

    def active(self, id, where = {}):
        obj = {"status": self.URLS_STATUS_ACTIVE}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'uid': int(id)}
        else:
            where.update({'uid': int(id)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=False)

    def disable(self, id, where = {}):
        obj = {"status": self.URLS_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'uid': int(id)}
        else:
            where.update({'uid': int(id)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=False)

    def disable_by_site(self, sid, where = {}):
        obj = {"status": self.URLS_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {"siteid": int(sid)}
        else:
            where.update({"siteid": int(sid)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=True)

    def disable_by_project(self, pid, where = {}):
        obj = {"status": self.URLS_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'projectid': int(pid)}
        else:
            where.update({'projectid': int(pid)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=True)

    def get_detail(self, id):
        return self.get(where={'uid': int(id)})

    def get_list(self, where = {}, select=None, **kwargs):
        kwargs.setdefault('sort', [('uid', 1)])
        return self.find(where=where, select=select, **kwargs)

    def get_new_list(self, id, siteid, select=None, **kwargs):
        kwargs.setdefault('sort', [('uid', 1)])
        return self.find(where={'uid': {'$gt': int(id)}, 'siteid': siteid}, select=select, **kwargs)

    def get_max_id(self, siteid):
        data = self.get(where={"siteid": siteid}, sort=[('uid', -1)], select={'uid': True})
        return data['uid']
