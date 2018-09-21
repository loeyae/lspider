#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-6-21 18:53:06
:version: SVN: $Id: ChannelRulesdb.py 2114 2018-07-04 03:56:01Z zhangyi $
"""
import time
import pymongo
from cdspider.database.base import ChannelRulesDB as BaseChannelRulesDB
from .Mongo import Mongo

class ChannelRulesDB(Mongo, BaseChannelRulesDB):
    """
    ChannelRulesDB
    """
    __tablename__ = 'channel_rules'

    incr_key = 'channel_rules'

    def __init__(self, connector, table=None, **kwargs):
        super(ChannelRulesDB, self).__init__(connector, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if not 'crid' in indexes:
            collection.create_index('crid', unique=True, name='crid')
        if not 'title' in indexes:
            collection.create_index('title', name='title')
        if not 's_s' in indexes:
            collection.create_index([('sid', pymongo.ASCENDING),('status', pymongo.ASCENDING)], name='s_s')
        if not 'url' in indexes:
            collection.create_index([('url', pymongo.TEXT)], name='url')
        if not 'ctime' in indexes:
            collection.create_index('ctime', name='ctime')

    def insert(self, obj = {}):
        obj['crid'] = self._get_increment(self.incr_key)
        obj.setdefault('status', self.STATUS_INIT)
        obj.setdefault('ctime', int(time.time()))
        obj.setdefault('utime', 0)
        _id = super(ChannelRulesDB, self).insert(setting=obj)
        return obj['crid']

    def update(self, id, obj = {}):
        obj['utime'] = int(time.time())
        return super(ChannelRulesDB, self).update(setting=obj, where={'crid': int(id)}, multi=False)

    def update_many(self, obj = {},where=None):
        if where=={} or where==None:
            return
        obj['utime'] = int(time.time())
        return super(ChannelRulesDB, self).update(setting=obj, where=where, multi=True)

    def delete(self, id, where = {}):
        obj = {"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        if not where:
            where = {'crid': int(id)}
        else:
            where.update({'crid': int(id)})
        return super(ChannelRulesDB, self).update(setting=obj, where=where, multi=False)

    def active(self, id, where = {}):
        obj = {"status": self.STATUS_ACTIVE}
        obj['utime'] = int(time.time())
        if not where:
            where = {'crid': int(id)}
        else:
            where.update({'crid': int(id)})
        return super(ChannelRulesDB, self).update(setting=obj, where=where, multi=False)

    def disable(self, id, where = {}):
        obj = {"status": self.STATUS_INIT}
        obj['utime'] = int(time.time())
        if not where:
            where = {'crid': int(id)}
        else:
            where.update({'crid': int(id)})
        return super(ChannelRulesDB, self).update(setting=obj, where=where, multi=False)

    def get_detail(self, id):
        return self.get(where={'crid': int(id)})

    def get_list(self, where = {}, select=None, **kwargs):
        kwargs.setdefault('sort', [('crid', 1)])
        return self.find(where=where, select=select, **kwargs)

    def get_list_by_sid(self, sid, where = {}, select=None, **kwargs):
        kwargs.setdefault('sort', [('crid', 1)])
        if not where:
            where = {}
        where['sid'] = sid
        return self.find(where=where, select=select, **kwargs)

    def get_new_list_by_pid(self, id, pid, where = {}, select=None, **kwargs):
        kwargs.setdefault('sort', [('crid', 1)])
        if not where:
            where = {}
        where['crid'] = {'$gt': int(id)}
        where['pid'] = pid
        return self.find(where = where, select=select, **kwargs)
