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
from cdspider.database.base import SiteDB as BaseSiteDB
from .Mongo import Mongo

class SiteDB(Mongo, BaseSiteDB):

    __tablename__ = 'sites'

    def __init__(self, host='localhost', port=27017, db = None, user=None,
            password=None, table=None, **kwargs):
        super(SiteDB, self).__init__(host = host, port = port, db = db,
            user = user, password = password, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if not 'sid' in indexes:
            collection.create_index('sid', unique=True, name='sid')
        if not 'p_s' in indexes:
            collection.create_index([('projectid', pymongo.ASCENDING),('status', pymongo.ASCENDING)], name='p_s')
        if not 's_s' in indexes:
            collection.create_index([('stid', pymongo.ASCENDING),('status', pymongo.ASCENDING)], name='s_s')
        if not 'type' in indexes:
            collection.create_index('type', name='type')
        if not 'lastuid' in indexes:
            collection.create_index('lastuid', name='lastuid')
        if not 'lastkwid' in indexes:
            collection.create_index('lastkwid', name='lastkwid')
        if not 'createtime' in indexes:
            collection.create_index('createtime', name='createtime')

    def insert(self, obj={}):
        obj['sid'] = self._get_increment(self.table)
        obj.setdefault('status', self.SITE_STATUS_INIT)
        obj.setdefault('limb', self.SITE_LIMB_FALSE)
        obj.setdefault('createtime', int(time.time()))
        obj.setdefault('updatetime', 0)
        _id = super(SiteDB, self).insert(setting=obj)
        return obj['sid']

    def enable(self, id, where = {}):
        if not where:
            where = {'sid': int(id)}
        else:
            where.update({'sid': int(id)})
        return super(SiteDB, self).update(setting={"status": self.SITE_STATUS_INIT, 'updatetime': int(time.time())},
                where=where, multi=False)

    def enable_by_project(self, pid, where = {}):
        if not where:
            where = {'projectid': int(pid)}
        else:
            where.update({'projectid': int(pid)})
        return super(SiteDB, self).update(setting={"status": self.SITE_STATUS_INIT, 'updatetime': int(time.time())},
                where=where, multi=True)

    def disable(self, id, where = {}):
        if not where:
            where = {'sid': int(id)}
        else:
            where.update({'sid': int(id)})
        return super(SiteDB, self).update(setting={"status": self.SITE_STATUS_DISABLE, 'updatetime': int(time.time())},
                where=where, multi=False)

    def disable_by_project(self, pid, where = {}):
        if not where:
            where = {'projectid': int(pid)}
        else:
            where.update({'projectid': int(pid)})
        return super(SiteDB, self).update(setting={"status": self.SITE_STATUS_DISABLE, 'updatetime': int(time.time())},
                where=where, multi=True)

    def active(self, id, where = {}):
        if not where:
            where = {'sid': int(id)}
        else:
            where.update({'sid': int(id)})
        return super(SiteDB, self).update(setting={"status": self.SITE_STATUS_ACTIVE, 'updatetime': int(time.time())},
                where=where, multi=False)

    def update(self, id, obj = {}):
        obj['updatetime'] = int(time.time())
        return super(SiteDB, self).update(setting=obj, where={'sid': int(id)}, multi=False)

    def delete(self, id, where = {}):
        if not where:
            where = {'sid': int(id)}
        else:
            where.update({'sid': int(id)})
        return super(SiteDB, self).update(setting={"status": self.SITE_STATUS_DELETED, 'updatetime': int(time.time())},
                where=where, multi=False)

    def delete_by_project(self, pid, where = {}):
        if not where:
            where = {'projectid': int(pid)}
        else:
            where.update({'projectid': int(pid)})
        return super(SiteDB, self).update(setting={"status": self.SITE_STATUS_DELETED, 'updatetime': int(time.time())},
                where=where, multi=True)

    def get_detail(self, id):
        return self.get(where={'sid': int(id)})

    def get_new_list(self, id, projectid, select=None, **kwargs):
        return self.find(where={'sid': {'$gt': int(id)}, 'projectid': projectid, "status": self.SITE_STATUS_ACTIVE}, select=select, sort=[('sid', 1)], **kwargs)

    def get_list(self, where, select=None, **kwargs):
        kwargs.setdefault('sort', [('sid', 1)])
        return self.find(where=where, select=select, **kwargs)

    def get_max_id(self, projectid):
        kwargs.setdefault('sort', [('sid', 1)])
        data = self.get(where={"projectid": projectid}, select={'sid': True})
        return data['sid']
