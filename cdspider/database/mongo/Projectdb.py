#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 21:29:26
:version: SVN: $Id: Projectdb.py 1074 2018-06-08 07:11:48Z zhangyi $
"""
import time
import pymongo
from cdspider.database.base import ProjectDB as BaseProjectDB
from .Mongo import Mongo

class ProjectDB(Mongo, BaseProjectDB):

    __tablename__ = 'projects'

    def __init__(self, host='localhost', port=27017, db = None, user=None,
            password=None, table=None, **kwargs):
        super(ProjectDB, self).__init__(host = host, port = port, db = db,
            user = user, password = password, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if not 'pid' in indexes:
            collection.create_index('pid', unique=True, name='pid')
        if not 't_s' in indexes:
            collection.create_index([('type', pymongo.ASCENDING),('status', pymongo.ASCENDING)], name='t_s')
        if not 'lastsid' in indexes:
            collection.create_index('lastsid', name='lastsid')
        if not 'createtime' in indexes:
            collection.create_index('createtime', name='createtime')

    def get_detail(self, id):
        return self.get(where={'pid': int(id)})

    def insert(self, obj):
        obj['pid'] = self._get_increment(self.table)
        obj.setdefault('type', self.PROJECT_TYPE_GENERAL)
        obj.setdefault('status', self.PROJECT_STATUS_INIT)
        obj.setdefault('createtime', int(time.time()))
        obj.setdefault('updatetime', 0)
        super(ProjectDB, self).insert(setting=obj)
        return obj['pid']

    def update(self, id, obj):
        obj['updatetime'] = int(time.time())
        return super(ProjectDB, self).update(setting=obj, where={"pid": int(id)}, multi=False)

    def enable(self, id):
        return super(ProjectDB, self).update(setting={"status": self.PROJECT_STATUS_INIT},
                where={"pid": int(id)}, multi=False)

    def active(self, id):
        return super(ProjectDB, self).update(setting={"status": self.PROJECT_STATUS_ACTIVE},
                where={"pid": int(id)}, multi=False)

    def disable(self, id):
        return super(ProjectDB, self).update(setting={"status": self.PROJECT_STATUS_DISABLE},
                where={"pid": int(id)}, multi=False)

    def delete(self, id):
        return super(ProjectDB, self).update(setting={"status": self.PROJECT_STATUS_DELETED},
                where={"pid": int(id)}, multi=False)
                
    def get_count(self, where = {}, select = None, **kwargs):
        return self.count(where=where, select=select, **kwargs)

    def get_list(self, where = {}, select = None, **kwargs):
        kwargs.setdefault('sort', [('pid', 1)])
        return self.find(where=where, select=select, **kwargs)
