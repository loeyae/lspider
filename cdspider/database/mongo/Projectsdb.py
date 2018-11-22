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
from cdspider.database.base import ProjectsDB as BaseProjectsDB
from .Mongo import Mongo

class ProjectsDB(Mongo, BaseProjectsDB):

    __tablename__ = 'project'

    incr_key = 'project'

    def __init__(self, connector, table=None, **kwargs):
        super(ProjectsDB, self).__init__(connector, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if not 'uuid' in indexes:
            collection.create_index('uuid', unique=True, name='uuid')
        if not 'status' in indexes:
            collection.create_index('status', name='status')
        if not 'ctime' in indexes:
            collection.create_index('ctime', name='ctime')

    def get_detail(self, id):
        return self.get(where={'uuid': int(id)})

    def insert(self, obj):
        obj['uuid'] = self._get_increment(self.incr_key)
        obj.setdefault('status', self.STATUS_INIT)
        obj.setdefault('ctime', int(time.time()))
        obj.setdefault('utime', 0)
        super(ProjectsDB, self).insert(setting=obj)
        return obj['uuid']

    def update(self, id, obj):
        obj['utime'] = int(time.time())
        return super(ProjectsDB, self).update(setting=obj, where={"uuid": int(id)}, multi=False)

    def active(self, id):
        return super(ProjectsDB, self).update(setting={"status": self.STATUS_ACTIVE},
                where={"uuid": int(id)}, multi=False)

    def disable(self, id):
        return super(ProjectsDB, self).update(setting={"status": self.STATUS_INIT},
                where={"uuid": int(id)}, multi=False)

    def delete(self, id):
        return super(ProjectsDB, self).update(setting={"status": self.STATUS_DELETED},
                where={"uuid": int(id)}, multi=False)

    def get_count(self, where = {}, select = None, **kwargs):
        return self.count(where=where, select=select, **kwargs)

    def get_list(self, where = {}, select = None, **kwargs):
        kwargs.setdefault('sort', [('uuid', 1)])
        return self.find(where=where, select=select, **kwargs)

    def get_new_list(self, id, where = {}, select = None, **kwargs):
        kwargs.setdefault('sort', [('uuid', 1)])
        if not where:
            where == {}
        where['uuid'] = {"$gt": id}
        return self.find(where=where, select=select, **kwargs)
