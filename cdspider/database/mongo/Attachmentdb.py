#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-6-21 18:53:06
:version: SVN: $Id: Attachmentdb.py 2114 2018-07-04 03:56:01Z zhangyi $
"""
import time
import pymongo
from cdspider.database.base import AttachmentDB as BaseAttachmentDB
from .Mongo import Mongo

class AttachmentDB(Mongo, BaseAttachmentDB):
    """
    AttachmentDB
    """
    __tablename__ = 'attachment'

    def __init__(self, connector, table=None, **kwargs):
        super(AttachmentDB, self).__init__(connector, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if not 'aid' in indexes:
            collection.create_index('aid', unique=True, name='aid')
        if not 'title' in indexes:
            collection.create_index('title', name='title')
        if not 's_s' in indexes:
            collection.create_index([('sid', pymongo.ASCENDING),('status', pymongo.ASCENDING)], name='s_s')
        if not 'url' in indexes:
            collection.create_index([('url', pymongo.TEXT)], name='url')
        if not 'ctime' in indexes:
            collection.create_index('ctime', name='ctime')

    def insert(self, obj = {}):
        obj['aid'] = self._get_increment(self.table)
        obj.setdefault('status', self.STATUS_INIT)
        obj.setdefault('ctime', int(time.time()))
        obj.setdefault('utime', 0)
        _id = super(AttachmentDB, self).insert(setting=obj)
        return obj['aid']

    def update(self, id, obj = {}):
        obj['utime'] = int(time.time())
        return super(AttachmentDB, self).update(setting=obj, where={'aid': int(id)}, multi=False)

    def delete(self, id, where = {}):
        obj = {"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        if not where:
            where = {'aid': int(id)}
        else:
            where.update({'aid': int(id)})
        return super(AttachmentDB, self).update(setting=obj, where=where, multi=False)

    def delete_by_site(self, sid, where = {}):
        obj = {"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        if not where:
            where = {"sid": int(sid)}
        else:
            where.update({"sid": int(sid)})
        return super(AttachmentDB, self).update(setting=obj, where=where, multi=True)

    def delete_by_project(self, pid, where = {}):
        obj = {"status": self.STATUS_DELETED}
        obj['utime'] = int(time.time())
        if not where:
            where = {'pid': int(pid)}
        else:
            where.update({'pid': int(pid)})
        return super(AttachmentDB, self).update(setting=obj, where=where, multi=True)

    def active(self, id, where = {}):
        obj = {"status": self.STATUS_ACTIVE}
        obj['utime'] = int(time.time())
        if not where:
            where = {'aid': int(id)}
        else:
            where.update({'aid': int(id)})
        return super(AttachmentDB, self).update(setting=obj, where=where, multi=False)

    def disable(self, id, where = {}):
        obj = {"status": self.STATUS_DISABLE}
        obj['utime'] = int(time.time())
        if not where:
            where = {'aid': int(id)}
        else:
            where.update({'aid': int(id)})
        return super(AttachmentDB, self).update(setting=obj, where=where, multi=False)

    def get_detail(self, id):
        return self.get(where={'aid': int(id)})

    def get_list(self, where = {}, select=None, **kwargs):
        kwargs.setdefault('sort', [('aid', 1)])
        return self.find(where=where, select=select, **kwargs)
