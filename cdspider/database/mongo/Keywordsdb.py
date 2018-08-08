#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 19:49:34
:version: SVN: $Id: KeywordsDB.py 2114 2018-07-04 03:56:01Z zhangyi $
"""
import time
from cdspider.database.base import KeywordsDB as BaseKeywordsDB
from .Mongo import Mongo

class KeywordsDB(Mongo, BaseKeywordsDB):

    __tablename__ = 'keywords'

    def __init__(self, connector, table=None, **kwargs):
        super(KeywordsDB, self).__init__(connector, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if not 'kwid' in indexes:
            collection.create_index('kwid', unique=True, name='kwid')
        if not 'word' in indexes:
            collection.create_index('word', unique=True, name='word')
        if not 'status' in indexes:
            collection.create_index('status', name='status')
        if not 'ctime' in indexes:
            collection.create_index('ctime', name='ctime')

    def insert(self, obj):
        obj['kwid'] = self._get_increment(self.table)
        obj.setdefault('status', self.STATUS_INIT)
        obj.setdefault('ctime', int(time.time()))
        obj.setdefault('utime', 0)
        _id = super(KeywordsDB, self).insert(setting=obj)
        return obj['kwid']

    def update(self, id, obj):
        obj['utime'] = int(time.time())
        return super(KeywordsDB, self).update(setting=obj, where={"kwid": int(id)}, multi=False)

    def update_many(self,obj, where=None):
        if where==None or where=={}:
            return
        obj['utime'] = int(time.time())
        return super(KeywordsDB, self).update(setting=obj, where=where, multi=False)

    def active(self, id, where = {}):
        if not where:
            where = {'kwid': int(id)}
        else:
            where.update({'kwid': int(id)})
        return super(KeywordsDB, self).update(setting={"status": self.STATUS_ACTIVE},
                where=where, multi=False)

    def disable(self, id, where = {}):
        if not where:
            where = {'kwid': int(id)}
        else:
            where.update({'kwid': int(id)})
        return super(KeywordsDB, self).update(setting={"status": self.STATUS_INIT},
                where=where, multi=False)

    def delete(self, id, where = {}):
        if not where:
            where = {'kwid': int(id)}
        else:
            where.update({'kwid': int(id)})
        return super(KeywordsDB, self).update(setting={"status": self.STATUS_DELETED},
                where=where, multi=False)

    def get_detail(self, id):
        return self.get(where={"kwid": int(id)})

    def get_new_list(self, id, select=None, **kwargs):
        kwargs.setdefault('sort', [('kid', 1)])
        return self.find(where={"kid": {"$gt": int(id)}},
            select=select, **kwargs)

    def get_list(self, where = {}, select=None, **kwargs):
        kwargs.setdefault('sort', [('kwid', 1)])
        return self.find(where=where, select=select, **kwargs)
