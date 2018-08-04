#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 19:49:34
:version: SVN: $Id: Keywordsdb.py 2114 2018-07-04 03:56:01Z zhangyi $
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
        if not 'kid' in indexes:
            collection.create_index('kid', unique=True, name='kid')
        if not 'word' in indexes:
            collection.create_index('word', unique=True, name='word')
        if not 'status' in indexes:
            collection.create_index('status', name='status')
        if not 'ctime' in indexes:
            collection.create_index('ctime', name='ctime')

    def insert(self, obj):
        obj['kid'] = self._get_increment(self.table)
        obj.setdefault('status', self.KEYWORDS_STATUS_INIT)
        obj.setdefault('ctime', int(time.time()))
        obj.setdefault('utime', 0)
        _id = super(KeywordsDB, self).insert(setting=obj)
        return obj['kid']

    def update(self, id, obj):
        obj['utime'] = int(time.time())
        return super(KeywordsDB, self).update(setting=obj, where={"kid": int(id)}, multi=False)

    def active(self, id, where = {}):
        if not where:
            where = {'kid': int(id)}
        else:
            where.update({'kid': int(id)})
        return super(KeywordsDB, self).update(setting={"status": self.KEYWORDS_STATUS_ACTIVE},
                where=where, multi=False)

    def disable(self, id, where = {}):
        if not where:
            where = {'kid': int(id)}
        else:
            where.update({'kid': int(id)})
        return super(KeywordsDB, self).update(setting={"status": self.KEYWORDS_STATUS_DISABLE},
                where=where, multi=False)

    def delete(self, id, where = {}):
        if not where:
            where = {'kid': int(id)}
        else:
            where.update({'kid': int(id)})
        return super(KeywordsDB, self).update(setting={"status": self.KEYWORDS_STATUS_DELETED},
                where=where, multi=False)

    def get_detail(self, id):
        return self.get(where={"kid": int(id)})

    def get_list(self, where = {}, select=None, **kwargs):
        kwargs.setdefault('sort', [('kid', 1)])
        return self.find(where=where, select=select, **kwargs)
