# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 21:30:20
:version: SVN: $Id: Resultdb.py 2338 2018-07-08 05:58:24Z zhangyi $
"""
import time
from cdspider.database.base import ListRuleDB as BaseListRuleDB
from .Mongo import Mongo, SplitTableMixin


class ListRuleDB(Mongo, BaseListRuleDB, SplitTableMixin):

    __tablename__ = 'listRule'

    incr_key = 'listRule'

    def __init__(self, connector, table=None, **kwargs):
        super(ListRuleDB, self).__init__(connector, table = table, **kwargs)
        self._create_collection(self.table)

    def insert(self, obj={}):
        obj.setdefault("type", self.RULE_TYPE_DEFAULT)
        obj.setdefault("preid", 0)
        obj.setdefault("ctime", int(time.time()))
        obj.setdefault("status", self.STATUS_INIT)
        obj['uuid'] = self._get_increment(self.incr_key)
        super(ListRuleDB, self).insert(setting=obj)
        return obj['uuid']

    def update(self, id, obj = {}):
        obj['utime'] = int(time.time())
        return super(ListRuleDB, self).update(setting=obj, where={"uuid": int(id)})

    def active(self, id, where = {}):
        obj = {"utime": int(time.time()), "status": self.STATUS_ACTIVE}
        if not where:
            where = {'uuid': int(id)}
        else:
            where.update({'uuid': int(id)})
        return super(ListRuleDB, self).update(setting=obj, where=where, multi=False)

    def enable(self, id, where = {}):
        obj = {"utime": int(time.time()), "status": self.STATUS_INIT}
        if not where:
            where = {'uuid': int(id)}
        else:
            where.update({'uuid': int(id)})
        return super(ListRuleDB, self).update(setting=obj, where=where, multi=False)

    def disable(self, id, where = {}):
        obj = {"utime": int(time.time()), "status": self.STATUS_DISABLE}
        if not where:
            where = {'uuid': int(id)}
        else:
            where.update({'uuid': int(id)})
        return super(ListRuleDB, self).update(setting=obj, where=where, multi=False)

    def delete(self, id, where={}):
        obj = {"utime": int(time.time()), "status": self.STATUS_DELETED}
        if not where:
            where = {'uuid': int(id)}
        else:
            where.update({'uuid': int(id)})
        return super(ListRuleDB, self).update(setting=obj, where=where, multi=False)

    def get_detail(self, id, select=None):
        return self.get(where={"uuid": int(id)}, select=select)

    def get_list(self, where={}, select=None, **kwargs):
        kwargs.setdefault('sort', [('uuid', 1)])
        return self.find(where=where, select=select, **kwargs)

    def get_count(self, where={}, select=None, **kwargs):
        return self.count(where=where, select=select, **kwargs)

    def _create_collection(self, table):
        collection = self._db.get_collection(table)
        indexes = collection.index_information()
        if 'uuid' not in indexes:
            collection.create_index('uuid', unique=True, name='uuid')
        if 'tid' not in indexes:
            collection.create_index('tid', name='tid')
        if 'type' not in indexes:
            collection.create_index('type', name='type')
        if 'status' not in indexes:
            collection.create_index('status', name='status')
        if 'ctime' not in indexes:
            collection.create_index('ctime', name='ctime')
