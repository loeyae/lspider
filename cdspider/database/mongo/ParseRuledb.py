# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-4 23:51:39
"""
import time
from cdspider.database.base import ParseRuleDB as BaseParseRuleDB
from .Mongo import Mongo


class ParseRuleDB(Mongo, BaseParseRuleDB):
    """
    parse_rule data object
    """

    __tablename__ = 'detail_rule'

    incr_key = 'detail_rule'

    def __init__(self, connector, table=None, **kwargs):
        super(ParseRuleDB, self).__init__(connector, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if 'uuid' not in indexes:
            collection.create_index('uuid', unique=True, name='uuid')
        if 'domain' not in indexes:
            collection.create_index('domain', name='domain')
        if 'subdomain' not in indexes:
            collection.create_index('subdomain', name='subdomain')
        if 'status' not in indexes:
            collection.create_index('status', name='status')
        if 'ctime' not in indexes:
            collection.create_index('ctime', name='ctime')

    def insert(self, obj):
        obj['uuid'] = self._get_increment(self.incr_key)
        obj.setdefault('status', self.STATUS_INIT)
        obj.setdefault('ctime', int(time.time()))
        obj.setdefault('utime', 0)
        _id = super(ParseRuleDB, self).insert(setting=obj)
        return obj['uuid']

    def update(self, id, obj={}):
        obj['utime'] = int(time.time())
        return super(ParseRuleDB, self).update(setting=obj, where={"uuid": int(id)}, multi=False)

    def active(self, id, where = {}):
        obj = {"utime": int(time.time()), "status": self.STATUS_ACTIVE}
        if not where:
            where = {'uuid': int(id)}
        else:
            where.update({'uuid': int(id)})
        return super(ParseRuleDB, self).update(setting=obj, where=where, multi=False)

    def enable(self, id, where = {}):
        obj = {"utime": int(time.time()), "status": self.STATUS_INIT}
        if not where:
            where = {'uuid': int(id)}
        else:
            where.update({'uuid': int(id)})
        return super(ParseRuleDB, self).update(setting=obj, where=where, multi=False)

    def disable(self, id, where = {}):
        obj = {"utime": int(time.time()), "status": self.STATUS_DISABLE}
        if not where:
            where = {'uuid': int(id)}
        else:
            where.update({'uuid': int(id)})
        return super(ParseRuleDB, self).update(setting=obj, where=where, multi=False)

    def delete(self, id, where={}):
        obj = {"utime": int(time.time()), "status": self.STATUS_DELETED}
        if not where:
            where = {'uuid': int(id)}
        else:
            where.update({'uuid': int(id)})
        return super(ParseRuleDB, self).update(setting=obj, where=where, multi=False)

    def get_detail(self, id):
        return self.get(where={"uuid": int(id)})

    def get_detail_by_domain(self, domain):
        where = {'domain': domain, 'subdomain': {"$in": ["", None]}}
        return self.get(where=where)

    def get_detail_by_subdomain(self, subdomain):
        where = {'subdomain': subdomain}
        return self.get(where=where)

    def get_list(self, where={}, select=None, **kwargs):
        return self.find(where=where, select=select, **kwargs)

    def get_list_by_domain(self, domain, where={}, select=None, **kwargs):
        if not where:
            where={}
        where.update({'domain': domain, 'subdomain': {"$in": ["", None]}})
        where.update({'status': self.STATUS_ACTIVE})
        return self.find(where=where, select=select, **kwargs)

    def get_list_by_subdomain(self, subdomain, where={}, select=None, **kwargs):
        if not where:
            where={}
        where.update({'subdomain': subdomain})
        where.update({'status': self.STATUS_ACTIVE})
        return self.find(where=where, select=select, **kwargs)
