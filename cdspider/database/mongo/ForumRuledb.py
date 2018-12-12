#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-12 20:51:39
"""
import time
from cdspider.database.base import ForumRuleDB as BaseForumRuleDB
from .Mongo import Mongo

class ForumRuleDB(Mongo, BaseForumRuleDB):
    """
    forumRule data object
    """

    __tablename__ = 'forumRule'

    incr_key = 'forumRule'

    def __init__(self, connector, table=None, **kwargs):
        super(ForumRuleDB, self).__init__(connector, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if not 'uuid' in indexes:
            collection.create_index('uuid', unique=True, name='uuid')
        if not 'domain' in indexes:
            collection.create_index('domain', name='domain')
        if not 'subdomain' in indexes:
            collection.create_index('subdomain', name='subdomain')
        if not 'status' in indexes:
            collection.create_index('status', name='status')
        if not 'ctime' in indexes:
            collection.create_index('ctime', name='ctime')

    def insert(self, obj):
        obj['uuid'] = self._get_increment(self.incr_key)
        obj.setdefault('status', self.STATUS_INIT)
        obj.setdefault('ctime', int(time.time()))
        obj.setdefault('utime', 0)
        _id = super(ForumRuleDB, self).insert(setting=obj)
        return obj['kwid']

    def update(self, id, obj = {}):
        obj['utime'] = int(time.time())
        return super(ForumRuleDB, self).update(setting=obj, where={"uuid": int(id)}, multi=False)

    def delete(self, id, wherer = {}):
        if not where:
            where = {'uuid': int(id)}
        else:
            where.update({'uuid': int(id)})
        return super(ForumRuleDB, self).update(setting={"status": self.STATUS_DELETED},
                where=where, multi=False)

    def get_detail(self, id):
        return self.get(where={"uuid": int(id)})

    def get_detail_by_domain(self, domain):
        where = {'domain': domain, 'subdomain': {"$in": ["", None]}}
        return self.get(where=where)

    def get_detail_by_subdomain(self, subdomain):
        where = {'subdomain': subdomain}
        return self.get(where=where)

    def get_list(self, where = {}, select = None):
        return self.find(where=where, select=select)

    def get_list_by_domain(self, domain, where = {}, select = None):
        if not where:
            where = {}
        where.update({'domain': domain, 'subdomain': {"$in": ["", None]}})
        return self.find(where=where, select=select)

    def get_list_by_subdomain(self, subdomain, where = {}, select = None):
        if not where:
            where = {}
        where.update({'subdomain': subdomain})
        return self.find(where=where, select=select)
