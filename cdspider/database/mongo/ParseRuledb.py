#-*- coding: utf-8 -*-

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

    __tablename__ = 'parse_rules'

    incr_key = 'parseRule'

    def __init__(self, connector, table=None, **kwargs):
        super(ParseRuleDB, self).__init__(connector, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if not 'prid' in indexes:
            collection.create_index('prid', unique=True, name='prid')
        if not 'domain' in indexes:
            collection.create_index('domain', name='domain')
        if not 'subdomain' in indexes:
            collection.create_index('subdomain', name='subdomain')
        if not 'status' in indexes:
            collection.create_index('status', name='status')
        if not 'ctime' in indexes:
            collection.create_index('ctime', name='ctime')

    def insert(self, obj):
        obj['prid'] = self._get_increment(self.incr_key)
        obj.setdefault('status', self.STATUS_INIT)
        obj.setdefault('ctime', int(time.time()))
        obj.setdefault('utime', 0)
        _id = super(KeywordsDB, self).insert(setting=obj)
        return obj['kwid']

    def update(self, id, obj = {}):
        obj['utime'] = int(time.time())
        return super(KeywordsDB, self).update(setting=obj, where={"prid": int(id)}, multi=False)

    def delete(self, id, wherer = {}):
        if not where:
            where = {'prid': int(id)}
        else:
            where.update({'prid': int(id)})
        return super(KeywordsDB, self).update(setting={"status": self.STATUS_DELETED},
                where=where, multi=False)

    def get_detail(self, id):
        return self.get(where={"prid": int(id)})

    def get_detail_by_domain(self, domain):
        where = {'domain': domain}
        return self.get(where=where)

    def get_detail_by_subdomain(self, subdomain):
        where = {'subdomain': subdomain}
        return self.get(where=where)

    def get_list(self, where = {}, select = None):
        return self.find(where=where, select=select)
