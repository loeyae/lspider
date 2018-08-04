#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:48:24
:version: SVN: $Id: Basedb.py 1465 2018-06-23 04:55:26Z zhangyi $
"""

from __future__ import unicode_literals, division, absolute_import
import json
import six
import logging

class BaseDB:
    '''
    BaseDB
    '''
    __tablename__ = None
    placeholder = '%s'
    maxlimit = -1
    _operator = {
        "$and": "and",
        "$or": "or",
        "$eq": "=",
        "$gt": ">",
        "$gte": ">=",
        "$in": "IN",
        "$lt": "<",
        "$lte": "<=",
        "$ne": "<>",
        "$nin": "NOT IN",
    }

    @staticmethod
    def escape(string):
        return '`%s`' % string

    @property
    def cursor(self):
        raise NotImplementedError

    @property
    def archive_fields(self):
        raise NotImplementedError

    def archive_unzip(self, row):
        if self.archive_fields:
            for item in row:
                if item in self.archive_fields and row[item]:
                    row[item] = json.loads(row[item])
        return row

    def archive_zip(self, row):
        if self.archive_fields:
            for item in row:
                if item in self.archive_fields and row[item]:
                    row[item] = json.dumps(row[item])
        return row

    def _execute(self, sql_query, values=[]):
        cursor = self.cursor
        cursor.execute(sql_query, values)
        return cursor

    def _select(self, where, table = None, select = None, group=None, having=None, sort = None, offset = 0, hits = 10):
        tablename = self.escape(table or self.__tablename__)
        if isinstance(select, list) or isinstance(select, tuple) or isinstance(select, dict) or select is None:
            select = ','.join(self.escape(f) for f in select) if select else '*'

        sql_query = "SELECT %s FROM %s" % (select, tablename)
        where_values = []
        if where:
            sql_query += " WHERE %s" % self._build_where(where, where_values)
        if group:
            sql_query += " GROUP BY %s" % self._build_group(group)
        if having:
            sql_query += " HAVING %s" % self._build_where(having, where_values)
        if sort:
            sql_query += ' ORDER BY %s' % self._build_sort(sort)
        if hits:
            sql_query += " LIMIT %d, %d" % (offset, hits)
        self.logger.debug("Sql: %s, params: %s" % (sql_query, where_values))

        for row in self._execute(sql_query, where_values):
            yield row

    def _select2dic(self, where, table = None, select = None, group=None, having=None, sort = None, offset = 0, hits = 10):
        tablename = self.escape(table or self.__tablename__)
        if isinstance(select, list) or isinstance(select, tuple) or isinstance(select, dict) or select is None:
            select = ','.join(self.escape(f) for f in select) if select else '*'

        sql_query = "SELECT %s FROM %s" % (select, tablename)
        where_values = []
        if where:
            sql_query += " WHERE %s" % self._build_where(where, where_values)
        if group:
            sql_query += " GROUP BY %s" % self._build_group(group)
        if having:
            sql_query += " HAVING %s" % self._build_where(having, where_values)
        if sort:
            sql_query += ' ORDER BY %s' % self._build_sort(sort)
        if hits:
            sql_query += " LIMIT %d, %d" % (offset, hits)
        self.logger.debug("Sql: %s, params: %s" % (sql_query, where_values))

        cursor = self._execute(sql_query, where_values)
        fields = [f[0] for f in cursor.description]

        for row in cursor:
            yield dict(zip(fields, row))

    def _replace(self, setting, table = None):
        tablename = self.escape(table or self.__tablename__)
        if isinstance(setting, dict):
            _keys = ", ".join(self.escape(k) for k in values)
            _values = ", ".join([self.placeholder, ] * len(values))
            sql_query = "REPLACE INTO %s (%s) VALUES (%s)" % (tablename, _keys, _values)
        else:
            raise CDSpiderDBError("Fields setting error: %s" % setting)
        self.logger.debug("Sql: %s, params: %s" % (sql_query, setting))

        cursor = self._execute(sql_query, list(six.itervalues(setting)))
        return cursor.lastrowid

    def _insert(self, setting, table = None):
        tablename = self.escape(table or self.__tablename__)
        if isinstance(setting, dict):
            _keys = ", ".join(self.escape(k) for k in setting)
            _values = ", ".join([self.placeholder, ] * len(setting))
            sql_query = "INSERT INTO %s (%s) VALUES (%s)" % (tablename, _keys, _values)
        else:
            raise CDSpiderDBError("Fields setting error: %s" % setting)
        self.logger.debug("Sql: %s, params: %s" % (sql_query, setting))

        cursor = self._execute(sql_query, list(six.itervalues(setting)))
        if cursor.lastrowid:
            return cursor.lastrowid
        return cursor.rowcount

    def _update(self, setting, where, table = None, multi = False):
        tablename = self.escape(table or self.__tablename__)
        _key_values = ", ".join([
            "%s = %s" % (self.escape(k), self.placeholder) for k in setting
        ])
        where_values = []
        sql_query = "UPDATE %s SET %s WHERE %s" % (tablename, _key_values, self._build_where(where, where_values))
        if not multi:
            sql_query += ' LIMIT 1'
        self.logger.debug("Sql: %s, params: %s" % (sql_query, list(six.itervalues(setting)) + where_values))

        cursor = self._execute(sql_query, list(six.itervalues(setting)) + list(where_values))
        return cursor.rowcount

    def _delete(self, where, table = None, multi = False):
        tablename = self.escape(table or self.__tablename__)
        sql_query = "DELETE FROM %s" % tablename
        where_values = []
        sql_query += " WHERE %s" % self._build_where(where, where_values)
        if not multi:
            sql_query += " LIMIT 1"
        self.logger.debug("Sql: %s, params: %s" % (sql_query, where_values))

        cursor = self._execute(sql_query, where_values)
        return cursor.rowcount

    def _build_sort(self, sort):
        if isinstance(sort, (list, tuple)):
            return ','.join("%s %s" % (self.escape(f[0]), "ASC" if f[1] == 1 else "DESC") for f in sort if (isinstance(f, list) or isinstance(f, tuple)))
        return None

    def _build_group(self, group):
        if isinstance(group, (list, tuple)):
            return ', '.join(self.escape(f) for f in group)
        return self.escape(group)

    def get_operator(self, p):
        return self._operator.get(p, p)

    def is_operator(self, p):
        if p in self._operator or p in self._operator.values():
            return True
        return False

    def _build_where(self, where, params = []):
        if not where:
            return '1'
        if isinstance(where, six.string_types):
            return where
        elif isinstance(where, tuple):
            if len(where) > 3:
                cw = []
                for each in where:
                    cw.append(self._build_where(each, params))
                return ' AND '.join(cw)
            if len(where) > 2:
                if not self.is_operator(where[1]):
                    cw = []
                    for each in where:
                        cw.append(self._build_where(each, params))
                    return ' AND '.join(cw)
                if isinstance(where[2], (list, tuple)):
                    swhere = []
                    for i in where[2]:
                        swhere.append(self.placeholder)
                        params.append(i)
                    s = '(' + ','.join(swhere) +')'
                else:
                    s = self.placeholder
                    params.append(where[2])
                return "%s %s %s" % (where[0], self.get_operator(where[1]), s)
            elif len(where) > 1:
                return self._build_where({where[0]: where[1]}, params)
            else:
                raise NotImplementedError
        elif isinstance(where, list):
            cw = []
            for each in where:
                cw.append(self._build_where(each, params))
            return ' AND '.join(cw)
        elif isinstance(where, dict):
            cw = []
            for k, v in where.items():
                k = self.get_operator(k)
                if k.upper() in ('AND', 'OR'):
                    if isinstance(v, (list, tuple)):
                        p = " %s " % k.upper()
                        cwl = []
                        for each in v:
                            cwl.append(self._build_where(each, params))
                        cw.append(p.join(cwl))
                    else:
                        raise NotImplementedError
                elif isinstance(v, (list, tuple)):
                    s = self._build_multi_values(v, params)
                    cw.append('%s IN %s' % (k, s))
                elif isinstance(v, dict):
                    s = []
                    for p, val in v.items():
                        if isinstance(val, (list, tuple)):
                            placeholder = self._build_multi_values(val, params)
                        else:
                            placeholder = self.placeholder
                            params.append(val)
                        s.append("%s %s %s" % (k, self.get_operator(p), placeholder))
                    cw.append(' AND '.join(s))
                else:
                    params.append(v)
                    cw.append('%s = %s' % (k, self.placeholder))
            return ' AND '.join(cw)
        else:
            raise NotImplementedError

    def _build_multi_values(self, values, params):
        s = [self.placeholder for i in range(len(values))]
        params.extend(values)
        return "(%s)" % ",".join(s)
