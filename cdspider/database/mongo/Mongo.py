# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:46:37
:version: SVN: $Id: Mongo.py 2303 2018-07-06 08:56:57Z zhangyi $
"""

from cdspider.database import BaseDataBase
from cdspider.exceptions import *


class Mongo(BaseDataBase):
    """
    Mongo数据库操作类
    """

    _operator = {
        "and": "$and",
        "or": "$or",
        "=": "$eq",
        ">": "$gt",
        ">=": "$gte",
        "IN": "$in",
        "<": "$lt",
        "<=": "$lte",
        "<>": "$ne",
        "NOT IN": "$nin",
        "$regex": "$regex",
    }

    def __init__(self, connector, table=None, **kwargs):
        if table is None:
            table = self.__tablename__
        super(Mongo, self).__init__(connector, table = table, **kwargs)
        self._db = self.conn.cursor

    def collection(self, table):
        self.table = table
        return self

    def find(self, where, table=None, select=None, sort=None, offset=0, hits=10):
        """
        多行查询
        """
        collection = self._db.get_collection(table or self.table)
        where = self._build_where(where)
        projection = self._build_projection(select)
        self.logger.debug('find %s from %s where %s order %s limit %s, %s' % (projection, table or self.table, where, sort, offset, hits))
        cursor = collection.find(filter=where, projection=projection, sort = sort, skip = offset, limit = hits)
        for each in cursor:
            if each and '_id' in each and (select and '_id' not in select or not select):
                del each['_id']
            yield each

    def get(self, where, table=None, select=None, sort=None):
        """
        单行查询
        """
        collection = self._db.get_collection(table or self.table)
        where = self._build_where(where)
        projection = self._build_projection(select)
        self.logger.debug('find %s from %s where %s order %s' % (projection, table or self.table, where, sort))
        doc = collection.find_one(filter=where, projection=projection, sort = sort)
        if doc and '_id' in doc and (select and '_id' not in select or not select):
            del doc['_id']
        return doc

    def insert(self, setting, table=None):
        """
        插入数据
        """
        self.logger.debug('insert %s into %s' % (setting, table or self.table))
        collection = self._db.get_collection(table or self.table)
        cursor = collection.insert_one(document=setting)
        return cursor.inserted_id

    def update(self, setting, where, table=None, multi=False, upsert=False):
        """
        修改数据
        """
        collection = self._db.get_collection(table or self.table)
        where = self._build_where(where)
        self.logger.debug('update %s to %s with %s by multi %s' % (table or self.table, setting, where, multi))
        if multi:
            cursor = collection.update_many(filter = where, update = {"$set": setting}, upsert = upsert)
        else:
            cursor = collection.update_one(filter = where, update = {"$set": setting}, upsert = upsert)
        return cursor.modified_count

    def delete(self, where, table=None, multi=False):
        """
        删除数据
        """
        collection = self._db.get_collection(table or self.table)
        where = self._build_where(where)
        self.logger.debug('delete from %s with %s by multi %s' % (table or self.table, where, multi))
        if multi:
            cursor = collection.delete_many(filter=where)
        else:
            cursor = collection.delete_one(filter=where)
        return cursor.deleted_count

    def count(self, where, select=None, table=None):
        """
        count
        """
        collection = self._db.get_collection(table or self.table)
        projection = self._build_projection(select)
        where = self._build_where(where)
        self.logger.debug('find %s from %s where %s' % (projection, table or self.table, where))
        cursor = collection.find(filter=where, projection=projection)
        return cursor.count()

    def aggregate(self, pipeline, table=None):
        """
        aggregate
        """
        collection = self._db.get_collection(table or self.table)
        self.logger.debug('aggregate %s from %s' % (pipeline, table or self.table))
        return collection.aggregate(pipeline)

    def _build_projection(self, select):
        if isinstance(select, list) or isinstance(select, tuple) or select is None:
            return dict.fromkeys(select, 1) if select else None
        elif isinstance(select, dict):
            return select
        raise CDSpiderDBError("Projection setting error: %s" % select)

    def get_operator(self, p):
        return self._operator.get(p, p)

    def is_operator(self, p):
        if p in self._operator or p in self._operator.values():
            return True
        return False

    def _build_where(self, where):
        if not where:
            return {}
        if isinstance(where, tuple):
            if len(where) > 3:
                return {'$and': [self._build_where(item) for item in where]}
            elif len(where) > 2:
                if self.is_operator(where[1]):
                    return {where[0]: {self.get_operator(where[1]): where[2]}}
                return {'$and': [self._build_where(item) for item in where]}
            elif len(where) > 1:
                return {where[0]: where[1]}
        elif isinstance(where, list):
            return {'$and': [self._build_where(item) for item in where]}
        elif isinstance(where, dict):
            cw={}
            for k, v in where.items():
                k = self.get_operator(k)
                if k.lower() in ['$and', '$or']:
                    return {k: [self._build_where(item) for item in v]}
                elif isinstance(v, (tuple, list)):
                    cw.update({k: {'$in': v}})
                else:
                    cw.update({k: v})
            return cw
        return {}

    def _get_increment(self, name):
        collection = self._db.get_collection('inc_ids')
        indexes = collection.index_information()
        if 'name' not in indexes:
            collection.create_index('name', unique=True, name='name')
        res = collection.find_one_and_update({"name": name}, {"$inc": {"id": 1}}, upsert=True, return_document=True)
        return res['id']


class SplitTableMixin(object):

    __tablename__ = None

    _db = None

    def _collection_name(self, suffix):
        if not suffix:
            raise NotImplementedError
        return "%s.%s" % (self.__tablename__, suffix)

    def _list_collection(self):
        self._collections = set()
        prefix = "%s." % self.__tablename__
        for each in self._db.collection_names():
            if each.startswith('system.'):
                continue
            if each.startswith(prefix):
                self._collections.add(each)
