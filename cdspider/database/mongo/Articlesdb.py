#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 21:30:20
:version: SVN: $Id: Resultdb.py 2338 2018-07-08 05:58:24Z zhangyi $
"""
import time
import datetime
from cdspider.database.base import ArticlesDB as BaseArticlesDB
from .Mongo import Mongo, SplitTableMixin

class ArticlesDB(Mongo, BaseArticlesDB, SplitTableMixin):

    __tablename__ = 'articles'

    def __init__(self, connector, table=None, **kwargs):
        super(ArticlesDB, self).__init__(connector, table = table, **kwargs)
        self._check_collection()

    def insert(self, obj = {}):
        obj.setdefault("createtime", int(time.time()))
        table = self._get_collection(obj['createtime'])
        id = self._get_increment(table)
        obj['rid'] = BaseArticlesDB.build_id(obj['createtime'], id)
        obj.setdefault('status', self.RESULT_STATUS_INIT)
        obj.setdefault('urlid', 0)
        obj.setdefault('atid', 0)
        obj.setdefault('kwid', 0)
        super(ArticlesDB, self).insert(setting=obj, table=table)
        return obj['rid']

    def update(self, id, obj = {}):
        table = self._table_name(id)
        obj['updatetime'] = int(time.time())
        return super(ArticlesDB, self).update(setting=obj, where={"rid": id}, table=table)

    def add_crwal_info(self, unid, createtime, crawlinfo):
        obj={}
        table = self._get_collection(createtime)
        obj['updatetime'] = int(time.time())
        data = self.get(where={"unid": unid}, select={'rid': True, 'crawlinfo': True}, table=table)
        if not data:
            return False
        need_update = True
        _, cv = list(crawlinfo.items())[0]
        for item in data['crawlinfo'].values():
            if cv['task'] == item['task']:
                need_update = False
                break
        if need_update:
            data['crawlinfo'].update(crawlinfo)
            obj['crawlinfo'] = data['crawlinfo']
            super(ArticlesDB, self).update(setting=obj, where={"rid": data['rid']}, table=table)
        return data['rid']

    def get_detail(self, id):
        table = self._table_name(id)
        return self.get(where={"rid": id}, table=table)

    def get_detail_by_unid(self, unid, createtime):
        table = self._get_collection(createtime)
        return self.get(where = {"unid", unid}, table=table)

    def get_list(self, createtime, where = {}, select = None, **kwargs):
        table = self._get_collection(createtime)
        kwargs.setdefault('sort', [('createtime', 1)])
        return self.find(table=table, where=where, select=select, **kwargs)

    def get_count(self, createtime, where = {}, select = None, **kwargs):
        table = self._get_collection(createtime)
        return self.count(table=table, where=where, select=select, **kwargs)

    def aggregate_by_day(self, createtime, where={}):
        table = self._get_collection(createtime)
        pipeline = [
            {
                '$group': {
                    '_id': {
                        '$dateToString': {
                            'format': '%d',
                            'date': {
                                '$add': [
                                    datetime.datetime(1970,1,1),
                                    {"$multiply": ["$createtime", 1000]},
                                ]
                            }
                        }
                    },
                    'count': {'$sum': 1}
                }
            },
            {
                "$sort": {"_id": 1}
            }
        ]
        if where:
            pipeline.insert(0, {"$match": self._build_where(where)})
        courser = self.aggregate(pipeline=pipeline, table=table)
        for each in courser:
            each['day'] = each['_id']
            del each['_id']
            yield each

    def _get_collection(self, createtime):
        suffix = time.strftime("%Y%m", time.localtime(createtime))
        name = super(ArticlesDB, self)._collection_name(suffix)
        if not name in self._collections:
            self._create_collection(name)
        return name

    def _table_name(self, id):
        suffix, _ = BaseArticlesDB.unbuild_id(id)
        name = super(ArticlesDB, self)._collection_name(suffix)
        if not name in self._collections:
            self._create_collection(name)
        return name

    def _check_collection(self):
        self._list_collection()
        suffix = time.strftime("%Y%m")
        name = super(ArticlesDB, self)._collection_name(suffix)
        if not name in self._collections:
            self._create_collection(name)

    def _create_collection(self, table):
        collection = self._db.get_collection(table)
        indexes = collection.index_information()
        if not 'rid' in indexes:
            collection.create_index('rid', unique=True, name='rid')
        if not 'unid' in indexes:
            collection.create_index('unid', unique=True, name='unid')
        if not 'p_s_u' in indexes:
            collection.create_index([('pid', 1), ('siteid', 1), ('urlid', 1)], name='p_s_u')
        if not 'p_s_a' in indexes:
            collection.create_index([('projectid', 1), ('siteid', 1), ('atid', 1)], name='p_s_a')
        if not 'p_s_k' in indexes:
            collection.create_index([('projectid', 1), ('siteid', 1), ('kwid', 1)], name='p_s_k')
        if not 'parentid' in indexes:
            collection.create_index('parentid', name='parentid')
        if not 'createtime' in indexes:
            collection.create_index('createtime', name='createtime')
        self._collections.add(table)
