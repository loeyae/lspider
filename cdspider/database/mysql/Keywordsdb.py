#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 19:49:34
:version: SVN: $Id: Keywordsdb.py 2117 2018-07-04 03:56:17Z zhangyi $
"""
import time
from cdspider.database.base import KeywordsDB as BaseKeywordsDB
from .Mysql import Mysql

class KeywordsDB(Mysql, BaseKeywordsDB):

    __tablename__ = 'keywords'

    def __init__(self, host='localhost', port=27017, db = None, user=None,
                password=None, table=None, **kwargs):
        super(KeywordsDB, self).__init__(host = host, port = port, db = db,
            user = user, password = password, table = table, **kwargs)

        self._init_table()

    def _init_table(self):
        if not self.have_table(self.table):
            sql = """
            CREATE TABLE `{table}` (
                `kid` bigint UNSIGNED NOT NULL AUTO_INCREMENT COMMENT "keywords id",
                `word` varchar(32) NOT NULL COMMENT "keyword",
                `status` tinyint(1) UNSIGNED NOT NULL DEFAULT '0' COMMENT "status",
                `src_txt` varchar(16) NULL DEFAULT NULL COMMENT "关键词来源",
                `createtime` int(10) UNSIGNED NOT NULL COMMENT "创建时间",
                `updatetime` int(10)  UNSIGNED NOT NULL DEFAULT '0' COMMENT "最后一次更新时间",
                `creator` varchar(16) NULL DEFAULT NULL COMMENT "创建人",
                `updator` varchar(16) NULL DEFAULT NULL COMMENT "最后一次更新的人",
                PRIMARY KEY (`kid`),
                UNIQUE (`word`),
                INDEX (`status`),
                INDEX (`createtime`)
            ) ENGINE = MyISAM CHARSET=utf8 COLLATE utf8_general_ci COMMENT = '关键词'
            """
            sql = sql.format(table=self.table)
            self._execute(sql)

    def insert(self, obj={}):
        obj.setdefault('status', self.KEYWORDS_STATUS_INIT)
        obj.setdefault('createtime', int(time.time()))
        obj.setdefault('updatetime', 0)
        return super(KeywordsDB, self).insert(setting=obj)

    def update(self, id, obj):
        obj['updatetime'] = int(time.time())
        return super(KeywordsDB, self).update(setting=obj, where={"kid": id}, multi=False)

    def enable(self, id, where = {}):
        if not where:
            where = {'kid': int(id)}
        else:
            where.update({'kid': int(id)})
        return super(KeywordsDB, self).update(setting={"status": self.KEYWORDS_STATUS_INIT},
                where=where, multi=False)

    def active(self, id, where = {}):
        if not where:
            where = {'kid': int(id)}
        else:
            where.update({'kid': int(id)})
        return super(KeywordsDB, self).update(setting={"status": self.KEYWORDS_STATUS_ACTIVE},
                where=where, multi=False)

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
        return self.get(where={"kid": id})

    def get_new_list(self, id, select=None, **kwargs):
        return self.find(where=[("kid", ">", id), ("status", BaseKeywordsDB.KEYWORDS_STATUS_ACTIVE)],
            select=select, sort=[('kid', 1)], **kwargs)

    def get_list(self, where = {}, select=None,sort=[('kid', 0)], **kwargs):
        return self.find(where=where, sort=sort, select=select, **kwargs)

    def get_max_id(self):
        data = self.get(select="MAX(`kid`) AS maxid", where=None)
        return data['maxid']
