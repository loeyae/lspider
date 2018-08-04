#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-13 12:48:16
:version: SVN: $Id: Sitetypedb.py 990 2018-06-07 10:12:02Z zhangyi $
"""
import time
from cdspider.database.base import SitetypeDB
from .Mysql import Mysql

class SitetypeDB(Mysql, SitetypeDB):
    """
    站点类型DAO
    """
    __tablename__ = "sitetype"

    def __init__(self, host='localhost', port=27017, db = None, user=None,
                password=None, table=None, **kwargs):
        super(SitetypeDB, self).__init__(host = host, port = port, db = db,
            user = user, password = password, table = table, **kwargs)
        self._init_table()

    def _init_table(self):
        if not self.have_table(self.table):
            sql = """
            CREATE TABLE `{table}` (
                `stid` int UNSIGNED NOT NULL AUTO_INCREMENT COMMENT "site type id",
                `type` varchar(16) NOT NULL COMMENT "网站类型",
                `status` tinyint(1) UNSIGNED NOT NULL DEFAULT '0' COMMENT "状态",
                `domain` varchar(64) NOT NULL COMMENT "站点域名",
                `subdomain` varchar(64) NOT NULL COMMENT "子域名",
                `createtime` int UNSIGNED NOT NULL COMMENT "创建时间",
                `updatetime` int UNSIGNED NOT NULL DEFAULT '0' COMMENT "最后一次更新时间",
                `creator` varchar(16) NULL DEFAULT NULL COMMENT "创建人",
                `updator` varchar(16) NULL DEFAULT NULL COMMENT "最后一次更新人",
                PRIMARY KEY (`stid`),
                INDEX (`type`),
                UNIQUE (`domain`, `subdomain`),
                INDEX (`createtime`)
            ) ENGINE = MyISAM CHARSET=utf8 COLLATE utf8_general_ci COMMENT = '站点类型'
            """
            sql = sql.format(table=self.table)
            self._execute(sql)

    def insert(self, obj={}):
        obj.setdefault('subdomain', '')
        obj.setdefault('createtime', int(time.time()))
        obj.setdefault('updatetime', 0)
        obj.setdefault('status', self.SITETYPE_STATUS_NORMAl)
        return super(SitetypeDB, self).insert(setting=obj)

    def update(self, id, obj = {}):
        obj['updatetime'] = int(time.time())
        return super(SitetypeDB, self).update(setting=obj, where={'stid':id}, multi=False)

    def delete(self, id):
        return super(SitetypeDB, self).update(setting={"status": self.SITETYPE_STATUS_DELETED},
                where={'stid':id}, multi=False)

    def get_detail(self, id):
        return self.get(where={'stid':id})

    def get_detail_by_domain(self, domain, subdomain=None):
        res = None
        if subdomain:
            return self.get(where={'subdomain':subdomain, 'domain': domain})
        return self.get(where={'domain': domain})

    def get_list(self, where = {}, select=None,sort=[('stid', 1)], **kwargs):
        return self.find(where=where, select=select, sort=sort, **kwargs)
