#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 21:32:22
:version: SVN: $Id: Sitedb.py 2118 2018-07-04 03:56:24Z zhangyi $
"""
import time
from cdspider.database.base import SiteDB as BaseSiteDB
from .Mysql import Mysql

class SiteDB(Mysql, BaseSiteDB):
    """
    站点DAO
    """
    __tablename__ = "sites"

    def __init__(self, host='localhost', port=27017, db = None, user=None,
                password=None, table=None, **kwargs):
        super(SiteDB, self).__init__(host = host, port = port, db = db,
            user = user, password = password, table = table, **kwargs)
        self._init_table()

    @property
    def archive_fields(self):
        return ['base_request', 'main_process', 'sub_process', 'identify']

    def _init_table(self):
        if not self.have_table(self.table):
            sql = """
            CREATE TABLE `{table}` (
                `sid` bigint UNSIGNED NOT NULL AUTO_INCREMENT COMMENT "site id",
                `name` varchar(32) NOT NULL COMMENT "站点名称",
                `status` tinyint UNSIGNED NOT NULL DEFAULT '0' COMMENT "site status",
                `projectid` bigint UNSIGNED NOT NULL COMMENT "project id",
                `rate` tinyint UNSIGNED NOT NULL DEFAULT '0'  COMMENT "更新频率",
                `url` varchar(512) NOT NULL COMMENT "基础 url",
                `stid` int UNSIGNED NOT NULL DEFAULT '0' COMMENT "sitetype id",
                `script` longtext NULL DEFAULT NULL COMMENT "自定义handler",
                `base_request` longtext NULL DEFAULT NULL COMMENT "基础请求配置",
                `main_process` longtext NULL DEFAULT NULL COMMENT "主请求流程配置",
                `sub_process` longtext NULL DEFAULT NULL COMMENT "子请求流程配置",
                `identify` longtext NULL DEFAULT NULL COMMENT "生成unique id的配置",
                `limb` tinyint COMMENT "是否包含子集， 0 为否， 1为是",
                `domain` varchar(64) NOT NULL COMMENT "站点域名",
                `desc` text NULL DEFAULT NULL COMMENT "站点描述",
                `createtime` int UNSIGNED NOT NULL COMMENT "创建时间",
                `updatetime` int UNSIGNED NOT NULL DEFAULT '0' COMMENT "最后一次更新时间",
                `lastuid` int UNSIGNED NOT NULL DEFAULT '0' COMMENT "最后一次入队的 url id",
                `lastkwid` int UNSIGNED NOT NULL DEFAULT '0' COMMENT "最后一次入队的 keyword id",
                `creator` varchar(16) NULL DEFAULT NULL COMMENT "创建人",
                `updator` varchar(16) NULL DEFAULT NULL COMMENT "最后一次更新人",
                PRIMARY KEY (`sid`),
                INDEX (`name`),
                INDEX `p_s` (`projectid`, `status`),
                INDEX `s_s` (`stid`, `status`),
                INDEX (`lastuid`),
                INDEX (`lastkwid`),
                INDEX (`createtime`)
            ) ENGINE = MyISAM CHARSET=utf8 COLLATE utf8_general_ci COMMENT = '站点'
            """
            sql = sql.format(table=self.table)
            self._execute(sql)

    def insert(self, obj={}):
        obj.setdefault('status', self.SITE_STATUS_INIT)
        obj.setdefault('createtime', int(time.time()))
        obj.setdefault('updatetime', 0)
        return super(SiteDB, self).insert(setting=obj)

    def enable(self, id, where = {}):
        if not where:
            where = {'sid': int(id)}
        else:
            where.update({'sid': int(id)})
        return super(SiteDB, self).update(setting={"status": self.SITE_STATUS_INIT, 'updatetime': int(time.time())},
                where=where, multi=False)

    def enable_by_project(self, pid, where = {}):
        if not where:
            where = {'projectid': int(pid)}
        else:
            where.update({'projectid': int(pid)})
        return super(SiteDB, self).update(setting={"status": self.SITE_STATUS_INIT, 'updatetime': int(time.time())},
                where=where, multi=True)

    def disable(self, id, where = {}):
        if not where:
            where = {'sid': int(id)}
        else:
            where.update({'sid': int(id)})
        return super(SiteDB, self).update(setting={"status": self.SITE_STATUS_DISABLE, 'updatetime': int(time.time())},
                where=where, multi=False)

    def disable_by_project(self, pid, where = {}):
        if not where:
            where = {'projectid': int(pid)}
        else:
            where.update({'projectid': int(pid)})
        return super(SiteDB, self).update(setting={"status": self.SITE_STATUS_DISABLE, 'updatetime': int(time.time())},
                where=where, multi=True)

    def active(self, id, where = {}):
        if not where:
            where = {'sid': int(id)}
        else:
            where.update({'sid': int(id)})
        return super(SiteDB, self).update(setting={"status": self.SITE_STATUS_ACTIVE, 'updatetime': int(time.time())},
                where=where, multi=False)

    def update(self, id, obj = {}):
        obj['updatetime'] = int(time.time())
        return super(SiteDB, self).update(setting=obj, where={'sid':id}, multi=False)

    def delete(self, id, where = {}):
        if not where:
            where = {'sid': int(id)}
        else:
            where.update({'sid': int(id)})
        return super(SiteDB, self).update(setting={"status": self.SITE_STATUS_DELETED, 'updatetime': int(time.time())},
                where=where, multi=False)

    def delete_by_project(self, pid, where = {}):
        if not where:
            where = {'projectid': int(pid)}
        else:
            where.update({'projectid': int(pid)})
        return super(SiteDB, self).update(setting={"status": self.SITE_STATUS_DELETED, 'updatetime': int(time.time())},
                where=where, multi=True)

    def get_detail(self, id):
        return self.get(where={'sid':id})

    def get_new_list(self, id, projectid, select=None, **kwargs):
        return self.find(where=[('sid', '>', id), ('projectid', projectid), ("status", BaseSiteDB.SITE_STATUS_ACTIVE)], select=select, sort=[('sid', 1)], **kwargs)

    def get_list(self, where, select=None, sort=[('sid', 1)],**kwargs):
        return self.find(where=where, select=select, sort=sort, **kwargs)

    def get_max_id(self, projectid):
        data = self.get(where={"projectid": projectid}, select="MAX(`sid`) AS maxid")
        return data['maxid']
