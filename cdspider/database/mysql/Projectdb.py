#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 21:29:26
:version: SVN: $Id: Projectdb.py 1415 2018-06-22 06:27:35Z zhangyi $
"""
import time
from cdspider.database.base import ProjectDB as BaseProjectDB
from .Mysql import Mysql

class ProjectDB(Mysql, BaseProjectDB):
    """
    项目DAO
    """
    __tablename__ = "projects"

    def __init__(self, host='localhost', port=27017, db = None, user=None,
                password=None, table=None, **kwargs):
        super(ProjectDB, self).__init__(host = host, port = port, db = db,
            user = user, password = password, table = table, **kwargs)
        self._init_table()

    @property
    def archive_fields(self):
        return ['base_request', 'main_process', 'sub_process', 'custom_columns', 'identify']

    def _init_table(self):
        if not self.have_table(self.table):
            sql = """
            CREATE TABLE `{table}` (
                `pid` bigint UNSIGNED NOT NULL AUTO_INCREMENT COMMENT "项目id",
                `title` varchar(32) NOT NULL COMMENT "项目标题",
                `type` tinyint UNSIGNED NOT NULL DEFAULT '0' COMMENT "项目类型",
                `status` tinyint UNSIGNED NOT NULL DEFAULT '0' COMMENT "项目状态",
                `script` longtext NULL DEFAULT NULL COMMENT "自定义handler",
                `base_request` longtext NULL DEFAULT NULL COMMENT "基础请求配置",
                `main_process` longtext NULL DEFAULT NULL COMMENT "主请求流程配置",
                `sub_process` longtext NULL DEFAULT NULL COMMENT "子请求流程配置",
                `custom_columns` longtext NULL DEFAULT NULL COMMENT "自定义字段",
                `identify` text NULL DEFAULT NULL COMMENT "生成unique id的配置",
                `comments` text NULL DEFAULT NULL COMMENT "项目描述",
                `rate` tinyint  UNSIGNED NOT NULL DEFAULT '0' COMMENT "更新频率",
                `lastsid` bigint UNSIGNED NOT NULL DEFAULT '0' COMMENT "最后入队的 site id",
                `createtime` int UNSIGNED NOT NULL COMMENT "创建时间",
                `updatetime` int UNSIGNED NOT NULL DEFAULT '0' COMMENT "最后一次更新时间",
                `creator` varchar(16) NULL DEFAULT NULL COMMENT "创建人",
                `updator` varchar(16) NULL DEFAULT NULL COMMENT "最后一次更新人",
                PRIMARY KEY (`pid`),
                INDEX (`title`),
                INDEX `t_s` (`type`, `status`),
                INDEX (`lastsid`),
                INDEX (`createtime`)
            ) ENGINE = MyISAM CHARSET=utf8 COLLATE utf8_general_ci COMMENT = '项目'
            """
            sql = sql.format(table=self.table)
            self._execute(sql)

    def get_detail(self, id):
        return self.get(where={'pid': id})

    def insert(self, obj):
        obj.setdefault('type', BaseProjectDB.PROJECT_TYPE_GENERAL)
        obj.setdefault('status', BaseProjectDB.PROJECT_STATUS_INIT)
        obj.setdefault('createtime', int(time.time()))
        obj.setdefault('updatetime', 0)
        return super(ProjectDB, self).insert(setting=obj)

    def update(self, id, obj):
        obj['updatetime'] = int(time.time())
        return super(ProjectDB, self).update(setting=obj, where={"pid": id}, multi=False)

    def enable(self, id):
        return super(ProjectDB, self).update(setting={"status": self.PROJECT_STATUS_INIT},
                where={"pid": id}, multi=False)

    def active(self, id):
        return super(ProjectDB, self).update(setting={"status": self.PROJECT_STATUS_ACTIVE},
                where={"pid": id}, multi=False)

    def disable(self, id):
        return super(ProjectDB, self).update(setting={"status": self.PROJECT_STATUS_DISABLE},
                where={"pid": id}, multi=False)

    def delete(self, id):
        return super(ProjectDB, self).update(setting={"status": self.PROJECT_STATUS_DELETED},
                where={"pid": id}, multi=False)

    def get_list(self, where = {}, select = None, sort=[("pid", 1)], **kwargs):
        return self.find(where=where, select=select, sort=sort, **kwargs)

    def get_count(self, where = {}, select = None, **kwargs):
        return self.count(where=where, select=select, **kwargs)

    def get_list_c(self, where = {}, select = None,sort=[("createtime", 1)], **kwargs):
        return self.find(where=where, select=select, sort=sort, **kwargs)
