#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 21:58:31
:version: SVN: $Id: Urlsdb.py 2134 2018-07-04 06:28:08Z zhangyi $
"""
import time
from cdspider.database.base import UrlsDB as BaseUrlsDB
from .Mysql import Mysql

class UrlsDB(Mysql, BaseUrlsDB):
    """
    站点类型DAO
    """
    __tablename__ = "urls"

    def __init__(self, host='localhost', port=27017, db = None, user=None,
                password=None, table=None, **kwargs):
        super(UrlsDB, self).__init__(host = host, port = port, db = db,
            user = user, password = password, table = table, **kwargs)
        self._init_table()

    @property
    def archive_fields(self):
        return ['base_request', 'main_process', 'sub_process', 'identify']

    def _init_table(self):
        if not self.have_table(self.table):
            sql = """
            CREATE TABLE `{table}` (
                `uid` bigint UNSIGNED NOT NULL AUTO_INCREMENT COMMENT "url id",
                `title` varchar(32) NOT NULL COMMENT "title",
                `siteid` bigint UNSIGNED NOT NULL COMMENT "site id",
                `projectid` int UNSIGNED NOT NULL COMMENT "project id",
                `url` varchar(512) NOT NULL COMMENT "url",
                `rate` tinyint NOT NULL DEFAULT '0' COMMENT "int",
                `status` tinyint NOT NULL DEFAULT '0' COMMENT "status",
                `base_request` longtext NULL DEFAULT NULL COMMENT "基础请求配置",
                `main_process` longtext NULL DEFAULT NULL COMMENT "主请求流程配置",
                `sub_process` longtext NULL DEFAULT NULL COMMENT "子请求流程配置",
                `identify` text NULL DEFAULT NULL COMMENT "生成unique id的配置",
                `desc` text NULL DEFAULT NULL COMMENT "描述",
                `createtime` int NOT NULL COMMENT "create time",
                `updatetime` int NOT NULL DEFAULT '0' COMMENT "last update time",
                `creator` varchar(16) NULL DEFAULT NULL COMMENT "creator",
                `updator` varchar(16) NULL DEFAULT NULL COMMENT "updator",
                PRIMARY KEY (`uid`),
                INDEX (`title`),
                INDEX `s_s` (`siteid`, `status`),
                FULLTEXT (`url`),
                INDEX (`createtime`)
            ) ENGINE = MyISAM CHARSET=utf8 COLLATE utf8_general_ci COMMENT = '站点类型'
            """
            sql = sql.format(table=self.table)
            self._execute(sql)

    def insert(self, obj = {}):
        obj.setdefault('status', self.URLS_STATUS_INIT)
        obj.setdefault('createtime', int(time.time()))
        obj.setdefault('updatetime', 0)
        _id = super(UrlsDB, self).insert(setting=obj)
        if _id:
            return _id
        return False

    def update(self, id, obj = {}):
        obj['updatetime'] = int(time.time())
        return super(UrlsDB, self).update(setting=obj, where={'uid':id}, multi=False)

    def delete(self, id, where = {}):
        obj = {"status": self.URLS_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'uid':int(id)}
        else:
            where.update({'uid':int(id)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=False)

    def delete_by_site(self, sid, where={}):
        obj = {"status": self.URLS_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'siteid':int(sid)}
        else:
            where.update({'siteid':int(sid)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=True)

    def delete_by_project(self, pid, where = {}):
        obj = {"status": self.URLS_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'projectid':int(pid)}
        else:
            where.update({'projectid':int(pid)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=True)

    def enable(self, id, where = {}):
        obj = {"status": self.URLS_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'uid':int(id)}
        else:
            where.update({'uid':int(id)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=False)

    def enable_by_site(self, sid, where = {}):
        obj = {"status": self.URLS_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'siteid':int(sid)}
        else:
            where.update({'siteid':int(sid)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=True)

    def enable_by_project(self, pid, where = {}):
        obj = {"status": self.URLS_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'projectid':int(pid)}
        else:
            where.update({'projectid':int(pid)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=True)

    def active(self, id, where = {}):
        obj = {"status": self.URLS_STATUS_ACTIVE}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'uid':int(id)}
        else:
            where.update({'uid':int(id)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=False)

    def disable(self, id, where = {}):
        obj = {"status": self.URLS_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'uid':int(id)}
        else:
            where.update({'uid':int(id)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=False)

    def disable_by_site(self, sid, where = {}):
        obj = {"status": self.URLS_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'siteid':int(sid)}
        else:
            where.update({'siteid':int(sid)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=True)

    def disable_by_project(self, pid, where = {}):
        obj = {"status": self.URLS_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        if not where:
            where = {'projectid':int(pid)}
        else:
            where.update({'projectid':int(pid)})
        return super(UrlsDB, self).update(setting=obj, where=where, multi=True)

    def get_detail(self, id):
        return self.get(where={'uid':id})

    def get_list(self, where = {}, select=None, sort=[('uid', 1)],**kwargs):
        return self.find(where=where, sort=sort, select=select, **kwargs)

    def get_new_list(self, id, siteid, select=None, **kwargs):
        return self.find(where=[('uid', '>', id), ('siteid', siteid)], select=select, sort=[('uid', 1)], **kwargs)

    def get_max_id(self, siteid):
        data = self.get(where={"siteid": siteid}, select="MAX(`uid`) AS maxid")
        return data['maxid']
