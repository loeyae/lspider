#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 21:33:44
:version: SVN: $Id: Taskdb.py 2142 2018-07-04 06:44:38Z zhangyi $
"""
import time
from cdspider.database.base import TaskDB as BaseTaskDB
from .Mysql import Mysql, SplitTableMixin

class TaskDB(Mysql, BaseTaskDB, SplitTableMixin):

    __tablename__ = 'task'

    def __init__(self, host='localhost', port=27017, db = None, user=None,
            password=None, table=None, **kwargs):
        super(TaskDB, self).__init__(host = host, port = port, db = db,
            user = user, password = password, table = table, **kwargs)
        self._list_table()

    def insert(self, obj):
        table = self._table_name(obj['projectid'])
        obj.setdefault('status', self.TASK_STATUS_INIT)
        obj.setdefault('createtime', int(time.time()))
        obj.setdefault('updatetime', 0)
        obj.setdefault('urlid', 0)
        obj.setdefault('atid', 0)
        obj.setdefault('kwid', 0)
        return super(TaskDB, self).insert(setting=obj, table=table)

    def update(self, id, pid, obj):
        table = self._table_name(pid)
        obj['updatetime'] = int(time.time())
        return super(TaskDB, self).update(setting=obj, where={"tid": id}, table=table, multi=False)

    def update_url_by_site(self, id, pid, url):
        table = self._table_name(pid)
        obj={"url": url}
        obj['updatetime'] = int(time.time())
        return super(TaskDB, self).update(setting=obj, where={"siteid": int(id), "atid": 0, "urlid": 0}, table=table, multi=False)

    def update_url_by_urls(self, id, pid, url):
        table = self._table_name(pid)
        obj={"url": url}
        obj['updatetime'] = int(time.time())
        return super(TaskDB, self).update(setting=obj, where={"urlid": int(id), "atid": 0}, table=table, multi=False)

    def enable(self, id, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'tid': int(id)}
        else:
            where.update({'tid': int(id)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=False)

    def enable_by_project(self, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = int(time.time())
        obj['save'] = None
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def enable_by_site(self, sid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'siteid': int(sid)}
        else:
            where.update({'siteid': int(sid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def enable_by_urls(self, uid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'urlid': int(uid)}
        else:
            where.update({'urlid': int(uid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def enable_by_attachment(self, aid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'atid': int(aid)}
        else:
            where.update({'atid': int(aid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def enable_by_keyword(self, kid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_INIT}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'kwid': int(kid)}
        else:
            where.update({'kwid': int(kid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def disable(self, id, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'tid': int(id)}
        else:
            where.update({'tid': int(id)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=False)

    def disable_by_project(self, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def disable_by_site(self, sid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'siteid': int(sid)}
        else:
            where.update({'siteid': int(sid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def disable_by_urls(self, uid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'urlid': int(uid)}
        else:
            where.update({'urlid': int(uid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def disable_by_attachment(self, aid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'atid': int(aid)}
        else:
            where.update({'atid': int(aid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def disable_by_keyword(self, kid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DISABLE}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'kwid': int(kid)}
        else:
            where.update({'kwid': int(kid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def active(self, id, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_ACTIVE}
        obj['plantime'] = int(time.time())
        if not where:
            where = {'tid': int(id)}
        else:
            where.update({'tid': int(id)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=False)

    def active_by_site(self, sid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_ACTIVE}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = int(time.time())
        if not where:
            where = {'siteid': int(sid)}
        else:
            where.update({'siteid': int(sid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def active_by_urls(self, uid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_ACTIVE}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = int(time.time())
        if not where:
            where = {'urlid': int(uid)}
        else:
            where.update({'urlid': int(uid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def active_by_attachment(self, aid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_ACTIVE}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = int(time.time())
        if not where:
            where = {'atid': int(aid)}
        else:
            where.update({'atid': int(aid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def active_by_keyword(self, kid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_ACTIVE}
        obj['updatetime'] = int(time.time())
        obj['plantime'] = int(time.time())
        if not where:
            where = {'kwid': int(kid)}
        else:
            where.update({'kwid': int(kid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def delete(self, id, pid, obj, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'tid': int(id)}
        else:
            where.update({'tid': int(id)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=False)

    def delete_by_project(self, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def delete_by_site(self, sid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'siteid': int(sid)}
        else:
            where.update({'siteid': int(sid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def delete_by_urls(self, uid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'urlid': int(uid)}
        else:
            where.update({'urlid': int(uid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def delete_by_attachment(self, aid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'atid': int(aid)}
        else:
            where.update({'atid': int(aid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def delete_by_keyword(self, kid, pid, where = {}):
        table = self._table_name(pid)
        obj={"status": self.TASK_STATUS_DELETED}
        obj['updatetime'] = int(time.time())
        obj['save'] = None
        if not where:
            where = {'kwid': int(kid)}
        else:
            where.update({'kwid': int(kid)})
        return super(TaskDB, self).update(setting=obj, where=where, table=table, multi=True)

    def get_detail(self, id, pid, crawlinfo=False):
        table = self._table_name(pid)
        if crawlinfo:
            select=None
        else:
            select={"crawlinfo": False}
        return self.get(where={"tid":id}, select=select, table=table)

    def get_count(self, pid, where = {}):
        table = self._table_name(pid)
        return self.count(where=where, table=table)

    def get_list(self, pid, where = {}, select=None, **kwargs):
        table = self._table_name(pid)
        return self.find(where=where, sort=[("tid", 1)], table=table, select=select, **kwargs)

    def get_init_list(self, pid, where = {}, select=None, **kwargs):
        table = self._table_name(pid)
        where['status'] = self.TASK_STATUS_ACTIVE
        where['plantime'] = 0
        return self.find(where=where, sort=[("tid", 1)], table=table, select=select, **kwargs)

    def get_plan_list(self, pid, plantime, where = {}, select=None, **kwargs):
        table = self._table_name(pid)
        where['status'] = TaskDB.TASK_STATUS_ACTIVE
        where['plantime'] = {"$lte": plantime}
        return self.find(where=where, sort=[("tid", 1)], table=table, select=select, **kwargs)

    @property
    def archive_fields(self):
        return ['crawlinfo', 'save']

    def _table_name(self, pid):
        table = super(TaskDB, self)._table_name(pid)
        self.logger.debug("currunt table %s, have tables: %s" % (table, self._tables))
        if not table in self._tables:
            self._create_table(table)
        return table

    def _create_table(self, table):
        sql = """
        CREATE TABLE IF NOT EXISTS `{table}` (
            `tid` bigint UNSIGNED NOT NULL AUTO_INCREMENT COMMENT "task id",
            `projectid` bigint UNSIGNED NOT NULL COMMENT "project id",
            `siteid` bigint UNSIGNED NOT NULL COMMENT "site id",
            `kwid` bigint UNSIGNED NOT NULL COMMENT "keyword id, if exists, default: 0",
            `urlid` bigint UNSIGNED NOT NULL COMMENT "url id, if exists, default: 0",
            `atid` bigint UNSIGNED NOT NULL COMMENT "attachment id, if exists, default: 0",
            `url` varchar(512) NOT NULL COMMENT "base url",
            `rate` int UNSIGNED NOT NULL DEFAULT '0' COMMENT "抓取频率",
            `script` text NULL DEFAULT NULL COMMENT "自定义handler",
            `status` tinyint UNSIGNED NOT NULL DEFAULT '0' COMMENT "status, default: 0",
            `save` text COMMENT "保留的参数",
            `queuetime` int UNSIGNED NOT NULL DEFAULT '0' COMMENT "最近一次入队时间",
            `crawltime` int UNSIGNED NOT NULL DEFAULT '0' COMMENT "最近一次抓取时间",
            `crawlinfo` text NULL DEFAULT NULL COMMENT "最近10次抓取记录",
            `plantime` int UNSIGNED NOT NULL DEFAULT '0' COMMENT "计划执行时间",
            `createtime` int UNSIGNED NOT NULL COMMENT "创建时间",
            `updatetime` int UNSIGNED NOT NULL DEFAULT '0' COMMENT "修改时间",
            PRIMARY KEY (`tid`),
            UNIQUE s_u_a_k (`siteid`, `urlid`, `atid`, `kwid`),
            INDEX (`status`),
            INDEX (`plantime`),
            INDEX (`createtime`)
        ) ENGINE = MyISAM CHARSET=utf8 COLLATE utf8_general_ci COMMENT = '任务'
        """
        sql = sql.format(table=table)
        self._execute(sql)
