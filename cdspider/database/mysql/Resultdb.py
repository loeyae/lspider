#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 21:30:20
:version: SVN: $Id: Resultdb.py 2173 2018-07-04 12:08:17Z zhangyi $
"""
import time
from cdspider.database.base import ResultDB as BaseResultDB
from .Mysql import Mysql, SplitTableMixin
from cdspider.libs.utils import base64encode

class ResultDB(Mysql, BaseResultDB, SplitTableMixin):

    __tablename__ = 'result'

    def __init__(self, host='localhost', port=27017, db = None, user=None,
            password=None, table=None, **kwargs):
        super(ResultDB, self).__init__(host = host, port = port, db = db,
            user = user, password = password, table = table, **kwargs)
        self._check_tables()

    def insert(self, obj = {}):
        obj.setdefault("createtime", int(time.time()))
        obj.setdefault("created", 0)
        table = self._get_table(obj['createtime'])
        obj.setdefault('status', self.RESULT_STATUS_INIT)
        _id = super(ResultDB, self).insert(setting=obj, table=table)
        if _id:
            return BaseResultDB.build_id(obj['createtime'], _id)
        return False

    def update(self, id, obj = {}):
        table = self._table_name(id)
        _, rid= BaseResultDB.unbuild_id(id)
        obj['updatetime'] = int(time.time())
        return super(ResultDB, self).update(setting=obj, where={"rid": rid}, table=table)

    def add_crwal_info(self, unid, createtime, crawlinfo):
        obj={}
        table = self._get_table(createtime)
        obj['updatetime'] = int(time.time())
        data = self.get(where={"unid": unid}, select={'rid': True, 'crawlinfo': True}, table=table)
        if not data:
            return False
        need_update = True
        _, cv = list(crawlinfo.items())[0]
        if isinstance(data['crawlinfo'], dict):
            for item in data['crawlinfo'].values():
                if cv['task'] == item['task']:
                    need_update = False
                    break
            if need_update:
                data['crawlinfo'].update(crawlinfo)
        else:
            data['crawlinfo'] = crawlinfo
        obj['crawlinfo'] = data['crawlinfo']
        super(ResultDB, self).update(setting=obj, where={"rid": data['rid']}, table=table)
        return BaseResultDB.build_id(createtime, data['rid'])

    def get_detail(self, id):
        table = self._table_name(id)
        _, rid= BaseResultDB.unbuild_id(id)
        result = self.get(where={"rid": rid}, table=table)
        if result:
            result['rid'] = id
        return result

    def get_detail_by_unid(self, unid, createtime):
        table = self._get_table(createtime)
        result = self.get(where = {"unid", unid}, table=table)
        if result:
            prefix = time.strftime("%Y%m", time.localtime(createtime))
            result['rid'] = base64encode("%s%d" % (prefix, result['rid']))
        return result

    def get_list(self, createtime, where = {}, select = None, sort=[("createtime", 1)], **kwargs):
        table = self._get_table(createtime)
        data = list(self.find(table=table, where=where, select=select, sort=sort, **kwargs))
        prefix = time.strftime("%Y%m", time.localtime(createtime))
        for item in data: item['rid'] = base64encode("%s%d" % (prefix, item['rid']))
        return data

    def get_count(self, createtime, where = {}, select = None, **kwargs):
        table = self._get_table(createtime)
        return self.count(table=table, where=where, select=select, **kwargs)

    def aggregate_by_day(self, createtime, where = {}):
        table = self._get_table(createtime)
        return self.count(table=table, where=where, select='FROM_UNIXTIME(createtime,"%d") AS day, COUNT(*) AS count', group='day', sort=[('day', 1)])

    @property
    def archive_fields(self):
        return ['result', 'crawlinfo']

    def _get_table(self, createtime):
        suffix = time.strftime("%Y%m", time.localtime(createtime))
        name = super(ResultDB, self)._table_name(suffix)
        if not name in self._tables:
            self._create_table(name)
        return name

    def _table_name(self, id):
        suffix, _ = BaseResultDB.unbuild_id(id)
        name = super(ResultDB, self)._table_name(suffix)
        if not name in self._tables:
            self._create_table(name)
        return name

    def _check_tables(self):
        self._list_table()
        suffix = time.strftime("%Y%m")
        name = super(ResultDB, self)._table_name(suffix)
        if not name in self._tables:
            self._create_table(name)

    def _create_table(self, table):
        sql = """
        CREATE TABLE IF NOT EXISTS `%s` (
            `rid` bigint UNSIGNED NOT NULL AUTO_INCREMENT COMMENT "result id",
            `unid` varchar(32) NOT NULL COMMENT "unique str",
            `crawl_id` int UNSIGNED NOT NULL COMMENT "抓取id, 与siteid一起标识同一站点的同一批次的结果",
            `url` varchar(512) COMMENT "抓取的url",
            `status` tinyint UNSIGNED NOT NULL DEFAULT '0' COMMENT "状态",
            `sitetype` varchar(16) NULL DEFAULT NULL COMMENT "站点类型",
            `projectid` int UNSIGNED NOT NULL COMMENT "project id",
            `siteid` bigint UNSIGNED NOT NULL COMMENT "site id",
            `urlid` bigint UNSIGNED NOT NULL DEFAULT '0' COMMENT "urls id",
            `atid` bigint UNSIGNED NOT NULL DEFAULT '0' COMMENT "attachment id",
            `kwid` bigint UNSIGNED NOT NULL DEFAULT '0' COMMENT "keywords id",
            `parentid` varchar(32) NOT NULL DEFAULT '0' COMMENT "parent id",
            `domain` varchar(64) NULL DEFAULT NULL COMMENT "站点域名",
            `title` varchar(128) NULL DEFAULT NULL COMMENT "标题",
            `author` varchar(64) NULL DEFAULT NULL COMMENT "作者",
            `created` int UNSIGNED NOT NULL COMMENT "发布时间",
            `summary` text NULL DEFAULT NULL COMMENT "摘要",
            `content` longtext NULL DEFAULT NULL COMMENT "正文",
            `crawlinfo` longtext NULL DEFAULT NULL COMMENT "抓取信息 [{\\"project\\":projectid,\\"task\\":taskid,\\"urls\\":urlid,\\"keywords\\":keywordid,\\"crawltime\\":crawltime},..]",
            `source` longtext NULL DEFAULT NULL COMMENT "抓到的源码",
            `result` longtext NULL DEFAULT NULL COMMENT "解析后的结果",
            `createtime` int UNSIGNED NOT NULL COMMENT "更新时间",
            `updatetime` int UNSIGNED NOT NULL DEFAULT '0' COMMENT "更新时间",
            PRIMARY KEY (`rid`),
            UNIQUE (`unid`),
            INDEX `s_c` (`siteid`, `crawl_id`),
            INDEX `p_s_u` (`projectid`, `siteid`, `urlid`),
            INDEX `p_s_a` (`projectid`, `siteid`, `atid`),
            INDEX `p_s_k` (`projectid`, `siteid`, `kwid`),
            INDEX (`status`),
            INDEX (`parentid`),
            INDEX (`createtime`)
        ) ENGINE = MyISAM CHARSET=utf8 COLLATE utf8_general_ci COMMENT = '结果集'
        """
        sql = sql % table
        self._execute(sql)
