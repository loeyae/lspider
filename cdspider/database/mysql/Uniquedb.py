#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 21:36:43
:version: SVN: $Id: Uniquedb.py 2429 2018-07-31 01:30:42Z zhangyi $
"""
import time
from cdspider.database.base import UniqueDB as BaseUniqueDB
from .Mysql import Mysql, SplitTableMixin
from mysql.connector.errors import IntegrityError

class UniqueDB(Mysql, BaseUniqueDB, SplitTableMixin):

    __tablename__ = 'unique'

    def __init__(self, host='localhost', port=27017, db = None, user=None,
            password=None, table=None, **kwargs):
        super(UniqueDB, self).__init__(host = host, port = port, db = db,
            user = user, password = password, table = table, **kwargs)
        self._check_table()

    def insert(self, obj, projectid, taskid, urlid, attachid, kwid, createtime):
        unid = self.build(obj)
        table = self._table_name(unid)
        try:
            super(UniqueDB, self).insert({"unid": unid, "projectid": projectid, "taskid": taskid, "urlid": urlid, "attachid": attachid, "kwid": kwid, "createtime": createtime}, table=table)
            return (True, {'unid': unid, 'createtime': createtime})
        except IntegrityError:
            result = self.get(where={"unid": unid}, table=table)
            return (False, {"unid": result['unid'], "createtime": result['createtime']})
        except:
            return (False, False)

    def _table_name(self, unid):
        return super(UniqueDB, self)._table_name(unid[0:1].lower())

    def _check_table(self):
        self._list_table()
        seq = [k for k in range(0, 10)] + [chr(k) for k in range(97, 103)] #[chr(k) for k in range(65, 91)]
        if not self._tables:
            sql = """
            CREATE TABLE `{table}` (
                `unid` char(32) NOT NULL COMMENT "unique str",
                `projectid` int NOT NULL DEFAULT '0' COMMENT "项目ID",
                `siteid` int NOT NULL DEFAULT '0' COMMENT "站点ID",
                `urlid` int NOT NULL DEFAULT '0' COMMENT "URL ID",
                `attachid` int NOT NULL DEFAULT '0' COMMENT "attachment ID",
                `kwid` int NOT NULL DEFAULT '0' COMMENT 关键词ID",
                `createtime` int NOT NULL COMMENT "创建时间",
                PRIMARY KEY (`unid`),
                INDEX (`projectid`),
                INDEX (`siteid`),
                INDEX (`urlid`),
                INDEX (`attachid`),
                INDEX (`kwid`)
            ) ENGINE = MyISAM CHARSET=utf8 COLLATE utf8_general_ci COMMENT = '唯一索引'
            """
            for i in seq:
                tablename = self._table_name(str(i))
                csql = sql.format(table=tablename)
                self._execute(csql)
