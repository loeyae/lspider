#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-23 11:18:57
:version: SVN: $Id: Admindb.py 776 2018-02-09 02:04:11Z liang $
"""
import time
from cdspider.database.base import AdminDB as BaseAdminDB
from .Mysql import Mysql
from cdspider.libs import utils

class AdminDB(Mysql, BaseAdminDB):
    """
    admin db
    """
    __tablename__ = 'admins'

    def __init__(self, host='localhost', port=27017, db = None, user=None,
                password=None, table=None, **kwargs):
        super(AdminDB, self).__init__(host = host, port = port, db = db,
            user = user, password = password, table = table, **kwargs)

        self._init_table()

    def _init_table(self):
        if not self.have_table(self.table):
            sql = """
            CREATE TABLE `{table}` (
                `aid` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'admin id' ,
                `ruleid` TINYINT(1) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'rule id' ,
                `status` TINYINT(1) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'status' ,
                `name` VARCHAR(32) NULL DEFAULT NULL COMMENT 'name' ,
                `email` VARCHAR(128) NOT NULL COMMENT 'email' ,
                `password` CHAR(32) NOT NULL COMMENT 'password' ,
                `createtime` INT(10) UNSIGNED NOT NULL COMMENT 'createtime' ,
                `updatetime` INT(10) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'updatetime' ,
                `updator` VARCHAR(32) NULL DEFAULT NULL COMMENT 'updator' ,
                PRIMARY KEY (`aid`),
                UNIQUE (`email`),
                INDEX (`ruleid`),
                INDEX (`status`),
                INDEX (`createtime`)
            ) ENGINE = MyISAM CHARSET=utf8 COLLATE utf8_general_ci COMMENT = '管理员'
            """
            sql = sql.format(table=self.table)
            self._execute(sql)
            insert_sql = """
            INSERT INTO `{table}` (`ruleid`, `status`, `name`, `email`, `password`, `createtime`)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            insert_sql = insert_sql.format(table=self.table)
            self._execute(insert_sql, [
                                        self.ADMIN_RULE_ADMIN_MANAGER,
                                        self.ADMIN_STATUS_ACTIVE,
                                        "admin",
                                        "cdspider@beyondsoft.com",
                                        self.encrypt("admin@cdspider"),
                                        int(time.time())])

    def get_detail(self, id):
        return self.get(where={"aid": id})

    def get_detail_by_email(self, email):
        return self.get(where={"email": email})

    def encrypt(self, str):
        str += utils.base64encode(str)
        return utils.md5(str)

    def insert(self, obj = {}):
        obj['password'] = self.encrypt(obj['password'])
        obj.setdefault('status', self.ADMIN_STATUS_INIT)
        obj.setdefault('createtime', int(time.time()))
        obj.setdefault('updatetime', 0)
        _id = super(AdminDB, self).insert(setting=obj)
        if _id:
            return _id
        return False

    def update(self, id, obj):
        obj['updatetime'] = int(time.time())
        if "password" in obj:
            obj['password'] = self.encrypt(obj['password'])
        return super(AdminDB, self).update(setting=obj, where={"aid": id}, multi=False)

    def active(self, id):
        return super(AdminDB, self).update(setting={"status": self.ADMIN_STATUS_ACTIVE},
                where={"aid": id}, multi=False)

    def disable(self, id):
        return super(AdminDB, self).update(setting={"status": self.ADMIN_STATUS_DISABLE},
                where={"aid": id}, multi=False)

    def delete(self, id):
        return super(AdminDB, self).update(setting={"status": self.ADMIN_STATUS_DELETED},
                where={"aid": id}, multi=False)

    def get_list(self, where = {}, select = None,sort=[('aid', 1)], **kwargs):
        return self.find(where=where, sort=sort, select=select, **kwargs)

    def verify_user(self, email, password):
        admin = self.get(where={"email": email})
        if not admin:
            return False, 'exists'
        if admin['ruleid'] == self.ADMIN_RULE_NONE:
            return False, 'rule'
        if admin['status'] != self.ADMIN_STATUS_ACTIVE:
            return False, 'status'
        password = self.encrypt(password)
        if password != admin['password']:
            return False, 'password'
        return True, admin
