#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-23 11:18:57
:version: SVN: $Id: Admindb.py 1048 2018-06-08 03:53:46Z zhangyi $
"""
import time
from cdspider.database.base import AdminDB as BaseAdminDB
from .Mongo import Mongo
from cdspider.libs import utils

class AdminDB(Mongo, BaseAdminDB):
    """
    admin db
    """
    __tablename__ = 'admins'

    def __init__(self, host='localhost', port=27017, db = None, user=None,
                password=None, table=None, **kwargs):
        super(AdminDB, self).__init__(host = host, port = port, db = db,
            user = user, password = password, table = table, **kwargs)
        collection = self._db.get_collection(self.table)
        indexes = collection.index_information()
        if not 'aid' in indexes:
            collection.create_index('aid', unique=True, name='aid')
        if not 'email' in indexes:
            collection.create_index('email', unique=True, name='email')
        if not 'status' in indexes:
            collection.create_index('status', name='status')
        if not 'ruleid' in indexes:
            collection.create_index('ruleid', name='ruleid')
        if not 'createtime' in indexes:
            collection.create_index('createtime', name='createtime')
        if not collection.find_one({"email":"cdspider@beyondsoft.com"}):
            collection.insert_one({
                            "aid": self._get_increment(self.table),
                            "name": "admin",
                            "email": "cdspider@beyondsoft.com",
                            "password": self.encrypt("admin@cdspider"),
                            "status": self.ADMIN_STATUS_ACTIVE,
                            "ruleid": self.ADMIN_RULE_ADMIN_MANAGER,
                            "createtime": int(time.time()),
                            "updatetime": 0
                        })

    def get_detail(self, id):
        return self.get(where={"aid": id})

    def get_detail_by_email(self, email):
        return self.get(where={"email": email})

    def encrypt(self, str):
        str += utils.base64encode(str)
        return utils.md5(str)

    def insert(self, obj = {}):
        obj['aid'] = self._get_increment(self.table)
        obj['password'] = self.encrypt(obj['password'])
        obj.setdefault('status', self.ADMIN_STATUS_INIT)
        obj.setdefault('createtime', int(time.time()))
        obj.setdefault('updatetime', 0)
        super(AdminDB, self).insert(setting=obj)
        return obj['aid']

    def update(self, id, obj):
        obj['updatetime'] = int(time.time())
        if "password" in obj:
            obj['password'] = self.encrypt(obj['password'])
        return super(AdminDB, self).update(setting=obj, where={"aid": id}, multi=False)

    def active(self, id):
        return super(AdminDB, self).update(setting={"status": self.ADMIN_STATUS_ACTIVE},
                where={"aid": int(id)}, multi=False)

    def disable(self, id):
        return super(AdminDB, self).update(setting={"status": self.ADMIN_STATUS_DISABLE},
                where={"aid": int(id)}, multi=False)

    def delete(self, id):
        return super(AdminDB, self).update(setting={"status": self.ADMIN_STATUS_DELETED},
                where={"aid": int(id)}, multi=False)

    def get_list(self, where = {}, select = None, **kwargs):
        kwargs.setdefault('sort', [('aid', 1)])
        return self.find(where=where, select=select, **kwargs)

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
