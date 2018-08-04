#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-23 11:05:39
:version: SVN: $Id: Admindb.py 469 2018-01-24 01:26:00Z zhangyi $
"""
{
    "urls": {
        'aid': int,         # url id
        'ruleid': int,      # rule
        'status': int,      # status
        'name': str,        # name
        'email': str,       # email
        'password': str,    # password
        'createtime': int,  # create time
        'updatetime': int,  # last update time
        'updator': str,     # updator
    }
}

class AdminDB(object):

    ADMIN_RULE_NONE = 0
    ADMIN_RULE_ADMIN_MANAGER = 1
    ADMIN_RULE_PROJECT_MANAGER = 2
    ADMIN_RULE_SITE_MANAGER = 3

    ADMIN_RULE_MAP = {
        ADMIN_RULE_ADMIN_MANAGER: "管理员",
        ADMIN_RULE_PROJECT_MANAGER: "项目管理员",
        ADMIN_RULE_SITE_MANAGER: "站点管理员",
    }

    ADMIN_STATUS_INIT = 0
    ADMIN_STATUS_ACTIVE = 1
    ADMIN_STATUS_DISABLE = 2
    ADMIN_STATUS_DELETED = 3

    def get_detail(self, id):
        raise NotImplementedError

    def get_detail_by_email(self, email):
        raise NotImplementedError

    def insert(self, obj = {}):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def active(self, id):
        raise NotImplementedError

    def disable(self, id):
        raise NotImplementedError

    def delete(self, id):
        raise NotImplementedError

    def get_list(self, where = {}, select = None):
        raise NotImplementedError

    def verify_user(self, email, password):
        raise NotImplementedError
