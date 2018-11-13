#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:43:17
"""

from . import Base

{
    "urls": {
        'uid': int,           # url id
        'name': str,          # 名称
        'sid': int,           # site id
        'url': str,           # url
        'rate': int,          # int
        'status': int,        # status
        'main_process': str,  # 主流程配置
        'sub_process': str,   # 子流程配置
        'unique': str,        # 生成unique id的配置
        'ctime': int,         # create time
        'utime': int,         # last update time
        'creator': int,       # creator
        'updator': int,       # last update updator
    }
}

class UrlsDB(Base):

    def insert(self, obj = {}):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def update_many(self, id, obj = {}):
        raise NotImplementedError

    def enable(self, id, where):
        raise NotImplementedError

    def enable_by_site(self, sid, where):
        raise NotImplementedError

    def enable_by_project(self, pid, where):
        raise NotImplementedError

    def delete(self, id, where):
        raise NotImplementedError

    def delete_by_site(self, sid, where):
        raise NotImplementedError

    def delete_by_project(self, pid, where):
        raise NotImplementedError

    def active(self, id, where):
        raise NotImplementedError

    def disable(self, id, where):
        raise NotImplementedError

    def disable_by_site(self, sid, where):
        raise NotImplementedError

    def disable_by_project(self, pid, where):
        raise NotImplementedError

    def get_detail(self, id):
        raise NotImplementedError

    def get_list(self, where = {}, select=None, **kwargs):
        raise NotImplementedError

    def get_new_list(self, id, sid, where = {}, select=None, **kwargs):
        raise NotImplementedError

    def get_new_list_by_pid(self, id, pid, where = {}, select=None, **kwargs):
        raise NotImplementedError
