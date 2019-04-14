# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:43:17
"""

from . import Base

{
    "urls": {
        'uuid' : int,             # '自增id',
        'pid' : int,              # '项目UUID',
        'sid' : int,              # '站点UUID',
        'typeChannelList' : int,  # '标记类型 频道列表页 是:1 否:0'
        'typeChannel' : int,      # '标记类型 频道页 是:1 否:0',
        'typeList' : int,         # '标记类型 列表页 是:1 否:0',
        'typeDetail' : int,       # '标记类型 文章页 是:1 否:0',
        'typeOther' : int,        # '标记类型 其它 是:1 否:0',
        'url' : str,              # '链接',
        'dataNum' : int,          # '内容条数',
        'addAuthor' : int,        # '添加人ID',
        'updated_at' : int,       # '更新时间',
        'status' : int,           # '状态(1:已处理,0:未处理,-1:删除)',
        'ruleId' : int,           # '该url对应规则的UUid',
        'ruleStatus' : int,       # '规则状态(1:全验证通过 0:未通过)',
    }
}


class UrlsDB(Base):

    IS_TYPE_CHANNEL_LIST = 1
    NO_TYPE_CHANNEL_LIST = 0
    IS_TYPE_CHANNEL = 1
    NO_TYPE_CHANNEL = 0
    IS_TYPE_LIST = 1
    NO_TYPE_LIST = 0

    def insert(self, obj={}):
        raise NotImplementedError

    def update(self, id, obj={}):
        raise NotImplementedError

    def update_many(self, id, obj={}):
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

    def get_list(self, where={}, select=None, **kwargs):
        raise NotImplementedError

    def get_new_list(self, id, where={}, select=None, **kwargs):
        raise NotImplementedError

    def get_new_list_by_pid(self, id, pid, where={}, select=None, **kwargs):
        raise NotImplementedError
