# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:41:28
"""
from . import Base

{
    'task': {
        'uuid' : int,                         # '自增id',
        'pid' : int,                          # '项目ID',
        'sid'  : int,                         # '站点ID',
        'type' : str,                         # '任务类别 int',
        'frequency' : str,                    # '更新频率'
        'name' : str,                         # '任务名称 | 唯一,MAX50',
        'desc' : str,                         # '任务描述'
        'ctime' : int,                        # '添加时间',
        'utime' : int,                        # '更新时间',
        'status' : int,                       # '状态(1:正常,0:冻结,-1:删除)',
    }
}

class TaskDB(Base):

    SEARCH_TYPE_ENGINE = '1'
    SEARCH_TYPE_SITE = '2'

    def insert(self, obj={}):
        raise NotImplementedError

    def disable(self, id, where):
        raise NotImplementedError

    def active(self, id, where):
        raise NotImplementedError

    def update(self, id, obj={}):
        raise NotImplementedError

    def update_many(self, id, obj={}):
        raise NotImplementedError

    def delete(self, id, where):
        raise NotImplementedError

    def get_detail(self, id, select=None):
        raise NotImplementedError

    def get_count(self, where={}):
        raise NotImplementedError

    def get_list(self, where={}, select=None, **kwargs):
        raise NotImplementedError

    def get_new_list(self, id, where={}, select=None, **kwargs):
        raise NotImplementedError
