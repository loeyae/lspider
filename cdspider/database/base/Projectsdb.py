# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:34:12
"""
from . import Base

{
    'projects': {
        'uuid' : int,         # '自增id',
        'name' : str,         # '项目名称 | 唯一',
        'baseUrl' : str,      # '根域名 | 唯一',
        'addAuthor' : int,    # '添加人ID',
        'updated_at' : str,   # '更新时间',
        'status' : int,       # '状态(1:正常,0:冻结,-1:删除)',
    }
}

class ProjectsDB(Base):

    def get_detail(self, id):
        raise NotImplementedError

    def insert(self, obj={}):
        raise NotImplementedError

    def update(self, id, obj={}):
        raise NotImplementedError

    def active(self, id):
        raise NotImplementedError

    def disable(self, id):
        raise NotImplementedError

    def delete(self, id):
        raise NotImplementedError

    def get_list(self, where={}, select = None, **kwargs):
        raise NotImplementedError

    def get_count(self, where={}, select = None, **kwargs):
        raise NotImplementedError

    def get_new_list(self, where={}, select = None, **kwargs):
        raise NotImplementedError
