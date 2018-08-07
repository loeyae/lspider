#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:40:43
"""

from . import Base

# site schema
{
    'sites': {
        'sid': int,          # site id
        'pid': int,          # project id
        'name': str,         # site name
        'url': str,          # 基础 url
        'status': int,       # site status
        'rate': int,         # 更新频率
        'comment': str,      # 站点描述
        'scripts': str,      # 自定义handler
        'type': str,         # 站点类型
        'main_process': str, # 主流程配置
        'sub_process': str,  # 子流程配置
        'unique': str,       # 生成unique id的配置
        'ctime': int,        # 创建时间
        'utime': int,        # 最后一次更新时间
        'creator': int,      # 创建人
        'updator': int,      # 最后一次更新人
    }
}

class SitesDB(Base):

    def insert(self, obj={}):
        raise NotImplementedError

    def disable(self, id, where):
        raise NotImplementedError

    def disable_by_project(self, pid, where):
        raise NotImplementedError

    def active(self, id, where):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def delete(self, id, where):
        raise NotImplementedError

    def delete_by_project(self, pid, where):
        raise NotImplementedError

    def get_detail(self, id):
        raise NotImplementedError

    def get_new_list(self, id, pid, where = {}, select=None, **kwargs):
        raise NotImplementedError

    def get_list(self, where = {}, select=None, **kwargs):
        raise NotImplementedError
