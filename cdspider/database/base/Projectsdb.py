#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:34:12
:version: SVN: $Id: Projectdb.py 1357 2018-06-21 10:41:16Z zhangyi $
"""

{
    'projects': {
        'pid': int,           # 项目id
        'name': str,          # 项目名称
        'status': int,        # 项目状态
        'scripts': str,       # 自定义handler
        'comments': str,      # 项目描述       
        'ctime': int,         # 创建时间
        'utime': int,         # 最后一次更新时间
        'creator': int,       # 创建人id
        'updator': int,       # 最后一次更新人id
    }
}

from . import Base

class ProjectsDB(Base):

    def get_detail(self, id):
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
    
    def get_count(self, where = {}, select = None):
        raise NotImplementedError
    
    def get_list_c(self, where = {}, select = None):
        raise NotImplementedError
