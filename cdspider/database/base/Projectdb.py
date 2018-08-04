#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:34:12
:version: SVN: $Id: Projectdb.py 1357 2018-06-21 10:41:16Z zhangyi $
"""

{
    'project': {
        'pid': int,           # 项目id
        'type': int,          # 项目类型
        'title': str,         # 项目标题
        'status': int,        # 项目状态
        'script': str,        # 自定义handler
        'base_request': str,  # 基础请求配置
        'main_process': str,  # 主流程配置
        'sub_process': str,   # 子流程配置
        'custom_columns': str,# 自定义字段
        'identify': str,      # 生成unique id的配置
        'comments': str,      # 项目描述
        'rate': int,          # 更新频率
        'lastsid': int,       # 最后入队的 site id
        'createtime': int,    # 创建时间
        'updatetime': int,    # 最后一次更新时间
        'creator': str,       # 创建人
        'updator': str,       # 最后一次更新人
    }
}

class ProjectDB(object):

    PROJECT_TYPE_GENERAL = 1
    PROJECT_TYPE_SEARCH = 2
    PROJECT_TYPE_ATTACHE = 3

    PROJECT_TYPE_MAP = {
        PROJECT_TYPE_GENERAL: "general",
        PROJECT_TYPE_SEARCH: "search",
        PROJECT_TYPE_ATTACHE: "attache",
    }

    PROJECT_STATUS_INIT = 0
    PROJECT_STATUS_ACTIVE = 1
    PROJECT_STATUS_DISABLE = 2
    PROJECT_STATUS_DELETED = 3

    def get_detail(self, id):
        raise NotImplementedError

    def insert(self, obj = {}):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def enable(self, id):
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
