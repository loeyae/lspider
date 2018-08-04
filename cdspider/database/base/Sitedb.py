#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:40:43
:version: SVN: $Id: Sitedb.py 2119 2018-07-04 03:56:41Z zhangyi $
"""
# site schema
{
    'site': {
        'sid': int,          # site id
        'name': str,         # site name
        'status': int,       # site status
        'projectid': int,    # project id
        'rate': int,         # 更新频率
        'url': str,          # 基础 url
        'stid': int,         # sitetype id
        'script': str,       # 自定义handler
        'base_request': str, # 基础请求配置
        'main_process': str, # 主流程配置
        'sub_process': str,  # 子流程配置
        'identify': str,     # 生成unique id的配置
        'limb': int,         # 是否包含子集， 0 为否， 1为是
        'domain': str,       # 站点域名
        'createtime': int,   # 创建时间
        'updatetime': int,   # 最后一次更新时间
        'lastuid': int,      # 最后一次入队的 url id
        'lastkwid': int,     # 最后一次入队的 keyword id
        'creator': str,      # 创建人
        'updator': str,      # 最后一次更新人
    }
}

class SiteDB(object):

    SITE_STATUS_INIT = 0
    SITE_STATUS_ACTIVE = 1
    SITE_STATUS_DISABLE = 2
    SITE_STATUS_DELETED = 3

    SITE_LIMB_TRUE = 1
    SITE_LIMB_FALSE = 0

    def insert(self, obj={}):
        raise NotImplementedError

    def enable(self, id, where):
        raise NotImplementedError

    def enable_by_project(self, pid, where):
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

    def get_new_list(self, id, projectid):
        raise NotImplementedError

    def get_list(self, where = {}, select=None, **kwargs):
        raise NotImplementedError

    def get_max_id(self, projectid):
        raise NotImplementedError
