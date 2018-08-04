#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-13 12:46:01
:version: SVN: $Id: Sitetypedb.py 188 2018-01-13 07:18:52Z zhangyi $
"""
# site schema
{
    'site': {
        'stid': int,       # site type id
        'type': str,       # 网站类型
        'status': int,     # 状态
        'domain': str,     # 站点域名
        'subdomain': str,  # 子域名
        'createtime': int, # 创建时间
        'updatetime': int, # 最后一次更新时间
        'creator': str,    # 创建人
        'updator': str,    # 最后一次更新人
    }
}

class SitetypeDB():

    SITETYPE_STATUS_NORMAl = 0
    SITETYPE_STATUS_DELETED = 4

    SITETYPE_TYPE_NO_MATCH = '其他'

    def insert(self, obj={}):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def delete(self, id):
        raise NotImplementedError

    def get_detail(self, id):
        raise NotImplementedError

    def get_detail_by_domain(self, domain, subdomain):
        raise NotImplementedError

    def get_list(self, where = {}, select=None, **kwargs):
        raise NotImplementedError