#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-4 19:42:23
"""

from . import Base

{
    "parse_rule": {
        'prid': int,       # parse_rule id
        'domain': str,     # 域名
        'subdomain': str,  # 二级域名
        'name': str,       # 网站名称
        'parse': str,      # 解析规则
        'paging': str,     # 翻页规则
        'ctime': int,      # 创建时间
        'utime': int,      # 最后一次更新时间
        'creator': int,    # 创建人ID
        'updator': int,    # 最后一次更新人ID
    }
}

class ParseRuleDB(Base):
    """
    parse_rule data object
    """

    def insert(self, obj = {}):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def delete(self, id, where = {}):
        raise NotImplementedError

    def get_detail(self, id):
        raise NotImplementedError

    def get_detail_by_domain(self, domain):
        raise NotImplementedError

    def get_detail_by_subdomain(self, subdomain):
        raise NotImplementedError

    def get_list(self, where = {}, select = None):
        raise NotImplementedError
