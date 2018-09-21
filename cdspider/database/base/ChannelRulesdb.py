#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-6-21 18:37:29
"""

from . import Base

{
    "rhannel_rules": {
        'crid': int,          # 频道任务ID
        'name': str,          # 名称
        'url': str,           # url
        'sid': int,           # 站点ID
        'status': int,        # 状态
        'rate': int,          # 更新频率
        'expire': int,        # 过期时间
        'process': str,       # 解析设置
        'unique': str,        # 唯一索引设置
        'ctime': int,         # 创建时间
        'utime': int,         # 最后一次更新时间
        'creator': int,       # 创建人ID
        'updator': int,       # 最后一次修改人ID
    }
}

class ChannelRulesDB(Base):
    """
    attachment database obejct
    """
    TYPE_IMPACT = 1
    TYPE_COMMENTS = 2

    def insert(self, obj = {}):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def update_many(self, id, obj = {}):
        raise NotImplementedError
    
    def delete(self, id, where):
        raise NotImplementedError

    def active(self, id, where):
        raise NotImplementedError

    def disable(self, id, where):
        raise NotImplementedError

    def get_detail(self, id):
        raise NotImplementedError

    def get_list(self, where = {}, select=None, **kwargs):
        raise NotImplementedError

    def get_list_sid(self, sid, where = {}, select=None, **kwargs):
        raise NotImplementedError

    def get_new_list_by_pid(self, id, pid, where = {}, select=None, **kwargs):
        raise NotImplementedError
