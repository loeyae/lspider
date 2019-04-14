# -*- coding: utf-8 -*-

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
        'uuid' : int,          # '自增id',
        'pid' : int,           # '项目ID',
        'name' : str,          # '站点名称 | 唯一,MAX50',
        'url' : str,           # '域名 | 唯一,MAX300',
        'mediaType' : int,     # '媒体类型(int)',
        'type' : int,          # '类别(int)',
        'industry' : int,      # '行业(int)',
        'grade' : int,         # '级别(int)',
        'frequency' : int,     # '更新频次(int)',
        'territory' : int,     # '地域(int)',
        'province' : int,      # '省(关联地域表ID)',
        'city' : int,          # '市(关联地域表ID)',
        'coding' : int,        # '编码(int)',
        'ip' : int,            # 'IP',
        'pv' : int,            # 'PV',
        'icp' : int,           # '备案号',
        'alexaRanking' : int,  # 'Alexa排名',
        'channel' : int,       # '频道',
        'weight' : int,        # '权重',
        'advancedSetup' : {
                'url' : str,   # '模拟登录url地址',
                'post' : str,  # 'post信息',
                'header' : {
                        'Cookie' : str,     # 'cookie',
                        'User-Agent' : str, # 'UA',
                        'Host' : str,       # 'Host',
                    },
        },
        'addAuthor' : int,     # '添加人ID',
        'updated_at' : int,    # '更新时间',
        'status' : int,        # '状态(1:正常,0:冻结,-1:删除)',
    }
}

class SitesDB(Base):

    TYPE_GENERALE = '1';
    TYPE_SEARCH = '2';

    def insert(self, obj={}):
        raise NotImplementedError

    def disable(self, id, where):
        raise NotImplementedError

    def disable_by_project(self, pid, where):
        raise NotImplementedError

    def active(self, id, where):
        raise NotImplementedError

    def update(self, id, obj={}):
        raise NotImplementedError

    def update_many(self, id, obj={}):
        raise NotImplementedError

    def delete(self, id, where):
        raise NotImplementedError

    def delete_by_project(self, pid, where):
        raise NotImplementedError

    def get_detail(self, id):
        raise NotImplementedError

    def get_site(self, id):
        raise NotImplementedError

    def get_new_list(self, id, pid, where={}, select=None, **kwargs):
        raise NotImplementedError

    def get_list(self, where={}, select=None, **kwargs):
        raise NotImplementedError
