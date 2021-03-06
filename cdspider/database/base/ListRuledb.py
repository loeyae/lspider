# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:35:54
"""
from . import Base

# listRule schema
{
    'listRule': {
        'uuid' : int,                   # '主键ID | 唯一',
        'preid': int,                   # 前置处理规则，默认为0
        'type': int,                    # 0默认列表处理规则，1前置处理规则
        'name' : str,                   # '规则名称',
        'domain' : str,                 # '域名',
        'subdomain' : str,              # '子域名',
        'mediaType' : int,              # '媒体类型',
        'frequency': int,               # '抓取频率'
        'request' : {
            'proxy' : int,              # '是否使用代理(auto:自动;ever:强制;never:从不)',
            'data' : str,               # '参数',
            'cookie' : str,             # 'cookie',
            'header' : str,             # 'header',
            'method' : str,             # '请求方式(GET:GET;POST:POST)',
        },
        'paging' : {
            'url' : {
                "type": str,            # 'url匹配类型'
                "filter": str,          # 'url匹配模式'
            },
            'max': int,                 # '最大翻页数'
            'first': bool,              # '首页是否匹配'
            'rule' : [{
                'type': str,            # '参数类型'
                'mode' : str,           # '参数模式',
                'name' : str,           # '关键词名',
                'value' : int,          # '初始值',
                'step' : int,           # '自增步长',
                'patch': str,           # '补全规则'
            }],
        },
        'parse' : {
            'filter' : str,             #'列表页识别规则',
            'item' : {
                'title' : {
                    'filter' : str,     # '标题识别规则'
                },
                'url' : {
                    'filter' : str,     # 'URL识别规则',
                    'patch' : str,      # 'url补全规则',
                },
                'author' : {
                    'filter' : str,     # '作者识别规则',
                    'patch' : str,      # '作者提取规则',
                },
                'pubtime' : {
                    'filter' : str,     # '发布时间识别规则',
                    'patch' : str,      # '发布时间提取规则',
                }
            },
        },
        'unique' : {
            'url' : str,                # 'url匹配规则',
            'query' : str,              # 'query参数',
            'data' : str,               # '匹配到的数据',
        },
        'scripts' : str,                # '自定义脚本',
        'ctime' : int,                  # '创建时间',
        'utime' : int,                  # '更新时间',
        'status' : int,                 # '状态(1:正常,0:冻结,-1:删除)',
    }
}

class ListRuleDB(Base):

    RULE_TYPE_DEFAULT = 0
    RULE_TYPE_PREPARE = 1

    def insert(self, obj={}):
        raise NotImplementedError

    def update(self, id, obj={}):
        raise NotImplementedError

    def active(self, id, where = {}):
        raise NotImplementedError

    def enable(self, id, where = {}):
        raise NotImplementedError

    def disable(self, id, where = {}):
        raise NotImplementedError

    def delete(self, id, where = {}):
        raise NotImplementedError

    def update_many(self,obj, where=None):
        raise NotImplementedError

    def get_detail(self, id):
        raise NotImplementedError

    def get_count(self, createtime, where={}, select = None, **kwargs):
        raise NotImplementedError

    def get_list(self, createtime, where={}, select = None, sort=[("pid", 1)], **kwargs):
        raise NotImplementedError
