#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:35:54
"""
from . import Base

# result schema
{
    'result': {
        'rid': str,        # result id
        'acid': str,       # unique str
        'url': str,        # 抓取的url
        'title': str,      # 标题
        'author': str,     # 作者
        'pubtime': int,    # 发布时间
        'subtitile': str,  # 二级标题
        'content': str,    # 详情
        'domain': str,     # 站点域名
        'subdomain': str,  # 站点二级域名
        'channel': str,    # 站点频道
        'result': str,     # 获取到的其他字段
        'media_type': str, # 媒体类型
        'ctime': int,      # 抓取时间
        'utime': int,      # 更新时间
    }
}

class ListRuleDB(Base):

    STATUS_PARSED = 1

    def insert(self, obj = {}):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def update_many(self,obj, where=None):
        raise NotImplementedError

    def get_detail(self, id):
        raise NotImplementedError

    def get_count(self, createtime, where = {}, select = None, **kwargs):
        raise NotImplementedError

    def get_list(self, createtime, where = {}, select = None, sort=[("pid", 1)], **kwargs):
        raise NotImplementedError