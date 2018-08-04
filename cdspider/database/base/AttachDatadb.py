#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-4 19:07:46
"""

from . import Base

{
    "attach_data": {
        'rid': str,              # result id
        'acid': str,             # 文章ID
        'views': int,            # 阅读数
        'like_num': int,         # 点赞数
        'reposts_num': int,      # 转发数
        'comments_num': int,     # 评论数
        'ctime': int,            # 创建时间
        'utime': int,            # 最后一次更新时间
    }
}

class AttachDataDB(Base):
    """
    attach_data data object
    """

    def insert(self, createtime, obj = {}):
        raise NotImplementedError

    def update(self, createtime, id, obj = {}):
        raise NotImplementedError

    def get_detail(self, createtime, id):
        raise NotImplementedError

    def get_detail_by_acid(self, acid, createtime):
        raise NotImplementedError

    def get_count(self, createtime, where = {}, select = None, **kwargs):
        raise NotImplementedError

    def get_list(self, createtime, where = {}, select = None, sort=[("pid", 1)], **kwargs):
        raise NotImplementedError

