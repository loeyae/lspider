#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-4 19:16:35
"""
from . import Base

{
    "comments": {
        'rid': str,         # result id
        'unid': str,        # 评论唯一id
        'acid': str,        # 文章ID
        'id': str,          # 评论ID
        'author': str,      # 评论作者
        'pubtime': int,     # 发表时间
        'comment': str,     # 评论内容
        'parentid': str,    # 父级评论ID
        'ctime': int,       # 创建时间
        'utime': int,       # 最后一次更新时间
    }
}

class CommentsDB(Base):
    """
    comments data object
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
