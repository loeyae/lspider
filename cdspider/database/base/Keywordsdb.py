# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:32:52
"""
from . import Base

{
    'keywords': {
        'uuid': int,        # keywords id
        'word': str,        # keyword
        'tid': int,         # 所属任务ID, 默认为0，全局所有
        'status': int,      # status
        'frequency': str,   # 关键词频率
        'expire': int,      # 过期时间
        'src': str,         # 来源
        'ctime': int,       # 创建时间
        'utime': int,       # 最后一次更新时间
    }
}

class KeywordsDB(Base):

    KEYWORDS_STATUS_INIT = 0
    KEYWORDS_STATUS_ACTIVE = 1
    KEYWORDS_STATUS_DISABLE = 2
    KEYWORDS_STATUS_DELETED = 3


    def insert(self, obj={}):
        raise NotImplementedError

    def update(self, id, obj={}):
        raise NotImplementedError

    def update_many(self, id, obj={}):
        raise NotImplementedError
    
    def active(self, id, where={}):
        raise NotImplementedError

    def disable(self, id, where = {}):
        raise NotImplementedError

    def delete(self, id, where = {}):
        raise NotImplementedError

    def get_detail(self, id):
        raise NotImplementedError

    def get_new_list(self, id, tid, select=None, **kwargs):
        raise NotImplementedError

    def get_list(self, where = {}, select=None, **kwargs):
        raise NotImplementedError

    def get_list_by_tid(self, tid, where = {}, select=None, **kwargs):
        raise NotImplementedError
