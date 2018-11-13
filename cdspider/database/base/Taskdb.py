#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:41:28
"""
from . import Base

{
    'task': {
        'tid': int,        # task id
        'pid': int,        # project id
        'sid': int,        # site id
        'kwid': int,       # keyword id, if exists, default: 0
        'uid': int,        # url id, if exists, default: 0
        'aid': int,        # attachment id, if exists, default: 0
        'crid': int,       # channel rule id
        'url': str,        # base url
        'rate': int,       # 抓取频率
        'status': int,     # status, default: 0
        'expire': int,     # 过期时间, default: 0
        'save': str,       # 保留的参数
        'crawlinfo': str,  # 最近10次抓取记录
        'queuetime': int,  # 最近一次入队时间
        'crawltime': int,  # 最近一次抓取时间
        'plantime': int,   # 计划执行时间
        'ctime': int,      # 创建时间
        'utime': int,      # 最后一次修改时间
    }
}

class TaskDB(Base):

    def insert(self, obj={}):
        raise NotImplementedError

    def disable(self, id, pid, where):
        raise NotImplementedError

    def disable_by_project(self, pid, where):
        raise NotImplementedError

    def disable_by_site(self, sid, pid, where):
        raise NotImplementedError

    def disable_by_urls(self, uid, pid, where):
        raise NotImplementedError

    def disable_by_attachment(self, aid, pid, where):
        raise NotImplementedError

    def disable_by_keyword(self, kid, pid, where):
        raise NotImplementedError

    def active(self, id, pid, where):
        raise NotImplementedError

    def active_by_site(self, sid, pid, where):
        raise NotImplementedError

    def active_by_urls(self, uid, pid, where):
        raise NotImplementedError

    def active_by_attachment(self, aid, pid, where):
        raise NotImplementedError

    def active_by_keyword(self, kid, pid, where):
        raise NotImplementedError

    def update(self, id, pid, obj={}):
        raise NotImplementedError

    def update_many(self, id, obj = {}):
        raise NotImplementedError
    
    def delete(self, id, pid, where):
        raise NotImplementedError

    def delete_by_project(self, pid, where):
        raise NotImplementedError

    def delete_by_site(self, sid, pid, where):
        raise NotImplementedError

    def delete_by_urls(self, uid, pid, where):
        raise NotImplementedError

    def delete_by_attachment(self, aid, pid, where):
        raise NotImplementedError

    def delete_by_keyword(self, kid, pid, where):
        raise NotImplementedError

    def get_detail(self, id, pid, crawlinfo=False):
        raise NotImplementedError

    def get_count(self, pid, where = {}):
        raise NotImplementedError

    def get_list(self, pid, where={}, select=None, **kwargs):
        raise NotImplementedError

    def get_init_list(self, pid, where={}, select=None, **kwargs):
        raise NotImplementedError

    def get_plan_list(self, pid, plantime, where={}, select=None, **kwargs):
        raise NotImplementedError
