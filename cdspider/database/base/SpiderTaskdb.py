#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-11-21 10:00:32
"""
from . import Base

# spider_task
{
    'spider_task': {
        'uuid': int,         # ai id
        'mode': str,         # handler mode
        'pid': int,          # project id
        'sid': int,          # site id
        'tid': int,          # task id
        'uid': int,          # url id
        'kid': int,          # keyword id
        'parentid': str,     # parentid
        'url': str,          # url
        'status': int,       # site status
        'status': int,       # site status
        'expire': int,       # expire time
        'plantime': int,     # plan time
        'crawltime': int,    # crawl time
        'crawlinfo': dict,   # crawl info, ex: {""}
        'ctime': int,        # 创建时间
        'utime': int,        # 最后一次更新时间
    }
}

class SpiderTaskDB(Base):
    """
    spider task data object
    """
    def insert(self, obj={}):
        raise NotImplementedError

    def disable(self, id, mode, where):
        raise NotImplementedError

    def disable_by_pid(self, pid, mode, where):
        raise NotImplementedError

    def disable_by_sid(self, sid, mode, where):
        raise NotImplementedError

    def disable_by_tid(self, tid, mode, where):
        raise NotImplementedError

    def disable_by_kid(self, kid, mode, where):
        raise NotImplementedError

    def disable_by_url(self, uid, mode, where):
        raise NotImplementedError

    def active(self, id, mode, where):
        raise NotImplementedError

    def active_by_pid(self, pid, mode, where):
        raise NotImplementedError

    def active_by_sid(self, sid, mode, where):
        raise NotImplementedError

    def active_by_tid(self, tid, mode, where):
        raise NotImplementedError

    def active_by_kid(self, kid, mode, where):
        raise NotImplementedError

    def active_by_url(self, uid, mode, where):
        raise NotImplementedError

    def update(self, id, mode, obj={}):
        raise NotImplementedError

    def update_many(self, id, mode, obj = {}):
        raise NotImplementedError

    def delete(self, id, mode, where):
        raise NotImplementedError

    def delete_by_pid(self, pid, mode, where):
        raise NotImplementedError

    def delete_by_sid(self, sid, mode, where):
        raise NotImplementedError

    def delete_by_tid(self, tid, mode, where):
        raise NotImplementedError

    def delete_by_kid(self, kid, mode, where):
        raise NotImplementedError

    def delete_by_url(self, uid, mode, where):
        raise NotImplementedError

    def get_detail(self, id, mode, select=None):
        raise NotImplementedError

    def get_count(self, mode, where = {}):
        raise NotImplementedError

    def get_list(self, mode, where={}, select=None, **kwargs):
        raise NotImplementedError

    def get_plan_list(self, mode, id, plantime, where={}, select=None, **kwargs):
        raise NotImplementedError

    def get_max_id(self, mode, id, where = {}):
        raise NotImplementedError
