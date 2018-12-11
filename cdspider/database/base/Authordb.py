#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:43:17
"""

from . import Base

{
    "author": {
        "sid" : int,                    # site id,
        "tid" : int,                    # task id,
        "name" : int,                   # name,
        "parameters" : {                # 参数
            "hard" : [                  # 固定参数
                {
                    "mode" : str,       # 参数模式 get|post|format|replace
                    "name" : str,       # 参数键名
                    "value" : str,      # 参数值
                }
            ],
            "randoms" : [               # 随机参数
                {
                    "mode" : str,       # 参数模式 get|post|format|replace
                    "prefix" : str,     # 前缀
                    "suffix" : str,     # 后缀
                    "name" : str,       # 参数键名
                    "value" : str,      # 随机数模式 ms:带毫秒数的时间戳，ms_:带毫秒的时间戳，小数点被替换为_,s:时间戳，数字:长度为该数字的随机数
                }
            ]
        },
        "frequency" : int,              # 更新频率
        "uuid" : int,                   # AI id
        "addAuthor" : int,              # 添加人
        "status" : int,                 # 状态
        "ctime" : int,                  # 添加时间
        "updated_at" : str,             # 修改时间
    }
}

class AuthorDB(Base):

    def insert(self, obj = {}):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def update_many(self, id, obj = {}):
        raise NotImplementedError

    def enable(self, id, where):
        raise NotImplementedError

    def enable_by_site(self, sid, where):
        raise NotImplementedError

    def enable_by_project(self, pid, where):
        raise NotImplementedError

    def delete(self, id, where):
        raise NotImplementedError

    def delete_by_site(self, sid, where):
        raise NotImplementedError

    def delete_by_project(self, pid, where):
        raise NotImplementedError

    def active(self, id, where):
        raise NotImplementedError

    def disable(self, id, where):
        raise NotImplementedError

    def disable_by_site(self, sid, where):
        raise NotImplementedError

    def disable_by_project(self, pid, where):
        raise NotImplementedError

    def get_detail(self, id):
        raise NotImplementedError

    def get_list(self, where = {}, select=None, **kwargs):
        raise NotImplementedError

    def get_new_list(self, id, sid, where = {}, select=None, **kwargs):
        raise NotImplementedError

    def get_new_list_by_pid(self, id, pid, where = {}, select=None, **kwargs):
        raise NotImplementedError
