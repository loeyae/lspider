#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-12 20:42:23
"""

from . import Base

{
    "forumRule": {
        "name" : str,                       # 规则名
        "domain" : str,                     # 域名
        "subdomain" : str,                  # 子域名
        "rate" : str,                       # 更新频率
        "mediaType" : str,                  # 媒体类型
        "expire" : int,                     # 过期时间
        "scripts" : str,                    # 自定义脚本
        "status" : int,                     # 状态
        "preparse" : {
            "url" : {
                "mode" : "get",
                "base" : None
            },
            "parse" : [
                {
                    "key" : None,
                    "filter" : None,
                    "patch" : None
                }
            ]
        },
        "unique" : {
            "data" : None
        },
        "request" : {
            "proxy" : "auto",
            "method" : "get",
            "cookie" : None,
            "header" : None,
            "data" : None
        },
        "paging" : {
            "pattern" : 1,
            "pageUrl" : "base_url",
            "rule" : [
                {
                    "method" : None,
                    "word" : None,
                    "value" : None,
                    "step" : None,
                    "max" : None,
                    "first" : "1"
                }
            ]
        },
        "parse" : {
            "one" : {
                "title" : {
                    "filter" : None,
                    "extract" : None
                },
                "author" : {
                    "filter" : None,
                    "extract" : None
                },
                "pubtime" : {
                    "filter" : None,
                    "extract" : None
                },
                "content" : {
                    "filter" : None
                },
                "channel" : {
                    "filter" : None,
                    "extract" : None
                }
            },
            "filter" : None,
            "item" : {
                "id" : {
                    "filter" : None,
                    "extract" : None
                },
                "content" : {
                    "filter" : None,
                    "extract" : None
                },
                "author" : {
                    "filter" : None,
                    "extract" : None
                },
                "pubtime" : {
                    "filter" : None,
                    "extract" : None
                }
            }
        },
        "date" : "201812",
        "uuid" : 4,
    }
}

class ParseRuleDB(Base):
    """
    parse_rule data object
    """

    def insert(self, obj = {}):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def delete(self, id, where = {}):
        raise NotImplementedError

    def get_detail(self, id):
        raise NotImplementedError

    def get_detail_by_domain(self, domain):
        raise NotImplementedError

    def get_detail_by_subdomain(self, subdomain):
        raise NotImplementedError

    def get_list(self, where = {}, select = None):
        raise NotImplementedError
