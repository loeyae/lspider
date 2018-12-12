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
                "base" : null
            },
            "parse" : [
                {
                    "key" : null,
                    "filter" : null,
                    "patch" : null
                }
            ]
        },
        "unique" : {
            "data" : null
        },
        "request" : {
            "proxy" : "auto",
            "method" : "get",
            "cookie" : null,
            "header" : null,
            "data" : null
        },
        "paging" : {
            "pattern" : 1,
            "pageUrl" : "base_url",
            "rule" : [
                {
                    "method" : null,
                    "word" : null,
                    "value" : null,
                    "step" : null,
                    "max" : null,
                    "first" : "1"
                }
            ]
        },
        "parse" : {
            "one" : {
                "title" : {
                    "filter" : null,
                    "extract" : null
                },
                "author" : {
                    "filter" : null,
                    "extract" : null
                },
                "pubtime" : {
                    "filter" : null,
                    "extract" : null
                },
                "content" : {
                    "filter" : null
                },
                "channel" : {
                    "filter" : null,
                    "extract" : null
                }
            },
            "filter" : null,
            "item" : {
                "id" : {
                    "filter" : null,
                    "extract" : null
                },
                "content" : {
                    "filter" : null,
                    "extract" : null
                },
                "author" : {
                    "filter" : null,
                    "extract" : null
                },
                "pubtime" : {
                    "filter" : null,
                    "extract" : null
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
