#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:35:54
"""
from . import Base

# AuthorListRule schema
{
    'AuthorListRule': {
        "tid" : int,                    # task id
        "sid" : int,                    # site id
        "status" : int,                 # status
        "name" : str,                   # name
        "baseUrl" : str,                # base url
        "request" : {                   # request rule
            "proxy" : str,              # proxy mode: auto|ever|never
            "method" : str,             # requet mothod: get|post
            "data" : str,               # data
            "cookie" : str,             # cookie
            "header" : str,             # headers
        },
        "paging" : {                    # paging rule
            "pattern" : int,            # paging type
            "pageUrl" : str,            # paging url
            "rule" : [                  # paging rule
                {
                    "word" : str,
                    "value" : int,
                    "step" : int,
                    "max" : int,
                    "method" : str,
                    "first" : int
                }
            ],
        },
        "parse" : {
            "filter" : str,
            "item" : {
                "title" : {
                    "filter" : str
                },
                "url" : {
                    "filter" : str,
                    "patch" : str
                },
                "author" : {
                    "filter" : str,
                    "extract" : str
                },
                "pubtime" : {
                    "filter" : str,
                    "extract" : str
                },
                "comment" : {
                    "filter" : str,
                    "extract" : str
                },
                "praise" : {
                    "filter" : str,
                    "extract" : str
                },
                "view" : {
                    "filter" : str,
                    "extract" : str
                },
                "repost" : {
                    "filter" : str,
                    "extract" : str
                }
            }
        },
        "scripts" : str,
        "uuid" : int,
        "addAuthor" : str,
        "updated_at" : str
    }
}

class AuthorListRuleDB(Base):

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
