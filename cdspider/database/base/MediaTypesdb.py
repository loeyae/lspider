#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-23 20:58:14
"""
from . import Base

{
    "attach_data": {
        "name" : str,
        "domain" : str,
        "subdomain" : str,
        "mediaType" : int,
        "status" : int,
        "ctime" : int,
        "utime" : int,
        "creator" : int,
        "updator" : int
    }
}

class MediaTypesDB(object):
    """
    put you comment
    """
    def insert(self, obj = {}):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def delete(self, id, where = {}):
        raise NotImplementedError

    def get_detail_by_domain(self, domain):
        raise NotImplementedError

    def get_detail_by_subdomain(self, subdomain):
        raise NotImplementedError

    def get_list(self, where = {}, select = None):
        raise NotImplementedError
