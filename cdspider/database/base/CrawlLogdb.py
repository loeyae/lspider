#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-4 21:43:09
"""

from . import Base

{
    "crawl_log": {
        'uuid': int,      # log id
        'machine': str, # matchine info
        'process': str, # process info
        'message': str, # error message
        'ctime': int,   # create time
        'tid': int,     # task id
    }
}

class CrawlLogDB(Base):
    """
    craw_log data object
    """

    def insert(self, obj = {}):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def delete(self, id):
        raise NotImplementedError
