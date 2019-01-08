#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-4 21:43:09
"""
import time
from cdspider.libs.utils import base64encode, base64decode
from . import Base

{
    "crawl_log": {
        'uuid': int,        # log id
        'lid': str,         # log id
        'stid': int,        # task id
        'pid': int,         # project id
        'sid': int,         # site id
        'tid': int,         # task id
        'uid': int,         # url id
        'kid': int,         # keyword id
        'rid': int,         # rule id
        'mode': str,        # handler mode
        'crawl_urls': str,  # {page: request url, ...}
        'crawl_start': int, # crawl start time
        'crawl_end': int,   # crawl end time
        'total': int,       # 抓取到的数据总数
        'new_count': int,   # 抓取到的数据入库数'
        'repeat_count': int,# 抓取到的数据重复数
        'page': int,        # 抓取的页数
        'repeat_page': int, # 重复的页数
        'errid': int,       # 如果有错误，关联的错误日志ID
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

    @staticmethod
    def build_id(createtime, id):
        prefix = time.strftime("%Y%m", time.localtime(createtime))
        return base64encode("%s%d" % (prefix, id))

    @staticmethod
    def unbuild_id(rid):
        s = base64decode(rid)
        return s[0:6], s[6:]
