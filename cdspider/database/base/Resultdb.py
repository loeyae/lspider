#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:35:54
:version: SVN: $Id: Resultdb.py 2186 2018-07-04 14:10:49Z zhangyi $
"""
import time
from cdspider.libs.utils import base64encode, base64decode

# result schema
{
    'result': {
        'rid': str,        # result id
        'unid': str,       # unique str
        'crawl_id': str,   # 抓取id, 与siteid一起标识同一站点的同一批次的结果
        'url': str,        # 抓取的url
        'status': int,     # 状态
        'sitetype': int,   # 站点类型
        'projectid': int,  # project id
        'siteid': int,     # site id
        'urlid': int,      # url id
        'atid': int,       # attachment id
        'kwid': int,       # url id
        'domain': str,     # 站点域名
        'title': str,      # 标题
        'author': str,     # 作者
        'created': int,    # 发布时间
        'summary': str,    # 摘要
        'content': str,    # 详情
        'crawlinfo': str,  # 抓取信息 [{"project":projectid,"task":taskid,"urls":urlid,"keywords":keywordid,"crawltime":crawltime},..]
        'source': str,     # 抓到的源码
        'result': str,     # 获取到的其他字段
        'createtime': int, # 更新时间
        'updatetime': int, # 更新时间
    }
}

class ResultDB(object):

    RESULT_STATUS_INIT = 0
    RESULT_STATUS_PARSED = 1
    RESULT_STATUS_DELETED = 2

    def insert(self, obj = {}):
        raise NotImplementedError

    def update(self, id, obj = {}):
        raise NotImplementedError

    def add_crwal_info(self, unid, createtime, crawlinfo):
        raise NotImplementedError

    def get_detail(self, id):
        raise NotImplementedError

    def get_detail_by_unid(self, unid, createtime):
        raise NotImplementedError

    def get_count(self, createtime, where = {}, select = None, **kwargs):
        raise NotImplementedError

    def get_list(self, createtime, where = {}, select = None, sort=[("pid", 1)], **kwargs):
        raise NotImplementedError

    def aggregate_by_day(self, createtime, where = {}):
        raise NotImplementedError

    @staticmethod
    def build_id(createtime, id):
        prefix = time.strftime("%Y%m", time.localtime(createtime))
        return base64encode("%s%d" % (prefix, id))

    @staticmethod
    def unbuild_id(rid):
        s = base64decode(rid)
        return s[0:6], s[6:]
