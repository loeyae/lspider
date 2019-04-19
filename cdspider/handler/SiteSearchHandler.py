# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-22 10:28:27
"""
from cdspider.handler import GeneralSearchHandler


class SiteSearchHandler(GeneralSearchHandler):
    """
    site search handler
    :property task 爬虫任务信息 {"mode": "search", "uuid": SpiderTask.list uuid}
                   当测试该handler，数据应为 {"mode": "search", "keyword": 关键词规则,
                   "authorListRule": 列表规则，参考列表规则}

    支持注册的插件:
        site-search_handler.mode_handle
            data参数为 {"save": save,"url": url}
    """

    def url_prepare(self, url):
        """
        获取真正的url
        """
        return url
