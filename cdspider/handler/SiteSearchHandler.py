# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-22 10:28:27
"""
import copy
from . import GeneralSearchHandler
from cdspider.parser import ListParser


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

    ns = "site-search_handler"

    def run_parse(self, rule):
        """
        根据解析规则解析源码，获取相应数据
        :param rule 解析规则
        :input self.response 爬虫结果 {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        :output self.response {"parsed": 解析结果}
        """
        parser = ListParser(source=self.response['content'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
        parsed = parser.parse()
        self.debug("%s parsed: %s" % (self.__class__.__name__, parsed))
        if parsed:
            self.response['parsed'] = self.build_url_by_rule(parsed, self.response['final_url'])

    def url_prepare(self, url):
        """
        获取真正的url
        """
        return url
