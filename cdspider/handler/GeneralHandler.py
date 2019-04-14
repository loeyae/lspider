# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-11-17 19:56:32
"""
from . import BaseHandler
from cdspider.parser.lib import LinksExtractor


class GeneralHandler(BaseHandler):
    """
    general handler
    """

    def route(self, handler_driver_name, rate, save):
        """
        route
        :param handler_driver_name:
        :param rate:
        :param save:
        :return:
        """
        yield None

    def init_process(self, save):
        """
        初始化抓取流程
        :return:
        """
        self.process = {
            "request": self.DEFAULT_PROCESS,
            "parse": None,
            "page": None
        }

    def run_parse(self, rule):
        """
        解析
        :param rule:
        :return:
        """
        extractor = LinksExtractor(url=self.response['url'])
        extractor.exctract(self.response['content'])
        self.response['parsed'] = extractor.infos['subdomains']

    def run_result(self, save):
        """
        结果处理
        :param save:
        :return:
        """
        if self.response['parsed']:
            for item in self.response['parsed']:
                print(item)
