#-*- coding: utf-8 -*-
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

    def route(self, mode, rate, save):
        pass

    def init_process(self):
        self.process =  {
            "request": self.DEFAULT_PROCESS,
            "parse": None,
            "page": None
        }

    def run_parse(self, rule):
        extractor = LinksExtractor(url=self.response['final_url'])
        extractor.exctract(self.response['last_source'])
        self.response['parsed'] = extractor.infos['subdomains']

    def run_result(self, save):
        if self.response['parsed']:
            for item in self.response['parsed']:
                print(item)
