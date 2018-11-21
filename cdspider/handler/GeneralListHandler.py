#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-11-21 20:45:56
"""
from . import BaseHandler
from cdspider.libs.constants import *
from cdspider.parser.lib import ListParser

class GeneralListHandler(BaseHandler):
    """
    general list handler
    """

    def route(self, mode, save):
        if mode == ROUTER_MODE_PROJECT:
            save['id'] = 0
            for item in self.db['ProjectsDB'].get_new_list(save['id']):
                if item['uuid'] > save['id']:
                    save['id'] = item['uuid']
                yield item['uuid']


    def schedule(self, message, save):
        yield None

    def get_scripts(self):
        return None

    def init_process(self):
        self.process =  {
            "request": self.DEFAULT_PROCESS,
            "parse": None,
            "page": None
        }

    def run_parse(self, rule):
        parser = ListParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
        self.response['parsed'] = parser.parse()

    def run_result(self, save):
        if self.response['parsed']:
            for item in self.response['parsed']:
                print(item)
