#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-2 15:16:34
"""
import copy
import time
from . import BaseHandler
from urllib.parse import urljoin, urlparse, urlunparse
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.libs.constants import *
from cdspider.parser import CustomParser

class CommentHandler(BaseHandler):
    """
    comment handler
    """
    def get_scripts(self):
        rule = self.match_rule()
        return rule.get("scripts", None)

    def match_rule(self):
        parse_rule = self.task.get("commentRule", {})
        return parse_rule

    def schedule(self, message, save):
        mode = message['mode']
        if not 'id' in save:
            save['id'] = 0
        if mode == ROUTER_MODE_PROJECT:
            if not 'sid' in save:
                save['sid'] = 0
            for item in self.db['SitesDB'].get_new_list(save['sid'], message['item']):
                self.debug("%s schedule site: %s" % (self.__class__.__name__, str(item)))
                while True:
                    has_item = False
                    for each in self.schedule_by_site(item, message['h-mode'], save):
                        yield each
                        has_item = True
                    if not has_item:
                        self.debug("%s schedule site end" % (self.__class__.__name__))
                        break
                if item['uuid'] > save['sid']:
                    save['sid'] = item['uuid']
        elif mode == ROUTER_MODE_SITE:
            site = self.db['SitesDB'].get_detail(message['item'])
            for each in self.schedule_by_site(site, message['h-mode'], save):
                yield each

    def route(self, mode, save):
        if not "id" in save:
            save["id"] = 0
        if mode == ROUTER_MODE_PROJECT:
            for item in self.db['ProjectsDB'].get_new_list(save['id'], select=["uuid"]):
                if item['uuid'] > save['id']:
                    save['id'] = item["uuid"]
                yield item['uuid']
        elif mode == ROUTER_MODE_SITE:
            if not "pid" in save:
                save["pid"] = 0
            for item in self.db['ProjectsDB'].get_new_list(save['pid'], select=["uuid"]):
                while True:
                    has_item = False
                    for each in self.db['SitesDB'].get_new_list(save['id'], item['uuid'], select=["uuid"]):
                        has_item = True
                        if each['uuid'] > save['id']:
                            save['id'] = each['uuid']
                        yield each['uuid']
                    if not has_item:
                        break
                if item['uuid'] > save['pid']:
                    save['pid'] = item['uuid']

    # 采集数据
    def run_parse(self, rule):
        parser = CustomParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
        self.response['parsed'] = parser.parse()

    # 保存结果
    def run_result(self, save):
        print('result')
        if self.response['parsed']:
            typeinfo = self._typeinfo(self.response['final_url'])
            self.result2db(save, typeinfo)





