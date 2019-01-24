#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:27:08
"""
import time
from cdspider.database.base import ArticlesDB
from cdspider.worker import BaseWorker
from cdspider.handler import GeneralItemHandler
from cdspider.libs import utils
from cdspider.libs.tools import load_cls
from cdspider.libs.constants import *
from cdspider.parser.lib.time_parser import Parser as TimeParser

class ResultWorker(BaseWorker):
    """
    结果处理
    """
    inqueue_key = QUEUE_NAME_SPIDER_TO_RESULT

    def match_mode(self, url):
        """
        匹配搜索结果详情页的mode
        """
        subdomain, domain = utils.domain_info(url)
        has_bbs = False
        if subdomain:
            '''
            优先获取子域名对应的规则
            '''
            parserule_list = self.db['ForumRuleDB'].get_list_by_subdomain(subdomain)
            for item in parserule_list:
                if  'urlPattern' in item and item['urlPattern']:
                    '''
                    如果规则中存在url匹配规则，则进行url匹配规则验证
                    '''
                    u = utils.preg(url, item['urlPattern'])
                    if u:
                        return HANDLER_MODE_BBS_ITEM
                has_bbs = True
            parserule_list = self.db['ParseRuleDB'].get_list_by_subdomain(subdomain)
            for item in parserule_list:
                if  'urlPattern' in item and item['urlPattern']:
                    '''
                    如果规则中存在url匹配规则，则进行url匹配规则验证
                    '''
                    u = utils.preg(url, item['urlPattern'])
                    if u:
                        return HANDLER_MODE_DEFAULT_ITEM
            if has_bbs:
                return HANDLER_MODE_BBS_ITEM
        else:
            '''
            获取域名对应的规则
            '''
            parserule_list = self.db['ForumRuleDB'].get_list_by_domain(domain)
            for item in parserule_list:
                if  'urlPattern' in item and item['urlPattern']:
                    '''
                    如果规则中存在url匹配规则，则进行url匹配规则验证
                    '''
                    u = utils.preg(url, item['urlPattern'])
                    if u:
                        return HANDLER_MODE_BBS_ITEM
                has_bbs = True
            parserule_list = self.db['ParseRuleDB'].get_list_by_domain(domain)
            for item in parserule_list:
                if  'urlPattern' in item and item['urlPattern']:
                    '''
                    如果规则中存在url匹配规则，则进行url匹配规则验证
                    '''
                    u = utils.preg(url, item['urlPattern'])
                    if u:
                        return HANDLER_MODE_DEFAULT_ITEM
            if has_bbs:
                return HANDLER_MODE_BBS_ITEM
        return HANDLER_MODE_DEFAULT_ITEM

    def on_result(self, message):
        self.debug("got message: %s" % message)
        if not 'rid' in message or not message['rid']:
            raise CDSpiderError("rid not in message")
        rid = message['rid']
        article = self.db['ArticlesDB'].get_detail(rid)
        if not article:
            raise CDSpiderDBDataNotFound("article: %s not exists" % rid)
        spider_cls = 'cdspider.spider.Spider'
        Spider = load_cls(self.ctx, None, spider_cls)
        spider = Spider(self.ctx, no_sync = True, handler=None, inqueue=False)
        task = {
            "rid": rid,
            "mode": self.match_mode(article['url']),
        }
        return_result = message.get('return_result', False)
        return spider.fetch(task=task, return_result=return_result)
