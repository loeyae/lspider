#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-22 10:28:27
"""
import re
import copy
import time
import traceback
import urllib.request
from . import GeneralSearchHandler, Loader
from urllib.parse import urljoin, urlparse, quote_plus
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.libs.constants import *
from cdspider.parser import ListParser
from cdspider.parser.lib import TimeParser


class WechatSearchHandler(GeneralSearchHandler):
    """
    general search handler
    :property task 爬虫任务信息 {"mode": "search", "uuid": SpiderTask.list uuid}
                   当测试该handler，数据应为 {"mode": "search", "keyword": 关键词规则, "authorListRule": 列表规则，参考列表规则}
    """

    def run_parse(self, rule):
        """
        根据解析规则解析源码，获取相应数据
        :param rule 解析规则
        :input self.response 爬虫结果 {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        :output self.response {"parsed": 解析结果}
        """
        parser = ListParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
        parsed = parser.parse()
        self.debug("%s parsed: %s" % (self.__class__.__name__, parsed))
        self.response['parsed'] = parsed

    def run_parse(self, rule):
        """
        根据解析规则解析源码，获取相应数据
        :param rule 解析规则
        :input self.response 爬虫结果 {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        :output self.response {"parsed": 解析结果}
        """
        parser = ListParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
        parsed = parser.parse()
        if not parsed:
            raise CDSpiderCrawlerForbidden()
        self.response['parsed'] = parsed

    def update_crawl_info(self, save):
        """
        构造文章数据的爬虫信息
        """
        crawlinfo = self.task.get('crawlinfo', {}) or {}
        self.crawl_info['crawl_end'] = int(time.time())
        crawlinfo[str(self.crawl_id)] = self.crawl_info
        crawlinfo_sorted = [(k, crawlinfo[k]) for k in sorted(crawlinfo.keys())]
        if len(crawlinfo_sorted) > self.CRAWL_INFO_LIMIT_COUNT:
            del crawlinfo_sorted[0]
        s = self.task.get("save")
        if not s:
            s = {}
        s.update(save)
        self.db['SpiderTaskDB'].update(self.task['uuid'], self.task['mode'], {"crawltime": self.crawl_id, "crawlinfo": dict(crawlinfo_sorted), "save": save})


    def run_result(self, save):
        """
        爬虫结果处理
        :param save 保存的上下文信息
        :input self.response {"parsed": 解析结果, "final_url": 请求的url}
        """
        self.crawl_info['crawl_urls'][str(self.page)] = self.response['last_url']
        self.crawl_info['crawl_count']['page'] += 1
        self.crawl_info['crawl_count']['total'] = len(self.response['parsed'])
        self.update_crawl_info(save)
        save['update_crawlinfo'] = True
        if self.response['parsed']:
            #格式化url
            item_save = {"base_url": self.response['last_url']}
            formated = self.build_url_by_rule(self.response['parsed'], self.response['final_url'])
            item_handler = Loader(self.ctx, task = self.build_item_task(self.response['last_url']), no_sync = self.no_sync).load()
            item_handler.init(item_save)
            for item in formated:
                try:
                    item_task = self.build_item_task(item['url'])
                    item_handler.run_next(item_task)
                    item_save['retry'] = 0
                    while True:
                        self.info('Item Spider crawl start')
                        item_handler.crawl(item_save)
                        if isinstance(item_handler.response['broken_exc'], CONTINUE_EXCEPTIONS):
                            item_handler.on_continue(item_handler.response['broken_exc'], item_save)
                            continue
                        elif item_handler.response['broken_exc']:
                            raise item_handler.response['broken_exc']
                        if not item_handler.response['last_source']:
                            raise CDSpiderCrawlerError('Item Spider crawl failed')
                        self.info("Item Spider crawl end, source: %s" % utils.remove_whitespace(item_handler.response["last_source"]))
                        self.info("Item Spider parse start")
                        item_handler.parse()
                        self.info("Item Spider parse end, result: %s" % str(item_handler.response["parsed"]))
                        self.info("Item Spider result start")
                        item_handler.on_result(save)
                        self.info("Item Spider result end")
                        break
                except Exception as e:
                    self.on_error(e, save)
            if item_handler:
                item_handler.finish(item_save)

    def url_prepare(self, url):
        return url

    def build_item_task(self, url):
        """
        生成详情抓取任务并入队
        """
        message = {
            'parent-mode': HANDLER_MODE_WECHAT_SEARCH,
            'mode': HANDLER_MODE_WECHAT_ITEM,
            'url': url,
            'crawlid': self.crawl_id,
            'stid': self.task['uuid'],
        }
        return message
#        self.queue['scheduler2spider'].put_nowait(message)

    def finish(self, save):
        """
        记录抓取日志
        """
        super(WechatSearchHandler, self).finish(save)
        _u = save.pop('update_crawlinfo', False)
        if not _u:
            self.update_crawl_info(save)
        else:
            s = self.task.get("save")
            if not s:
                s = {}
            s.update(save)
            self.db['SpiderTaskDB'].update(self.task['uuid'], self.task['mode'], {"crawltime": self.crawl_id, "save": s})
