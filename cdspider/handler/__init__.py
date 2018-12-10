#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
import os
import abc
import re
import six
import time
import logging
import traceback
import copy
from urllib.parse import urljoin, urlparse, urlunparse
from cdspider import Component
from cdspider.crawler import BaseCrawler
from cdspider.crawler import RequestsCrawler, SeleniumCrawler
from cdspider.database.base import Base as BaseDB
from cdspider.libs import utils
from cdspider.exceptions import *
from cdspider.libs.tools import *
from cdspider.parser import *
from cdspider.libs.url_builder import UrlBuilder
from cdspider.libs.constants import *

@six.add_metaclass(abc.ABCMeta)
class BaseHandler(Component):
    """
    handler基类
    """
    BLOOMFILTER_KEY = '%(prefix)s_cdspider_$(project)s_%(key)s'
    CRAWL_INFO_LIMIT_COUNT = 10
    EXPIRE_STEP = 1
    DEFAULT_RATE = 4

    DEFAULT_PROCESS = {
        "request": {
            "crawler": "tornado",
            "method": "GET",
            "proxy": "auto",
        }
    }

    MAX_RETRY = 5

    def __init__(self, context, task, **kwargs):
        """
        init
        """
        self.ctx = context
        self.task = task or {}
        g = context.obj
        self.proxy = g.get("proxy", None)
        self.logger = kwargs.pop('logger', logging.getLogger('handler'))
        self.log_level = logging.WARN
        if g.get('debug', False):
            self.log_level = logging.DEBUG
        super(BaseHandler, self).__init__(self.logger, self.log_level)
        self.db = g.get('db', None)
        self.queue = g.get('queue', None)
        self.ratemap = g.get('app_config', {}).get('ratemap', {})
        self.testing_mode = g.get('testing_mode', False)
        if self.task:
            attach_storage = g.get('app_config', {}).get('attach_storage', None)
            if attach_storage:
                attach_storage = os.path.realpath(os.path.join(g.get("app_path"), attach_storage))
            self.attach_storage = attach_storage
            self.crawl_id = int(time.time())
            self.crawl_info  = {
                "crawl_start": self.crawl_id,
                "crawl_end": None,
                "crawl_urls": {},
                "crawl_count": {
                    "total": 0,
                    "new_count": 0,
                    "repeat_count": 0,
                    "page": 0,
                    "repeat_page": 0,
                },
                "traceback": None
            }
            self.no_sync = kwargs.pop('no_sync', False)
            self.spider = kwargs.pop('spider', None)
            self._settings = kwargs or {}
            self.crawler = None
            self.process = {}
            self.proxy_mode = 'never'
            self.request = {}
            self.request_params = {}
            self.response = {
                "source": None,
                "last_source": None,
                "final_url": self.task.get('url', None),
                "last_url": self.task.get('url', None),
                "broken_exc": None,
                "parsed": None,
            }
            self.force_proxy = False
            self.auto_proxy = False
            self.page = 1
            self.last_result_id = None
        self.handle = {}

    def __del__(self):
        self.close()

    def route(self, mode, save):
        yield None

    def schedule(self, message, save):
        yield None

    def newtask(self, message):
        pass

    def status(self, message):
        mode = message['mode']
        status = message['status']
        if status == BaseDB.STATUS_DELETED:
            if "uid" in message and message['uid']:
                self.db['SpiderTaskDB'].delete_by_url(message['uid'], mode)
            elif "tid" in message and message['tid']:
                self.db['SpiderTaskDB'].delete_by_tid(message['tid'], mode)
            elif "sid" in message and message['sid']:
                self.db['SpiderTaskDB'].delete_by_sid(message['sid'], mode)
            elif "pid" in message and message['pid']:
                self.db['SpiderTaskDB'].delete_by_pid(message['pid'], mode)
            elif "kid" in message and message['kid']:
                self.db['SpiderTaskDB'].delete_by_kid(message['kid'], mode)
        elif status == BaseDB.STATUS_ACTIVE:
            if "uid" in message and message['uid']:
                self.db['SpiderTaskDB'].active_by_url(message['uid'], mode)
            elif "tid" in message and message['tid']:
                self.db['SpiderTaskDB'].active_by_tid(message['tid'], mode)
            elif "sid" in message and message['sid']:
                self.db['SpiderTaskDB'].active_by_sid(message['sid'], mode)
            elif "pid" in message and message['pid']:
                self.db['SpiderTaskDB'].active_by_pid(message['pid'], mode)
            elif "kid" in message and message['kid']:
                self.db['SpiderTaskDB'].active_by_kid(message['kid'], mode)
        elif status == BaseDB.STATUS_INIT:
            if "uid" in message and message['uid']:
                self.db['SpiderTaskDB'].disable_by_url(message['uid'], mode)
            elif "tid" in message and message['tid']:
                self.db['SpiderTaskDB'].disable_by_tid(message['tid'], mode)
            elif "sid" in message and message['sid']:
                self.db['SpiderTaskDB'].disable_by_sid(message['sid'], mode)
            elif "pid" in message and message['pid']:
                self.db['SpiderTaskDB'].disable_by_pid(message['pid'], mode)
            elif "kid" in message and message['kid']:
                self.db['SpiderTaskDB'].disable_by_kid(message['kid'], mode)

    def get_scripts(self):
        return None

    def handler_register(self, handle_type, priority = 1000):
        """
        handler register
        :param handle_type any of cdspider.libs.constants.(HANDLER_FUN_INIT, HANDLER_FUN_PROCESS, HANDLER_FUN_PREPARE, HANDLER_FUN_PRECRAWL, HANDLER_FUN_CRAWL, HANDLER_FUN_POSTCRAWL, HANDLER_FUN_PREPARSE, HANDLER_FUN_PARSE, HANDLER_FUN_POSTPARSE, HANDLER_FUN_RESULT, HANDLER_FUN_NEXT, HANDLER_FUN_CONTINUE, HANDLER_FUN_REPETITION, HANDLER_FUN_ERROR, HANDLER_FUN_FINISH)
        :param priority 数值越大，优先级越高
        """
        if not (isinstance(handle_type, list) or isinstance(handle_type, tuple)):
            handle_type = [handle_type]
        def _handler_register(fn):
            for _type in handle_type:
                if not _type in self.handle:
                    self.handle[_type] = []
                self.handle[_type].append((priority, fn))
            return fn
        return _handler_register

    def handler_run(self, handle_type, save):
        if not (isinstance(handle_type, list) or isinstance(handle_type, tuple)):
            handle_type = [handle_type]
        for _type in handle_type:
            func_list = self.handle.get(_type, None)
            if func_list:
                for _,fn in sorted(func_list, reverse=True):
                    fn(save)

    @property
    def current_page(self):
        return self.page

    def get_crawler(self, rule):
        """
        load crawler
        """
        crawler = rule.get('crawler', '') or 'requests'
        return utils.load_crawler(crawler, headers=rule.get('header', None), cookies=rule.get('cookie', None), proxy=rule.get('proxy'), log_level=self.log_level)

    def init(self, save):
        """
        初始化爬虫
        """
        if "uuid" in self.task and self.task['uuid']:
            task = self.db['SpiderTaskDB'].get_detail(self.task['uuid'], self.task['mode'])
            self.task.update(task)
        self.init_process(save)
        if not save['base_url']:
            save['base_url'] = self.task['url']
        self.handler_run(HANDLER_FUN_PROCESS, self.process)
        self.request = self._get_request()
        self.proxy_mode = self.request.pop('proxy', 'never')
        if not self.crawler:
            self.crawler = self.get_crawler(self.request)
        self.request['url'] = self.task.get('url')
        request = copy.deepcopy(self.request)
        if 'paging' in save and save['paging']:
            rule = self.process.get("paging")
            self.debug("%s paging rule: %s" % (self.__class__.__name__, rule))
            rule = self.format_paging(rule)
            request.update(rule)
        builder = UrlBuilder(self.logger, self.log_level)
        self.request_params = builder.build(request, self.response['last_source'], self.crawler, save)
        self.handler_run(HANDLER_FUN_INIT, self.request_params)

    @abc.abstractmethod
    def init_process(self, save):
        pass

    def prepare(self, save):
        """
        预处理
        """
        self.handler_run(HANDLER_FUN_PREPARE, {"request": self.request, "request_params": self.request_params, "save": save})

    def precrawl(self):
        if isinstance(self.crawler, SeleniumCrawler) and self.request_params['method'].upper() == 'GET':
            self.request_params['method'] = 'open'
        if (self.proxy_mode == PROXY_TYPE_EVER or self.force_proxy or (self.proxy_mode == PROXY_TYPE_AUTO and self.auto_proxy)) and self.proxy:
            self.request_params['proxy'] = copy.deepcopy(self.proxy)
        else:
            self.request_params['proxy'] = None
        self.handler_run(HANDLER_FUN_PRECRAWL, self.request_params)

    def crawl(self, save):
        """
        数据抓取
        :param: save 保持的上下文
        :param: crawler 是否更新crawler对象
        """
        try:
            self.precrawl()
            params = copy.deepcopy(self.request_params)
            if HANDLER_FUN_CRAWL in self.handle:
                self.handler_run(HANDLER_FUN_CRAWL, {"params": params, "response": self.response, "save": save})
            else:
                self.crawler.crawl(**params)
                self.response['last_source'] = self.crawler.page_source
                self.response['last_url'] = self.crawler.final_url
                save['request_url'] = self.crawler.final_url
            if self.page == 1:
                self.response['final_url'] = self.response['last_url']
                self.response['source'] = self.response['last_source']
        except Exception as exc:
            self.response['broken_exc'] = exc
        finally:
            self.handler_run(HANDLER_FUN_POSTCRAWL, {"reponse": self.response, "save": save})

    def _get_request(self):
        """
        获取请求配置
        """

        request = utils.dictjoin(self.process.get('request', {}), copy.deepcopy(self.DEFAULT_PROCESS['request']))
        if 'cookie' in request and request['cookie']:
            cookie_list = re.split('(?:(?:\r\n)|\r|\n)', request['cookie'])
            if len(cookie_list) > 1:
                for item in cookie_list:
                    request['cookies_list'].append(utils.query2dict(item))
            else:
                request['cookies'] = utils.query2dict(cookie_list[0])
            del request['cookie']
        if 'header' in request and request['header']:
            header_list = re.split('(?:(?:\r\n)|\r|\n)', request['header'])
            if len(header_list) > 1:
                for item in header_list:
                    request['headers_list'].append(utils.query2dict(item))
            else:
                request['headers'] = utils.query2dict(header_list[0])
            request['headers'] = utils.query2dict(request['header'])
            del request['header']
        if 'data' in request and request['data']:
            request['data'] = utils.query2dict(request['data'])
        else:
            request['data'] = {}
        return request

    def preparse(self, rule):
        self.handler_run(HANDLER_FUN_PREPARSE, rule)
        return rule

    def parse(self, rule = None):
        """
        页面解析
        """
        self.debug("%s parse start" % (self.__class__.__name__))
        if not rule:
            rule = self.process.get("parse")
        self.debug("%s parse rule: %s" % (self.__class__.__name__, rule))
        rule = self.preparse(rule)
        self.debug("%s preparseed rule: %s" % (self.__class__.__name__, rule))

        if HANDLER_FUN_CRAWL in self.handle:
            self.handler_run(HANDLER_FUN_PARSE, {"rule": rule, "response": self.response, "handler": self})
        else:
            self.run_parse(rule)
        self.handler_run(HANDLER_FUN_POSTPARSE, self.response)
        self.debug("%s parse end" % (self.__class__.__name__))

    @abc.abstractmethod
    def run_parse(self, rule):
#        parser = CustomParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url = self.response['final_url'], attach_storage = self.attach_storage)
#        self.debug("%s parse start: %s @ %s" % (self.__class__.__name__, str(rule), self.mode))
#        parsed = parser.parse()
#        self.response['parsed'] = parsed
        pass

    def on_repetition(self, save):
        """
        重复处理
        """
        self.debug("%s on repetition" % (self.__class__.__name__))
        if HANDLER_FUN_REPETITION in self.handle:
            self.handler_run(HANDLER_FUN_REPETITION, {"response": self.response, "save": save})
        else:
            raise CDSpiderCrawlerNoNextPage(base_url=save.get("base_url", ''), current_url=save.get("request_url", ''))

    def on_error(self, exc, save):
        """
        错误处理
        """
        self.debug("%s on error" % (self.__class__.__name__))
        self.exception(exc)
        self.crawl_info['traceback'] = str(traceback.format_exc())
        self.handler_run(HANDLER_FUN_ERROR, {"response": self.response, "crawl_info": self.crawl_info, "save": save})

    def on_result(self, save):
        """
        数据处理
        """
        self.debug("%s on result" % (self.__class__.__name__))
        self.run_result(save)
        self.handler_run(HANDLER_FUN_RESULT, {"response": self.response, "save": save})

    def run_result(self, save):
        pass

    def on_next(self, save):
        """
        下一页解析
        """
        self.page += 1
        rule = self.process.get("paging")
        self.debug("%s on next rule: %s" % (self.__class__.__name__, rule))
        rule = self.format_paging(rule)
        self.debug("%s on next formated rule: %s" % (self.__class__.__name__, rule))
        if not rule:
            raise CDSpiderCrawlerNoNextPage(base_url=save.get("base_url", ''), current_url=save.get('request_url'))
        builder = UrlBuilder(self.logger, self.log_level)
        save['page'] = self.page
        request = copy.deepcopy(self.request)
        request.update(rule)
        self.request_params = builder.build(request, self.response['last_source'], self.crawler, save)
        self.handler_run(HANDLER_FUN_RESULT, {"response": self.response, "save": save})
        save['next_url'] = self.request_params['url']

    def on_continue(self, broken_exc, save):
#        if isinstance(broken_exc, (CDSpiderCrawlerForbidden,)):
        if isinstance(self.crawler, RequestsCrawler):
            self.info('Change crawler to Tornado')
            self.crawler.close()
            self.crawler = utils.load_crawler('tornado', log_level=self.log_level)
        else:
            self.force_proxy = True
        if isinstance(broken_exc, (CDSpiderCrawlerProxyError, CDSpiderCrawlerProxyExpired)):
            data = {"addr": self.crawler.proxy_str, 'ctime': int(time.time())}
            typeinfo = self._typeinfo(self.task['url'])
            data.update(typeinfo)
            self.db['proxy_log'].insert(data)
        else:
            self.auto_proxy = True
        if save['retry'] < self.MAX_RETRY:
            save['retry'] += 1
            self.handler_run(HANDLER_FUN_CONTINUE, {"broken_exc": broken_exc, "save": save})
            self.info('Retry to fetch: %s because of %s, current times: %s' % (self.task['url'], str(broken_exc), save['retry']))
        else:
            raise broken_exc

    def format_paging(self, paging):
        if not paging:
            return paging
        if paging.get('pattern', '1') == '1':
            if not "pageUrl" in paging or not paging['pageUrl']:
                return None
            rule = {"url": paging['pageUrl'], 'incr_data': []}
            if isinstance(paging['rule'], (list, tuple)):
                for item in paging['rule']:
                    if not 'word' in item or not item['word']:
                        continue
                    rule['incr_data'].append({
                        "mode": item['method'],
                        "name": item['word'],
                        "value": item['value'],
                        "step": item['step'],
                        "max": item['max'],
                        "value": item['value'],
                        "first": item.get('first', '0')
                    })
            elif isinstance(paging['rule'], dict):
                for item in paging['rule'].values():
                    if not 'word' in item or not item['word']:
                        continue
                    rule['incr_data'].append({
                        "mode": item['method'],
                        "name": item['word'],
                        "value": item['value'],
                        "step": item['step'],
                        "max": item['max'],
                        "value": item['value'],
                        "first": item.get('first', '0')
                    })
            if not rule['incr_data']:
                return None
            return rule
        if not paging['rule']:
            return None
        return {"url": {"element": {"xpath": {"filter": paging['rule'], "type": "attr", "target": "href"}}}}

    def get_unique_setting(self, url, data):
        """
        获取生成唯一ID的字段
        :param url 用来生成唯一索引的url
        :param data 用来生成唯一索引的数据
        :input self.process 爬取流程 {"unique": 唯一索引设置}
        :return 唯一索引的源字符串
        """
        #获取唯一索引设置规则
        identify = self.process.get('unique', None)
        subdomain, domain = utils.parse_domain(url)
        if not subdomain:
            parsed = urlparse(url)
            arr = list(parsed)
            arr[1] = "www.%s" % arr[1]
            u = urlunparse(arr)
        else:
            u = url
        if identify:
            if 'url' in identify and identify['url']:
                rule, key = utils.rule2pattern(identify['url'])
                if rule and key:
                    ret = re.search(rule, url)
                    if ret:
                        u = ret.group(key)
                else:
                    ret = re.search(identify['url'], url)
                    if ret:
                        u = ret.group(0)
            if 'query' in identify and identify['query'] and identify['query'].strip(','):
                u = utils.build_filter_query(url, identify['query'].strip(',').split(','))
            if 'data' in identify and identify['data'] and identify['data'].strip(','):
                udict = dict.fromkeys(identify['data'].strip(',').split(','))
                query = utils.dictunion(data, udict)
                return utils.build_query(u, query)
        return u

    def finish(self, save):
        self.handler_run(HANDLER_FUN_FINISH, {"save": save, "response": self.response})

    def close(self):
        if hasattr(self, 'crawler') and isinstance(self.crawler, BaseCrawler):
            self.crawler.quit()
            self.crawler = None

from .Loader import Loader
from .GeneralHandler import GeneralHandler
from .GeneralListHandler import GeneralListHandler
from .GeneralItemHandler import GeneralItemHandler
from .CommentHandler import CommentHandler
from .InteractHandler import InteractHandler
from .LinksClusterHandler import LinksClusterHandler
