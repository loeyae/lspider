#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
#version: SVN: $Id: __init__.py 2428 2018-07-31 01:28:52Z zhangyi $
import abc
import re
import six
import time
import logging
import traceback
import copy
from urllib.parse import urljoin
from cdspider.crawler import SeleniumCrawler
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.exceptions import *
from cdspider.libs.tools import *
from cdspider.libs.url_builder import UrlBuilder
from cdspider.libs.time_parser import Parser as TimeParser

IGNORE_EXCEPTIONS = (CDSpiderCrawlerNoNextPage, CDSpiderCrawlerMoreThanMaximum, CDSpiderCrawlerNoExists, CDSpiderCrawlerNoSource)
RETRY_EXCEPTIONS = (CDSpiderCrawlerConnectionError, CDSpiderCrawlerTimeout)
NOT_EXISTS_EXCEPTIONS = (CDSpiderCrawlerNotFound, CDSpiderCrawlerNoSource, CDSpiderParserError)

@six.add_metaclass(abc.ABCMeta)
class BaseHandler(object):
    """
    handler基类
    """
    MODE_DEFAULT = 'list'
    MODE_LIST = MODE_DEFAULT
    MODE_ITEM = 'item'
    MODE_ATT = 'att'
    MODE_CHANNEL = 'channel'
    PROXY_TYPE_AUTO = 'auto'
    PROXY_TYPE_EVER = 'ever'
    PROXY_TYPE_NEVER = 'never'
    CRAWL_INFO_LIMIT_COUNT = 10
    EXPIRE_STEP = 1
    CONTINUE_EXCEPTIONS = ()

    DEFAULT_PROCESS = {
        "request": {
            "crawler": "requests",
            "method": "GET",
            "proxy": "auto",
        }
    }

    def __init__(self, *args, **kwargs):
        """
        init
        """
        self.logger = kwargs.pop('logger', logging.getLogger('handler'))
        self.log_level = kwargs.pop('log_level', logging.WARN)
        self.logger.setLevel(self.log_level)
        self.task = kwargs.pop('task')
        self.attach_storage = kwargs.pop('attach_storage', None)
        self.db = kwargs.pop('db')
        self.queue = kwargs.pop('queue')
        self.crawl_id = self.task.get('save', {}).get('crawl_id', int(time.time()))
        self.crawl_info  = {
            "crawl_start": self.crawl_id,
            "crawl_end": None,
            "crawl_count": {
                "count": 0,
                "new_count": 0,
                "parsed_count": 0,
                "req_error": 0,
                "repeat_count": 0,
            },
            "broken": None,
            "traceback": None
        }
        self._settings = kwargs or {}
        self.crawler = None
        self.process = None
        self.mode = self.task.get('save').get('mode', self.MODE_DEFAULT)
        self.page = 1

    @property
    def current_page(self):
        return self.page

    def get_crawler(self, rule):
        crawler = crawler.get('crawler', 'requests')
        return utils.load_crawler(crawler, headers=rule.get('header', None), cookies=rule.get('cookie', None), proxy=rule.get('proxy'), log_level=self.log_level)

    def prepare(self, save):
        """
        预处理
        """
        pass

    @property
    def continue_exceptions(self):
        """
        获取自定义
        """
        return self.CONTINUE_EXCEPTIONS

    def get_unique_setting(self, url, data):
        """
        获取生成唯一ID的字段
        """
        site = self.task.get('site')
        urls = self.task.get('urls', {})
        attachment = self.task.get('attachment')
        if attachment:
            identify = attachment.get('unique', None)
        else:
            identify = urls.get('unique', site.get('unique', None))
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
            if 'query' in identify and identify['query']:
                u = utils.build_filter_query(url, identify['query'])
            if 'data' in identify:
                udict = dict.fromkeys(identify['data'])
                query = utils.dictunion(data, udict)
                return utils.build_query(u, query)
        return u

    @abc.abstractmethod
    def newtask(self):
        """
        构建新任务
        """
        pass

    def craw(self, save):
        broken_exc = None
        final_url = self.task.get('url')
        last_source = None
        try:
            request = self._get_request()
            proxy = request.pop('proxy', 'never')
            if self.page == 1:
                self.crawler = self.get_crawler(request)
            request['url'] = self.task.get('url')
            if self.page > 1 or self.mode != self.MODE_ITEM:
                save['incr_data'] = self._get_paging(request['url'])

            #列表页抓取时，第一页不添加分页参数,因此在第二页时，先将自增字段自增
            if self.page == 2 and self.mode == self.MODE_ITEM:
                for i in save['incr_data']:
                    if not 'base_page' in  save['incr_data'][i] or int(save['incr_data'][i]['value']) == int(save['incr_data'][i]['base_page']):
                        save['incr_data'][i]['isfirst'] = False
                        save['incr_data'][i]['value'] = int(save['incr_data'][i]['value']) + int(save['incr_data'][i].get('step', 1))
            builder = UrlBuilder(self.logger, self.log_level)
            params = builder.build(request, last_source, self.crawler, save)
            if self.crawler isinstance SeleniumCrawler and params['method'].upper() == 'GET':
                params['method'] = 'open'
            if proxy == self.PROXY_TYPE_NEVER and save['proxy']:
                params['proxy'] = copy.deepcopy(save['proxy'])
            self.crawler.crawl(**params)
            if self.page == 1:
                final_url = self.task['url'] = self.crawler.final_url
            last_source = self.crawler.page_source
        except Exception as exc:
            broken_exc = exc
        finally:
            return last_source, broken_exc, final_url

    def _init_process(self, url = None):
        if self.mode == self.MODE_CHANNEL:
            self.process = self.task.get('urls', {}).get('main_process', self.task.get('site', {}).get('main_process', self.DEFAULT_PROCESS))
        elif self.mode == self.MODE_LIST:
            self.process = self.task.get('urls', {}).get('sub_process', self.task.get('site', {}).get('sub_process', self.DEFAULT_PROCESS))
        elif self.mode == self.MODE_ITEM:
            subdomain, domain = utisl.parse_domain(url)
            parserule = None
            if subdomain:
                parserule = self.db['parseruledb'].get_detail_by_subdomain('%s.%s' % (subdomain, domain))
            if not parserule:
                parserule = self.db['parseruledb'].get_detail_by_domain(domain)
            self.process = parserule:
        elif self.mode == self.MODE_ATT:
            self.process = self.task.get('attachment', {}).get('process', self.DEFAULT_PROCESS)

    def _get_request(self):
        """
        获取请求配置
        """
        if self.mode == self.MODE_ITEM:
            return self.DEFAULT_PROCESS

        if not self.process:
            self._init_process();
        return self.process.get('request', {})

    def _get_paging(self, url = None):
        if not self.process:
            self._init_process(url);
        return self.process.get('paging', None)

    def parse(self, source, url):
        """
        页面解析
        """
        rule = self._get_parse(url)
        if self.mode == self.MODE_ITEM:
            parser_name = 'item'
        else:
            parser_name = 'list'
        parser = utils.load_parser(parser_name, source=source, ruleset=copy.deepcopy(rule), log_level=self.log_level, url=url, attach_storage = self.attach_storage)
        parsed = parser.parse()
        return parsed

    def _get_parse(self, url = None):
        """
        获取解析规则
        """
        if not self.process:
            self._init_process(url)
        parse = self.process.get('parse', {})
        if self.mode == self.MODE_ATT:
            if not 'item' in  parse:
                parse = {"filter": "", "item": parse}
        return parse

    def on_attach(self, source, url):


    def on_repetition(self):
        raise CDSpiderCrawlerNoNextPage()

    def on_error(self, exc):
        """
        错误处理
        """
        self.crawl_info['broken'] = str(exc)
        if 'queue' in self.task and self.task['queue'] and 'queue_message' in self.task and self.task['queue_message']:
            if isinstance(exc, RETRY_EXCEPTIONS) or not isinstance(exc, CDSpiderError):
                self.task['queue'].put_nowait(self.task['queue_message'])
                return
        if isinstance(exc, NOT_EXISTS_EXCEPTIONS) and 'rid' in self.task and self.db['articlesdb']:
            self.db['articlesdb'].update(self.task['rid'], {"status": self.db['articlesdb'].STATUS_DELETED})
            return
        if not isinstance(exc, IGNORE_EXCEPTIONS) and self.queue['excqueue']:
            message = {
                'mode':  self.mode,
                'base_url': self.task.get("save", {}).get("base_url", None),
                'request_url': self.task.get("save", {}).get("request_url", None),
                'project': self.task.get("pid", None),
                'site': self.task.get("sid", None),
                'urls': self.task.get("uid", None),
                'site': self.task.get("kwid", None),
                'crawltime': self.crawl_id,
                'err_message': str(exc),
                'tracback': traceback.format_exc(),
                'last_source': self.task.get('last_source', None),
            }
            self.queue['excqueue'].put_nowait(message)

    @abc.abstractmethod
    def on_result(self, data, broken_exc, page_source, final_url):
        """
        数据处理
        """
        pass

    def on_continue(self, crawler, save):
        if 'incr_data' in save:
            for i in range(len(save['incr_data'])):
                save['incr_data'][i]['value'] = int(save['incr_data'][i]['value']) - int(save['incr_data'][i].get('step', 1))

    def finish(self):
        if self.db['taskdb'] and self.task.get('tid', None):
            crawlinfo = self.task.get('crawlinfo', {}) or {}
            self.crawl_info['crawl_end'] = int(time.time())
            crawlinfo[str(self.crawl_id)] = self.crawl_info
            crawlinfo_sorted = [(k, crawlinfo[k]) for k in sorted(crawlinfo.keys())]
            if len(crawlinfo_sorted) > self.CRAWL_INFO_LIMIT_COUNT:
                del crawlinfo_sorted[0]
            self.db['taskdb'].update(self.task.get('tid'), self.task.get('pid'), {"crawltime": self.crawl_id, "crawlinfo": dict(crawlinfo_sorted), "save": self.task.get("save")})
            #TODO 自动调节抓取频率

from .NewTaskTrait import NewTaskTrait
from .ResultTrait import ResultTrait
from .SearchHandler import SearchHandler
from .GeneralHandler import GeneralHandler
from .AttachHandler import AttachHandler
from .ProjectBaseHandler import ProjectBaseHandler