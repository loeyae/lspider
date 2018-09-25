#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
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
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.exceptions import *
from cdspider.libs.tools import *
from cdspider.parser import *
from cdspider.libs.url_builder import UrlBuilder

IGNORE_EXCEPTIONS = (CDSpiderCrawlerNoNextPage, CDSpiderCrawlerMoreThanMaximum, CDSpiderCrawlerProxyExpired, CDSpiderCrawlerNoExists, CDSpiderCrawlerNoSource)
RETRY_EXCEPTIONS = (CDSpiderCrawlerConnectionError, CDSpiderCrawlerTimeout)
NOT_EXISTS_EXCEPTIONS = (CDSpiderCrawlerNotFound, CDSpiderCrawlerNoSource, CDSpiderParserError)

@six.add_metaclass(abc.ABCMeta)
class BaseHandler(Component):
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

    MAX_RETRY = 5

    def __init__(self, *args, **kwargs):
        """
        init
        """
        self.logger = kwargs.pop('logger', logging.getLogger('handler'))
        self.log_level = kwargs.pop('log_level', logging.WARN)
        super(BaseHandler, self).__init__(self.logger, self.log_level)
        self.task = kwargs.pop('task')
        self.crawler_list = kwargs.pop('crawler', [])
        self.attach_storage = kwargs.pop('attach_storage', None)
        self.db = kwargs.pop('db',None)
        self.queue = kwargs.pop('queue',None)
        self.crawl_id = int(time.time())
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
        self.no_sync = kwargs.pop('no_sync', False)
        self._settings = kwargs or {}
        self.mycrawler = True
        self.crawler = None
        self.process = None
        self.force_proxy = False
        self.auto_proxy = False
        self.mode = self.task.get('save',{}).get('mode', self.MODE_DEFAULT)
        self.page = 1
        self.last_result_id = None
        self.sync_result = set()

    def __del__(self):
        if self.mycrawler and isinstance(self.crawler, BaseCrawler):
            self.crawler.quit()

    def _domain_info(self, url):
        subdomain, domain = utils.parse_domain(url)
        if not subdomain:
            subdomain = 'www'
        return "%s.%s" % (subdomain, domain), domain

    def _typeinfo(self, url):
        subdomain, domain = self._domain_info(url)
        return {"domain": domain, "subdomain": subdomain}


    def result_prepare(self, data):
        """
        入库数据预处理
        """
        return data

    def parse_media_type(self, url):
        """
        解析媒体类型
        """
        subdomain, domain = self._domain_info(url)
        media_type = self.db['MediaTypesDB'].get_detail_by_subdomain(subdomain)
        if media_type:
            return int(media_type['mediaType'])
        media_type = self.db['MediaTypesDB'].get_detail_by_domain(domain)
        if media_type:
            return int(media_type['mediaType'])
        if 'media_type' in self.process and self.process['media_type']:
            return int(self.process['media_type'])

        return 1

    def url_prepare(self, url):
        """
        url预处理
        """
        return url

    def item_result_post(self, result, unid):
        pass

    def build_url_by_rule(self, data, base_url = None):
        if not base_url:
            base_url = self.task.get('url')
        if self.mode in (self.MODE_ITEM, self.MODE_ATT):
            if not self.process:
                self._init_process()
            urlrule = self.process.get('url', {})
        elif self.mode == self.MODE_CHANNEL:
            url_process = self.task.get('urls', {}).get('main_process', {}) or {}
            site_process = self.task.get('site', {}).get('main_process', {}) or {}
            urlrule = url_process.get('url', {}) or site_process.get('url', {})
        elif self.mode == self.MODE_LIST:
            url_process = self.task.get('urls', {}).get('sub_process', {}) or {}
            site_process = self.task.get('site', {}).get('sub_process', {}) or {}
            urlrule = url_process.get('url', {}) or site_process.get('url', {})
        formated = []
        for item in data:
            if not 'url' in item or not item['url']:
                raise CDSpiderError("url no exists: %s @ %s" % (str(item), str(task)))
            if item['url'].startswith('javascript') or item['url'] == '/':
                continue
            try:
                item['url'] = self.url_prepare(item['url'])
            except:
                continue
            if urlrule and 'name' in urlrule and urlrule['name']:
                parsed = {urlrule['name']: item['url']}
                item['url'] = utils.build_url_by_rule(urlrule, parsed)
            else:
                item['url'] = urljoin(base_url, item['url'])
            formated.append(item)
        return formated

    @property
    def current_page(self):
        return self.page

    def get_crawler(self, rule):
        crawler = rule.get('crawler', '') or 'requests'
        if self.crawler_list and isinstance(self.crawler_list, (list, tuple)) and crawler in self.crawler_list:
            self.mycrawler = False
            return self.crawler_list[crawler]
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
            urls_unique = urls.get('unique', None) or {}
            site_unique = site.get('unique', None) or {}
            identify = {
                "url": urls_unique.get("url") or site_unique.get("url"),
                "query": urls_unique.get("query") or site_unique.get("query"),
                "data": urls_unique.get("data") or site_unique.get("data"),
            }
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
            if 'query' in identify and identify['query']:
                u = utils.build_filter_query(url, identify['query'])
            if 'data' in identify and identify['data']:
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

    def crawl(self, save, crawler = None):
        """
        数据抓取
        :param: save 保持的上下文
        :param: crawler 是否更新crawler对象
        """
        broken_exc = None
        final_url = self.task.get('url')
        last_source = None
        try:
            request = copy.deepcopy(self._get_request(final_url))
            proxy = request.pop('proxy', 'never')
            if not self.crawler:
                self.crawler = crawler or self.get_crawler(request)
            request['url'] = self.task.get('url')
            if self.page == 1 and self.mode != self.MODE_ITEM:
                request['incr_data'] = self._get_paging(save.get("parent_url", request['url']))

            #详情页抓取时，第一页不添加分页参数,因此在第二页时，先将自增字段自增
            if self.page == 2 and self.mode == self.MODE_ITEM:
                request['incr_data'] = self._get_paging(save.get("parent_url", request['url']))
                for i in request['incr_data']:
                    if not 'base_page' in  request['incr_data'][i] or int(request['incr_data'][i]['value']) == int(request['incr_data'][i]['base_page']):
                        request['incr_data'][i]['isfirst'] = False
                        request['incr_data'][i]['value'] = int(request['incr_data'][i]['value']) + int(request['incr_data'][i].get('step', 1))
            builder = UrlBuilder(self.logger, self.log_level)
            params = builder.build(request, last_source, self.crawler, save)
            if isinstance(self.crawler, SeleniumCrawler) and params['method'].upper() == 'GET':
                params['method'] = 'open'
            if (proxy == self.PROXY_TYPE_EVER or self.force_proxy or (proxy == self.PROXY_TYPE_AUTO and self.auto_proxy)) and save['proxy']:
                params['proxy'] = copy.deepcopy(save['proxy'])
            else:
                params['proxy'] = None
            self.crawler.crawl(**params)
            if self.page == 1:
                final_url = save['request_url'] = self.crawler.final_url
            last_source = self.crawler.page_source
        except Exception as exc:
            broken_exc = exc
        finally:
            return last_source, broken_exc, final_url

    def _init_process(self, url = None):
        if self.mode == self.MODE_ITEM:
            subdomain, domain = self._domain_info(url)
            parserule = None
            if subdomain:
                parserule_list = self.db['ParseRuleDB'].get_list_by_subdomain(subdomain)
                for item in parserule_list:
                    if not parserule:
                        parserule = item
                    if  'url_pattern' in item and item['url_pattern']:
                        u = utils.preg(url, item['url_pattern'])
                        if u:
                            parserule = item
            if not parserule:
                parserule_list = self.db['ParseRuleDB'].get_list_by_domain(domain)
                for item in parserule_list:
                    if not parserule:
                        parserule = item
                    if  'url_pattern' in item and item['url_pattern']:
                        u = utils.preg(url, item['url_pattern'])
                        if u:
                            parserule = item
            self.process = parserule or copy.deepcopy(self.DEFAULT_PROCESS)
        elif self.mode == self.MODE_CHANNEL:
            self.process = self.task.get('channel', {}).get('process', None) or copy.deepcopy(self.DEFAULT_PROCESS)
        elif self.mode == self.MODE_ATT:
            self.process = self.task.get('attachment', {}).get('process', None) or copy.deepcopy(self.DEFAULT_PROCESS)

    def _get_request(self, url):
        """
        获取请求配置
        """
        if self.mode in (self.MODE_ITEM, self.MODE_ATT, self.MODE_CHANNEL):
            if not self.process:
                self._init_process(url);
            request = utils.dictjoin(self.process.get('request', {}), copy.deepcopy(self.DEFAULT_PROCESS['request']))
        else:
            url_process = self.task.get('urls', {}).get('sub_process', {}) or {}
            site_process = self.task.get('site', {}).get('sub_process', {}) or {}
            request = utils.dictjoin(url_process.get('request', {}) or site_process.get('request', {}), copy.deepcopy(self.DEFAULT_PROCESS['request']))
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

    def _get_paging(self, url = None):
        if self.mode in (self.MODE_ITEM, self.MODE_ATT, self.MODE_CHANNEL):
            if not self.process:
                self._init_process(url);
            paging = self.process.get('paging', None)
        else:
            url_process = self.task.get('urls', {}).get('sub_process', {}) or {}
            site_process = self.task.get('site', {}).get('sub_process', {}) or {}
            paging = url_process.get('paging', {}) or site_process.get('paging', {})
        if paging:
            if "name" in paging and paging['name']:
                return paging
        return None

    def parse(self, source, url, rule = None, mode = None):
        """
        页面解析
        """
        if not rule:
            rule = self._get_parse(url)
        if not mode:
            mode = self.mode
        if mode == self.MODE_ATT:
            parser = CustomParser(source=source, ruleset=copy.deepcopy(rule), log_level=self.log_level, url=url, attach_storage = self.attach_storage)
        elif mode == self.MODE_ITEM:
            if not isinstance(rule, list):
                rule = [rule]
            p = None
            for item in rule:
                parser = ItemParser(source=source, ruleset=copy.deepcopy(item), log_level=self.log_level, url=url, attach_storage = self.attach_storage)
                parsed = parser.parse()
                if 'verifi' in item and 'verifi' in parsed and parsed['verifi']:
                    return parsed
                if not p:
                    p = parsed
                else:
                    if len(p.keys()) < len(parsed.keys()):
                        p = parsed
            return p
        else:
#            parser_name = 'list'
            parser = ListParser(source=source, ruleset=copy.deepcopy(rule), log_level=self.log_level, url=url, attach_storage = self.attach_storage)
        self.debug("%s parse start: %s @ %s" % (self.__class__.__name__, str(rule), self.mode))
#        parser = utils.load_parser(parser_name, source=source, ruleset=copy.deepcopy(rule), log_level=self.log_level, url=url, attach_storage = self.attach_storage)
        parsed = parser.parse()
        self.debug("%s parse end" % (self.__class__.__name__))
        return parsed

    def _get_parse(self, url = None):
        """
        获取解析规则
        """
        if self.mode in (self.MODE_ITEM, self.MODE_ATT, self.MODE_CHANNEL):
            if not self.process:
                self._init_process(url);
            parse = self.process.get('parse', {})
        else:
            url_process = self.task.get('urls', {}).get('sub_process', {}) or {}
            site_process = self.task.get('site', {}).get('sub_process', {}) or {}
            parse = url_process.get('parse', {}) or site_process.get('parse', {})

        if self.mode == self.MODE_ATT:
            if not 'item' in  parse:
                parse = dict((key, item) for key, item in parse.items() if 'filter' in item and item['filter'])
        return parse

    def on_attach(self, source, url, list_url, return_result = False):
        """
        获取附加任务链接，并push newtask
        """
        self.debug("%s attach start: %s @ %s" % (self.__class__.__name__, str(url), self.mode))
        pid = self.task.get('pid')
        subdomain, domain = self._domain_info(url)
        psubdomain, pdomain = self._domain_info(list_url)
        if pdomain == domain:
            subdomain = psubdomain
        attach_list = self.db['AttachmentDB'].get_list_by_subdomain(pid, subdomain, where={"status": AttachmentDB.STATUS_ACTIVE})
        attach_list = list(attach_list)
        dattach_list = self.db['AttachmentDB'].get_list_by_domain(pid, domain, where={"status": AttachmentDB.STATUS_ACTIVE})
        attach_list.extend(list(dattach_list))
        self.debug("%s attach list: %s" % (self.__class__.__name__, str(attach_list)))
        if return_result:
            return_data = []
        for each in attach_list:
            pparse = each.get('preparse', {}).get('parse', None)
            parse = {}
            for item in pparse.values():
                key = item.pop('key')
                if key and item['filter']:
                    if item['filter'] == '@value:parent_url':
                        item['filter'] = '@value:%s' % url
                    elif item['filter'].startswith('@url:'):
                        r = item['filter'][5:]
                        v = utils.preg(url, r)
                        if not v:
                            continue
                        item['filter'] = '@value:%s' % v
                    parse[key] = item
            urlrule = each.get('preparse', {}).get('url', None)
            if urlrule['base'] == 'parent_url':
                urlrule['base'] = url
            parsed = {}
            if parse:
                parsed = utils.filter(self.parse(source, url, parse, self.MODE_ATT))
                if not parsed or parse.keys() != parsed.keys():
                    continue
                self.debug("%s attach parsed: %s" % (self.__class__.__name__, parsed))
            attachurl = utils.build_url_by_rule(urlrule, parsed)
            message = {'aid': each['aid'], 'url': attachurl, 'pid': self.task.get('pid'), 'rid': self.last_result_id}
            if not return_result:
                self.debug("%s attach create task: %s" % (self.__class__.__name__, str(message)))
                self.queue['newtask_queue'].put_nowait(message)
            else:
                return_data.append(message)

        self.debug("%s attach end" % (self.__class__.__name__))
        if return_result:
            return return_data

    def on_sync(self):
        """
        同步大数据平台
        """
        if self.no_sync and self.sync_result:
            return
        self.info("result2kafka  starting...")
        res={}
        on_sync = self.task.get('project', {}).get('on_sync', None)
        if on_sync != None and on_sync != '':
            res['on_sync'] = on_sync
        for rid in self.sync_result:
            res['rid'] = rid
            self.queue['result2kafka'].put_nowait(res)
            self.info("result2kafka  end data: %s" % str(res))


    def on_repetition(self):
        """
        重复处理
        """
        raise CDSpiderCrawlerNoNextPage(base_url=self.task.get('save', {}).get("base_url", ''), current_url=self.task.get('save', {}).get("request_url", ''))

    def on_error(self, exc):
        """
        错误处理
        """
        if not isinstance(exc, CDSpiderError):
            exc = CDSpiderError(exc)
        exc.params.update({
            "pid": self.task.get("pid"),
            "sid": self.task.get("sid"),
            "uid": self.task.get("uid", 0),
            "kwid": self.task.get("kwid", 0),
            "aid": self.task.get("aid", 0),
            "tid": self.task.get("tid", 0),
            "crawlid": self.crawl_id,
        })
        self.error(str(exc))
        self.crawl_info['broken'] = str(exc)
        if 'queue' in self.task and self.task['queue'] and 'queue_message' in self.task and self.task['queue_message']:
            if isinstance(exc, RETRY_EXCEPTIONS) or not isinstance(exc, CDSpiderError):
                if  not 'retry' in self.task['queue_message']:
                    self.task['queue_message']['retry'] = self.task['save']['retry']
                if self.task['queue_message']['retry'] < self.MAX_RETRY:
                    self.task['queue_message']['retry'] += 1
                    self.task['queue'].put_nowait(self.task['queue_message'])
                return
        if isinstance(exc, NOT_EXISTS_EXCEPTIONS) and 'rid' in self.task and self.task['rid'] and self.db['ArticlesDB']:
            self.db['ArticlesDB'].update(self.task['rid'], {"status": self.db['ArticlesDB'].STATUS_DELETED})
            return
        self.crawl_info['err_message'] = str(traceback.format_exc())
#        if not isinstance(exc, IGNORE_EXCEPTIONS) and self.queue['excinfo_queue']:
#            self.crawl_info['err_message'] = str(traceback.format_exc())
#            message = {
#                'mode':  self.mode,
#                'base_url': self.task.get("save", {}).get("base_url", None),
#                'request_url': self.task.get("save", {}).get("request_url", None),
#                'project': self.task.get("pid", None),
#                'site': self.task.get("sid", None),
#                'urls': self.task.get("uid", None),
#                'keyword': self.task.get("kwid", None),
#                'crawltime': self.crawl_id,
#                'err_message': str(exc),
#                'tracback': traceback.format_exc(),
#                'last_source': self.task.get('last_source', None),
#            }
#            self.queue['excinfo_queue'].put_nowait(message)

    @abc.abstractmethod
    def on_result(self, data, broken_exc, page_source, final_url, return_result = False):
        """
        数据处理
        """
        pass

    def on_continue(self, broken_exc, save):
        if 'incr_data' in save:
            for i in range(len(save['incr_data'])):
                if int(save['incr_data'][i]['value']) > int(save['incr_data'][i]['base_page']):
                    save['incr_data'][i]['value'] = int(save['incr_data'][i]['value']) - int(save['incr_data'][i].get('step', 1))
        if isinstance(broken_exc, (CDSpiderCrawlerForbidden,)):
            if isinstance(self.crawler, RequestsCrawler):
                self.info('Change crawler to Tornado')
                self.crawler.close()
                self.crawler = utils.load_crawler('tornado', log_level=self.log_level)
            else:
                self.force_proxy = True
        elif isinstance(broken_exc, (CDSpiderCrawlerProxyError, CDSpiderCrawlerProxyExpired)):
            data = {"addr": self.crawler.proxy_str, 'ctime': int(time.time())}
            typeinfo = self._typeinfo(self.task['url'])
            data.update(typeinfo)
            self.db['base'].insert(data, 'proxy_log')
        else:
            self.auto_proxy = True
        if save['retry'] < self.MAX_RETRY:
            save['retry'] += 1
            self.info('Retry to fetch: %s because of %s, current times: %s' % (self.task['url'], str(broken_exc), self.task['save']['retry']))
        else:
            raise broken_exc

    def finish(self):
        if self.db['TaskDB'] and self.task.get('tid', None):
            crawlinfo = self.task.get('crawlinfo', {}) or {}
            self.crawl_info['crawl_end'] = int(time.time())
            crawlinfo[str(self.crawl_id)] = self.crawl_info
            crawlinfo_sorted = [(k, crawlinfo[k]) for k in sorted(crawlinfo.keys())]
            if len(crawlinfo_sorted) > self.CRAWL_INFO_LIMIT_COUNT:
                del crawlinfo_sorted[0]
            save = self.task.get("save")
            if 'incr_data' in save:
                for i in range(len(save['incr_data'])):
                    if int(save['incr_data'][i]['value']) > int(save['incr_data'][i]['base_page']):
                        save['incr_data'][i]['last_page'] = int(save['incr_data'][i]['value'])
                        save['incr_data'][i]['value'] = int(save['incr_data'][i]['base_page'])
                    save['incr_data'][i]['isfirst'] = True
            self.db['TaskDB'].update(self.task.get('tid'), self.task.get('pid'), {"crawltime": self.crawl_id, "crawlinfo": dict(crawlinfo_sorted), "save": save})
            #TODO 自动调节抓取频率

from .NewTaskTrait import NewTaskTrait
from .ResultTrait import ResultTrait
from .SearchHandler import SearchHandler
from .GeneralHandler import GeneralHandler
from .AttachHandler import AttachHandler
from .ProjectBaseHandler import ProjectBaseHandler
