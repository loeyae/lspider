# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2019/4/14 22:22
"""

import os
import time
import traceback
import re
import random
from urllib.parse import urljoin
from cdspider.scheduler import CounterMananger
from cdspider.crawler import BaseCrawler
from cdspider.crawler import RequestsCrawler
from cdspider.libs.tools import *
from cdspider.parser import *
from cdspider.libs.url_builder import UrlBuilder
from cdspider.libs.constants import *
from cdspider.parser import CustomParser
from .HandlerUtils import HandlerUtils


@six.add_metaclass(abc.ABCMeta)
class BaseHandler(Component):
    """
    handler基类
    """
    BLOOMFILTER_KEY = '%(prefix)s_cdspider_$(project)s_%(key)s'
    CRAWL_INFO_LIMIT_COUNT = 10
    EXPIRE_STEP = 1
    DEFAULT_FREQUENCY = 6
    ALLOWED_REPEAT = 1
    ROUTE_INTERVAL = 60
    ROUTE_LIMIT = 100

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
        :param context: click,Context
        :param task: task info
        :param kwargs: kwargs
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
        self.runtime_dir = g.get('runtime_dir')
        self.db = g.get('db', None)
        self.queue = g.get('queue', None)
        self.frequencymap = g.get('app_config', {}).get('frequencymap', {})
        self.testing_mode = g.get('testing_mode', False)
        self.log_id = None
        if self.task:
            attach_storage = g.get('app_config', {}).get('attach_storage', None)
            if attach_storage:
                attach_storage = os.path.realpath(os.path.join(g.get("app_path"), attach_storage))
            self.attach_storage = attach_storage
            self.crawl_id = int(time.time())
            self.crawl_info = HandlerUtils.init_crawl_info(self.crawl_id)
            self.no_sync = kwargs.pop('no_sync', False)
            self._settings = kwargs or {}
            self.crawler = None
            self.process = {}
            self.proxy_mode = 'never'
            self.request = {}
            self.request_params = {}
            self.response = HandlerUtils.init_reponse(self.task.get('url') or '')
            self.force_proxy = False
            self.auto_proxy = False
            self.page = 1
            self.last_result_id = None
        self.handle = {}

    def __del__(self):
        """
        释放
        :return:
        """
        self.close()
        del self.ctx
        del self.handle
        if self.task:
            del self.process
            del self.crawl_info
            del self.request
            del self.request_params
            del self.response
            del self.task
        super(BaseHandler, self).__del__()

    def route(self, handler_driver_name, frequency, save):
        """
        schedule 分发 该方法返回的迭代器用于router生成queue消息，以便plantask听取，消息格式为:
        {"mode": handler mode, "frequency": frequency, "offset": offset, "count": count}
        :param handler_driver_name: handler mode
        :param frequency: 频率
        :param save: 传递的上下文
        :return: 包含字典（{"offset": offset, "count": count}）的迭代器。
        """
        if "id" not in save:
            '''
            初始化上下文中的id参数,该参数用于数据查询
            '''
            save["id"] = 0
        if "now" in save:
            now = save['now']
        else:
            now = int(time.time())
            save['now'] = now

        # 按项目分发
        cmdir = os.path.join(self.runtime_dir, 'cm')
        setting = self.frequencymap[frequency]
        cm = CounterMananger(cmdir, (handler_driver_name, frequency))
        if not cm.get('stime') or cm.get('stime') + cm.get('ctime') <= now:
            cm.empty()
            total = self.db['SpiderTaskDB'].get_active_count(handler_driver_name, {"frequency": frequency})
            cm.event(stime=now, itime=self.ROUTE_INTERVAL, ctime=setting[0], total=total)
        cm.event(now=now)
        offset = cm.get('offset')
        count = cm.get('count')
        total = cm.get('total')
        self.error("%s route %s @ %s, total: %s offset: %s count: %s" % (self.__class__.__name__, frequency, now, total,
        offset, count))
        cm.value(count)
        while count > 0 and offset < total:
            yield {"offset": offset, "count": self.ROUTE_LIMIT if count > self.ROUTE_LIMIT else count}
            count -= self.ROUTE_LIMIT
            offset += self.ROUTE_LIMIT
        cm.dump()

    def schedule(self, message, save):
        """
        任务分发
        :param message: 消息 ex: {
                    "frequency": frequency,
                    "mode": mode,
                    "offset": offset,
                    "count": count,
                }
        :param save: 传递的上下文
        :return: 包含字典({"uuid": uuid, "url": url})的迭代器
        """
        if 'type' in message:
            if 'id' not in save:
                save['id'] = 0
            if message['type'] == ROUTER_MODE_PROJECT:
                where = {"pid": int(message['item'])}
            elif message['type'] == ROUTER_MODE_SITE:
                where = {"sid": int(message['item'])}
            elif message['type'] == ROUTER_MODE_URL:
                where = {"uid": int(message['item'])}
            else:
                where = {"tid": int(message['item'])}
            for item in self.db['SpiderTaskDB'].get_plan_list(message['mode'], save['id'], plantime=save['now'],
                                                              where=where, select=['uuid', 'url', 'frequency']):
                if not self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    frequency = str(item.pop('frequency', self.DEFAULT_FREQUENCY))
                    plantime = int(save['now']) + int(self.frequencymap[frequency][0])
                    self.db['SpiderTaskDB'].update(item['uuid'], message['mode'], {"plantime": plantime})
                if item['uuid'] > save['id']:
                    save['id'] = item['uuid']
                yield item
        else:
            for item in self.db['SpiderTaskDB'].get_active_list(
                    message['mode'], where={"frequency": str(message['frequency'])}, offset=int(message['offset']),
                    hits=int(message['count']), select=['uuid', 'url']):
                if not self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    plantime = int(save['now']) + int(self.frequencymap[str(message['frequency'])][0])
                    self.db['SpiderTaskDB'].update(item['uuid'], message['mode'], {"plantime": plantime})
                yield item

    def newtask(self, message):
        """
        生成新任务
        :param message: 消息
        :return:
        """
        pass

    def frequency(self, message):
        """
        更新更新频率
        :param message: 消息
        :return:
        """
        mode = message['mode']
        frequency = str(message['frequency'])
        where = HandlerUtils.build_spider_task_where(message)
        plantime = int(time.time()) + int(self.frequencymap[frequency][0])
        self.db['SpiderTaskDB'].update_many(mode, {"plantime": plantime, "frequency": frequency}, where=where)

    def expire(self, message):
        """
        更新过期时间
        :param message: 消息
        :return:
        """
        mode = message['mode']
        expire = message['expire']
        where = HandlerUtils.build_spider_task_where(message)
        expire = int(time.time()) + int(expire) if int(expire) > 0 else 0
        self.db['SpiderTaskDB'].update_many(mode, {"expire": expire}, where=where)

    def status(self, message):
        """
        更改状态
        :param message: 消息
        :return:
        """
        self.queue[QUEUE_NAME_STATUS].put_nowait(message);

    def get_scripts(self):
        """
        获取自定义脚本
        :return:
        """
        return None

    def handler_register(self, handle_type, priority = 1000):
        """
        handler register
        :param handle_type: any of cdspider.libs.constants.(HANDLER_FUN_INIT, HANDLER_FUN_PROCESS,
        HANDLER_FUN_PREPARE, HANDLER_FUN_PRECRAWL, HANDLER_FUN_CRAWL, HANDLER_FUN_POSTCRAWL, HANDLER_FUN_PREPARSE,
        HANDLER_FUN_PARSE, HANDLER_FUN_POSTPARSE, HANDLER_FUN_RESULT, HANDLER_FUN_NEXT, HANDLER_FUN_CONTINUE,
        HANDLER_FUN_REPETITION, HANDLER_FUN_ERROR, HANDLER_FUN_FINISH)
        :param priority: 数值越大，优先级越高
        :return:
        """
        if not (isinstance(handle_type, list) or isinstance(handle_type, tuple)):
            handle_type = [handle_type]

        def _handler_register(fn):
            for _type in handle_type:
                if _type not in self.handle:
                    self.handle[_type] = []
                self.handle[_type].append((priority, fn))
            return fn
        return _handler_register

    def handler_run(self, handle_type, kwargs):
        """
        运行注册的方法
        :param handle_type: 方法类型
        :param save: 参宿
        :return:
        """
        if not (isinstance(handle_type, list) or isinstance(handle_type, tuple)):
            handle_type = [handle_type]
        for _type in handle_type:
            func_list = self.handle.get(_type, None)
            print(_type, func_list)
            if func_list:
                for _,fn in sorted(func_list, reverse=True):
                    if callable(fn):
                        fn(self, kwargs)

    @property
    def current_page(self):
        """
        当前页码
        :return:
        """
        return self.page

    def init(self, save):
        """
        初始化爬虫
        :param save: 船体的上下文
        """
        if "uuid" in self.task and self.task['uuid']:
            task = self.db['SpiderTaskDB'].get_detail(self.task['uuid'], self.mode)
            if not task:
                raise CDSpiderDBDataNotFound("SpiderTask: %s not exists" % self.task['uuid'])
            self.task.update(task)
            if 'save' in task and task['save'] and 'hard_code' in task['save']:
                save['hard_code'] = task['save']['hard_code']
            if 'tid' in task and task['tid']:
                t = self.db['TaskDB'].get_detail(task['tid'])
                self.task['task'] = t or {}
            # 记录抓取日志
            log = HandlerUtils.build_log(self.mode, self.task, self.crawl_id)
            self.log_id = self.db['CrawlLogDB'].insert(log)

        self.task.setdefault('task', {})
        if "save" in self.task and self.task['save'] and "current_page" in self.task['save']:
            self.page = int(self.task['save']['current_page'])
        self.init_process(save)
        if not save['base_url'] or save['base_url'] == 'base_url':
            save['base_url'] = self.task['url']
        save['page'] = self.page
        self.handler_run(HANDLER_FUN_PROCESS, {"save": save})
        self.request = self._get_request(save)
        self.proxy_mode = self.request.pop('proxy', 'never')
        if not self.crawler:
            self.crawler = HandlerUtils.get_crawler(self.request, self.log_level)
        self.request['url'] = self.task.get('url')
        request = copy.deepcopy(self.request)
        if "request" in save and save['request']:
            self.debug("%s other request parameters: %s" % (self.__class__.__name__, save['request']))
            if save['request']:
                request = HandlerUtils.update_request(request, save['request'])

        rule = self.process.get("paging")
        self.debug("%s paging rule: %s" % (self.__class__.__name__, rule))
        rule = HandlerUtils.format_paging(copy.deepcopy(rule))
        self.debug("%s formated paging rule: %s" % (self.__class__.__name__, rule))
        if rule:
            request = HandlerUtils.update_request(request, rule)
        builder = UrlBuilder(CustomParser, self.logger, self.log_level)
        self.request_params = builder.build(request, self.response['content'] or DEFAULT_SOURCE,
                                            self.response.get('cookies', {}), save)
        self.handler_run(HANDLER_FUN_INIT, {"save": save})

    @abc.abstractmethod
    def init_process(self, save):
        """
        初始化抓取流程
        :param save: 传递的上下文
        :return:
        """
        pass

    def prepare(self, save):
        """
        预处理
        :param save: 传递的上下文
        """
        self.handler_run(HANDLER_FUN_PREPARE, {"save": save})

    def precrawl(self, save):
        """
        爬取预处理
        :param save: 传递的上下文
        :return:
        """
        if (self.proxy_mode == PROXY_TYPE_EVER or self.force_proxy or (self.proxy_mode == PROXY_TYPE_AUTO and
                                                                       self.auto_proxy)) and self.proxy:
            self.request_params['proxy'] = copy.deepcopy(self.proxy)
        else:
            self.request_params['proxy'] = None
        self.handler_run(HANDLER_FUN_PRECRAWL, {"save": save})

    def crawl(self, save):
        """
        数据抓取
        :param: save: 传递的上下文
        :return:
        """
        try:
            self.precrawl(save)
            params = copy.deepcopy(self.request_params)
            if HANDLER_FUN_CRAWL in self.handle:
                self.handler_run(HANDLER_FUN_CRAWL, {"requst_params": params, "save": save})
            else:
                response = self.crawler.crawl(**params)
                if response is None:
                    raise CDSpiderCrawlerBadRequest()
                self.response.update(response)
                save['request_url'] = response['url']
            if self.page == 1:
                self.response['final_url'] = self.response['url']
                self.response['source'] = self.response['content']
        except Exception as exc:
            self.response['broken_exc'] = exc
        finally:
            self.crawler.proxy_lock = None
            self.handler_run(HANDLER_FUN_POSTCRAWL, {"save": save})

    def _get_request(self, save):
        """
        获取请求配置
        :param save: 传递的上下文
        :return:
        """

        request = utils.dictjoin(self.process.get('request', {}), copy.deepcopy(self.DEFAULT_PROCESS['request']))
        if 'cookies_list' in request and request['cookies_list']:
            request['cookies'] = random.choice(request['cookies_list'])
            self.debug("%s parsed cookie: %s" % (self.__class__.__name__, request['cookies']))
            del request['cookies_list']
        if 'headers_list' in request and request['headers_list']:
            request['headers'] = random.choice(request['headers_list'])
            self.debug("%s parsed header: %s" % (self.__class__.__name__, request['headers']))
            del request['headers_list']
        if 'data' in request and request['data']:
            if 'hard_code' not in save or not save['hard_code']:
                rule = utils.array2rule(request.pop('data'), save['base_url'])
                parsed = utils.rule2parse(CustomParser, DEFAULT_SOURCE, save['base_url'], rule, self.log_level)
                self.debug("%s parsed data: %s" % (self.__class__.__name__, parsed))
                hard_code = request.get('request') or []
                for k, r in parsed.items():
                    hard_code.append({"mode": rule[k]['mode'], "name": k, "value": r})
                self.debug("%s parsed hard code: %s" % (self.__class__.__name__, hard_code))
                if hard_code:
                    request['hard_code'] = hard_code
            else:
                del request['data']
        else:
            request['data'] = {}
        return request

    def validate(self, rule=None, save={}):
        self.debug("%s validate start" % self.__class__.__name__)
        if not rule:
            rule = self.process.get("validate")
        self.debug("%s validate rule: %s" % (self.__class__.__name__, rule))
        if rule:
            url_rule = rule.get('url', '')
            if url_rule and utils.preg(self.response['final_url'], url_rule):
                self.response['broken_exc'] = CDSpiderCrawlerForbidden()
                return False
            ele_rule = rule.get('filter')
            if ele_rule:
                parser = CustomParser(source=self.response['content'], ruleset={"ele": {"filter": ele_rule}},
                                      log_level=self.log_level, url=self.response['final_url']);
                parsed = parser.parse()
                if parsed and "ele" in parsed and parsed["ele"]:
                    self.response['broken_exc'] =  CDSpiderCrawlerForbidden()
                return False
        self.debug("%s validate pass" % self.__class__.__name__)
        return True

    def preparse(self, rule, save={}):
        """
        解析预处理
        :param rule: 解析规则
        :return:
        """
        self.handler_run(HANDLER_FUN_PREPARSE, {"rule": rule, "save": save})
        return rule

    def parse(self, rule=None, save={}):
        """
        页面解析
        :param rule: 解析规则
        :return:
        """
        self.debug("%s parse start" % self.__class__.__name__)
        if not rule:
            rule = self.process.get("parse")
        self.debug("%s parse rule: %s" % (self.__class__.__name__, rule))
        rule = self.preparse(rule, save=save)
        self.debug("%s preparsed rule: %s" % (self.__class__.__name__, rule))

        if HANDLER_FUN_PARSE in self.handle:
            self.handler_run(HANDLER_FUN_PARSE, {"rule": rule, "save": save})
        else:
            self.run_parse(rule, save=save)
        self.handler_run(HANDLER_FUN_POSTPARSE, {"save": save})
        if not self.response['parsed']:
            self.response['broken_exc'] = CDSpiderCrawlerNoResponse()
        self.debug("%s parse end" % self.__class__.__name__)

    @abc.abstractmethod
    def run_parse(self, rule, save={}):
        """
        执行解析规则
        :param rule:
        :return:
        """
        # parser = CustomParser(source=self.response['content'], ruleset=copy.deepcopy(rule),
        # log_level=self.log_level, url = self.response['final_url'], attach_storage = self.attach_storage)
        # self.debug("%s parse start: %s @ %s" % (self.__class__.__name__, str(rule), self.mode))
        # parsed = parser.parse()
        # self.response['parsed'] = parsed
        pass

    def url_prepare(self, url):
        return url

    def build_url_by_rule(self, data, base_url = None):
        """
        根据url规则格式化url
        :param data 解析到的数据
        :param base_url 基本url
        """
        if not base_url:
            base_url = self.task.get('url')
        urlrule = self.process.get("url")
        self.debug("%s URL RULE: %s", self.__class__.__name__, urlrule)
        if urlrule and "parse" in urlrule:
            parser = CustomParser(source=self.response['content'], ruleset=urlrule['parse'])
            parsed = parser.parse()
            if not parsed:
                raise CDSpiderParserError(source=self.response['content'], rule=urlrule['parse'])
            if not isinstance(parsed, (list, tuple)):
                parsed = [parsed]
            parsed.extend(parsed[-1] for i in range(len(data) - len(parsed)))
        else:
            parsed = None
        formated = []
        i = 0
        for item in data:
            if 'url' not in item or not item['url']:
                continue
            if item['url'].startswith('javascript') or item['url'] == '/':
                continue
            try:
                item['url'] = self.url_prepare(item['url'])
            except:
                self.error(traceback.format_exc())
                continue
            if urlrule and 'name' in urlrule and urlrule['name']:
                if parsed:
                    params = parsed[i]
                else:
                    params = {}
                params.update({urlrule['name']: item['url']})
                item['url'] = utils.build_url_by_rule(urlrule, params)
            else:
                item['url'] = urljoin(base_url, item['url'])
            formated.append(item)
            i += 1
        return formated

    def on_repetition(self, save):
        """
        重复处理
        :param save: 传递的上下文
        :return:
        """
        self.debug("%s on repetition" % self.__class__.__name__)
        if self.crawl_info['crawl_count']['repeat_page'] < self.ALLOWED_REPEAT:
            return
        if HANDLER_FUN_REPETITION in self.handle:
            self.handler_run(HANDLER_FUN_REPETITION, {"save": save})
        else:
            raise CDSpiderCrawlerNoNextPage(base_url=save.get("base_url", self.task['url']), current_url=save.get(
                "request_url", self.task['url']))

    def on_error(self, exc, save):
        """
        错误处理
        :param exc: exception
        :param save: 传递的上下文
        :return:
        """
        self.debug("%s on error" % self.__class__.__name__)
        if isinstance(exc, CDSpiderError):
            self.error(exc.get_message())
        self.exception(exc)
        elid = 0
        if 'uuid' in self.task and self.task['uuid']:
            data = HandlerUtils.build_error_log(
                self.task['uuid'], self.mode, self.crawl_id, self.task.get('frequency', None),
                save.get('request_url', self.request_params.get('url', self.task['url'])), exc)
            elid = self.db['ErrorLogDB'].insert(data)
        elif 'parentid' in self.task and self.task['parentid']:
            data = HandlerUtils.build_error_log(
                self.task['parentid'], self.mode, self.crawl_id, self.task.get('frequency', None),
                save.get('request_url', self.request_params.get('url', self.task['url'])), exc)
            elid = self.db['ErrorLogDB'].insert(data)
        if elid == 0:
            self.crawl_info['exc'] = exc.__class__.__name__
            self.crawl_info['traceback'] = traceback.format_exc()
        else:
            self.crawl_info['errid'] = elid
        self.handler_run(HANDLER_FUN_ERROR, {"save": save})

    def on_result(self, save):
        """
        数据处理
        :param save: 传递的上下文
        """
        self.debug("%s on result" % self.__class__.__name__)
        if not self.response['parsed']:
            if self.page > 1:
                self.page -= 1
            if self.response['broken_exc']:
                raise self.response['broken_exc']
            if self.page > 1:
                raise CDSpiderCrawlerNoNextPage(
                    source=self.response['content'], base_url=save.get("base_url", ''),
                    current_url=save.get('request_url'))
            else:
                raise CDSpiderParserNoContent(
                    source=self.response['content'], base_url=save.get("base_url", ''),
                    current_url=save.get('request_url'))
        self.run_result(save)
        self.handler_run(HANDLER_FUN_RESULT, {"save": save})

    def run_result(self, save):
        """
        数据处理执行方法
        :param save: 传递的上下文
        :return:
        """
        pass

    def on_next(self, save):
        """
        下一页解析
        :param save: 传递的上下文
        :return:
        """
        self.page += 1
        request = copy.deepcopy(self.request)
        save['page'] = self.page
        rule = self.process.get("paging")
        self.debug("%s on next rule: %s" % (self.__class__.__name__, rule))
        rule = HandlerUtils.format_paging(copy.deepcopy(rule))
        self.debug("%s on next formated rule: %s" % (self.__class__.__name__, rule))
        if not rule:
            raise CDSpiderCrawlerNoNextPage(base_url=save.get("base_url", ''), current_url=save.get('request_url'))
        request = HandlerUtils.update_request(request, rule)
        builder = UrlBuilder(CustomParser, self.logger, self.log_level)
        self.request_params = builder.build(request, self.response['content'], self.response.get('cookies', {}),
                                            save)
        self.handler_run(HANDLER_FUN_NEXT, {"save": save})
        save['next_url'] = self.request_params['url']

    def on_continue(self, broken_exc, save):
        """
        重操作处理
        :param broken_exc: 中断执行的exception
        :param save: 传递的上下文
        :return:
        """
        # if isinstance(broken_exc, (CDSpiderCrawlerForbidden,)):
        if isinstance(self.crawler, RequestsCrawler):
            self.info('Change crawler to Tornado')
            self.crawler  = None
            self.crawler = utils.load_crawler('tornado', log_level=self.log_level)
        else:
            self.force_proxy = True
        if isinstance(broken_exc, (CDSpiderCrawlerProxyError, CDSpiderCrawlerProxyExpired)):
            data = {"addr": self.crawler.proxy_str, 'ctime': int(time.time())}
            typeinfo = utils.typeinfo(self.task['url'])
            data.update(typeinfo)
            self.db['proxy_log'].insert(data)
        else:
            self.auto_proxy = True
        if save['retry'] < self.MAX_RETRY:
            save['retry'] += 1
            self.handler_run(HANDLER_FUN_CONTINUE, {"broken_exc": broken_exc, "save": save})
            self.info('Retry to fetch: %s because of %s, current times: %s' % (self.request_params['url'],
                                                                               broken_exc, save['retry']))
        else:
            raise broken_exc

    def get_unique_setting(self, url, data):
        return HandlerUtils.get_unique_setting(self.process, url, data)

    def finish(self, save):
        """
        爬取结束处理
        :param save: 传递的上下文
        :return:
        """
        if self.log_id:
            log = HandlerUtils.build_update_log(self.crawl_info)
            self.log_id = self.db['CrawlLogDB'].update(self.log_id, log)
        self.handler_run(HANDLER_FUN_FINISH, {"save": save})

    @property
    def mode(self):
        name = re.sub(r'([A-Z])', lambda x: "-%s" % x.group(0).lower(), self.__class__.__name__.replace("Handler", ""))

        if name == '-general':
            return HANDLER_MODE_DEFAULT
        return name.replace("-general-", "").strip('-')

    @property
    def ns(self):
        return "%s_handler" % self.mode

    def extension(self, name, params, ns=None):
        if not ns:
            ns = self.ns
        return utils.run_extension(extension_ns="{0}.{1}".format(ns, name), data=params, handler=self)

    def close(self):
        """
        关闭
        :return:
        """
        if hasattr(self, 'crawler') and isinstance(self.crawler, BaseCrawler):
            del self.crawler
