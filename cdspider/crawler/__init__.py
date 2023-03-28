# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
import os
import abc
import time
import math
import urllib.request
import urllib.error
import traceback
import random
import re
import functools
import tornado.ioloop
from tornado import gen
from requests import cookies
from http import cookiejar as cookielib
from urllib.parse import *
from cdspider import Component
from cdspider.libs.tools import *
from cdspider.libs.constants import BROKEN_EXCEPTIONS


@six.add_metaclass(abc.ABCMeta)
class BaseCrawler(Component):
    """
    爬虫基类
    """
    STATUS_CODE_OK = 200
    STATUS_CODE_BAD_REQUEST = 400
    STATUS_CODE_FORBIDDEN = 403
    STATUS_CODE_NOT_FOUND = 404
    STATUS_CODE_INTERNAL_ERROR = 500
    STATUS_CODE_GATEWAY_TIMEOUT = 504

    _proxy = None
    __proxy_str = None

    default_options = {
        "method": "GET",
        'headers': {
        }
    }

    # 默认User-Agent
    user_agent = [
                "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 "
                "Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:62.0) Gecko/20100101 Firefox/62.0",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134",
                "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; LCTE; rv:11.0) like Gecko"
    ]

    # proxy setting
    proxies_setting = {
        'proxy_frequency': None,
        'proxies': None,
        'proxy_file': None,
        'proxy_url': None,
    }

    def __init__(self, *args, **kwargs):
        """
        实例化类时如果传入参数url, 则会以传入的url为参数执行crawl
        :param url: base url
        :param logger: logger
        :param log_level: logging level
        """
        if len(args) > 0:
            kwargs.setdefault('url', args[0])
        self._base_url = kwargs.get("url")
        self._referer = None
        self._cookies = cookies.RequestsCookieJar()
        self.async_ = kwargs.get('async', True)
        self.logger = kwargs.pop('logger', logging.getLogger('crawler'))
        log_level = kwargs.pop('log_level', logging.WARN)
        super(BaseCrawler, self).__init__(self.logger, log_level)
        self.fetch = copy.deepcopy(self.default_options)
        self.proxy_lock = None
        if kwargs:
            self._prepare_setting(**kwargs)
        self.ioloop = kwargs.get('ioloop', tornado.ioloop.IOLoop())

    def _prepare_setting(self, **kwargs):
        """
        基础设置
        :param headers: header dict
        :param cookies: cookie list
        :param encoding: character set
        :param proxy: proxy
        """
        headers = dict()
        headers['User-Agent'] = random.choice(self.user_agent)
        if 'headers' in kwargs and kwargs['headers']:
            headers.update(kwargs['headers'])

        for n, v in utils.mgkeyconvert(headers, True).items():
            self.set_header(n, v)

        if 'cookies' in kwargs and kwargs['cookies']:
            if isinstance(kwargs['cookies'], list):
                for item in kwargs['cookies']:
                    if isinstance(item, cookielib.Cookie):
                        self.set_cookie(name=item.name, value=item.value, path=item.path, domain=item.domain)
                    elif isinstance(item, dict):
                        if 'name' in item and 'value' in item:
                            self.set_cookie(**item)
                        else:
                            for k, v in item.items():
                                self.set_cookie(name=k, value=v)
            elif isinstance(kwargs['cookies'], cookielib.Cookie):
                self.set_cookie(
                    name=kwargs['cookies'].name, value=kwargs['cookies'].value, path=kwargs['cookies'].path,
                    domain=kwargs['cookies'].domain)
            elif isinstance(kwargs['cookies'], dict):
                if 'name' in kwargs['cookies'] and 'value' in kwargs['cookies']:
                    self.set_cookie(**kwargs['cookies'])
                else:
                    for k,v in utils.mgkeyconvert(kwargs['cookies'], True).items():
                        self.set_cookie(name=k, value=v)
        kwargs.setdefault('encoding', 'utf-8')
        self._encoding = kwargs['encoding']
        self._proxy = kwargs.get('proxy', None)
        if isinstance(self._proxy, six.string_types):
            self._proxy = json.loads(json.dumps(eval(self._proxy)))

    def _join_url(self, url):
        """
        join url by base url
        :param url: url
        :return: url
        """
        if self._referer:
            return urljoin(self._referer, url)
        if self._base_url:
            return urljoin(self._base_url, url)
        return url

    def fetch(self, *args, **kwargs):
        """
        fetch
        :param args:
        :param kwargs:
        :return:
        """
        l = len(args)
        if l > 0:
            kwargs.setdefault('url', args[0])
        if l > 1:
            kwargs.setdefault('method', args[1])
        kwargs.setdefault('method', 'get')
        callback = kwargs.pop("callback", None)
        self._base_url = kwargs['url']
        self._prepare_setting(**kwargs)
        fetch = self.parse_fetch(**kwargs)
        self.info("Requests crawl params: %s" % fetch)
        if self.async_:
            return self.async_fetch(kwargs['url'], fetch, callback)
        else:
            return self.async_fetch(kwargs['url'], fetch, callback).result()

    def crawl(self, *args, **kwargs):
        """
        抓取
        :param args:
        :param kwargs:
        :return:
        """
        l = len(args)
        if l > 0:
            kwargs.setdefault('url', args[0])
        if l > 1:
            kwargs.setdefault('method', args[1])
        kwargs.setdefault('method', 'get')
        callback = kwargs.pop("callback", None)
        self._base_url = kwargs['url']
        self._prepare_setting(**kwargs)
        fetch = self.parse_fetch(**kwargs)
        self.info("Requests crawl params: %s" % fetch)
        result = self.sync_fetch(kwargs['url'], fetch)
        if result:
            self._referer = result['url']
        if callback:
            callback(result)
        return result

    def parse_fetch(self, **kwargs):
        """
        解析抓取参数
        :param kwargs: 抓取时的参数
        """
        return kwargs

    def sync_fetch(self, url, fetch):
        """
        Synchronization fetch, usually used in xmlrpc thread
        :param url:
        :param fetch:
        :return:
        """
        return self.ioloop.run_sync(functools.partial(self.async_fetch, url, fetch, lambda r: True))

    @gen.coroutine
    def async_fetch(self, url, fetch, callback=None):
        """
        Do one fetch
        """
        start_time = time.time()
        if callback is None:
            callback = lambda x: True
        try:
            result = yield self.http_fetch(url, fetch)
        except Exception as e:
            self.exception(e)
            result = self.handle_error(url, start_time, e)
        callback(result)
        raise gen.Return(result)

    @abc.abstractmethod
    def http_fetch(self, url, fetch):
        pass

    def set_header(self, name, value):
        self.fetch['headers'][name] = utils.quote_chinese(value)

    def set_cookie(self, name, value, **kwargs):
        """
        设置cookie
        """
        self.info("Requests request set cookie: name:%s, value:%s, %s" % (str(name), str(value), str(kwargs)))
        if 'httponly' in kwargs:
            kwargs['rest'] = {'HttpOnly': kwargs['httponly']}
        if 'expiry' in kwargs:
            kwargs['expires'] = kwargs['expiry']
        allowed = dict(
            version=0,
            port=None,
            domain='',
            path='/',
            secure=False,
            expires=None,
            discard=True,
            comment=None,
            comment_url=None,
            rest={'HttpOnly': None},
            rfc2109=False,)
        result = utils.dictunion(kwargs, allowed)
        if ('domain' not in result or not result['domain']) and self._base_url:
            result['domain'] = utils.typeinfo(self._base_url)["domain"]

        return self._cookies.set(name, str(value), **result)

    @staticmethod
    def handle_error(url, start_time, error):
        result = {
            'status_code': getattr(error, 'code',  599),
            'error': error,
            'traceback': traceback.format_exc() if sys.exc_info()[0] else None,
            'content': "",
            'time': time.time() - start_time,
            'orig_url': url,
            'url': url,
        }
        return result

    def _prepare_response(self, code, url):
        """
        预处理response
        """

        if code == self.STATUS_CODE_NOT_FOUND:
            return CDSpiderCrawlerNotFound(None, self._base_url, url, code=code)
        elif code == self.STATUS_CODE_FORBIDDEN:
            return CDSpiderCrawlerForbidden(None, self._base_url, url, code=code)
        elif code == self.STATUS_CODE_INTERNAL_ERROR:
            return CDSpiderCrawlerRemoteServerError(None, self._base_url, url, code=code)
        elif code == self.STATUS_CODE_BAD_REQUEST:
            return CDSpiderCrawlerBadRequest(None, self._base_url, url, code=code)
        elif code == self.STATUS_CODE_GATEWAY_TIMEOUT:
            return CDSpiderCrawlerConnectTimeout(None, self._base_url, url, code=code)
        elif code != self.STATUS_CODE_OK:
            return CDSpiderCrawlerError(None, self._base_url, url, code=code)
        return None

    @abc.abstractmethod
    def set_proxy(self, addr, type='http', user=None, password=None):
        """
        设置代理
        :param addr: proxy addr ex: 127.0.0.1:8080
        :param type: proxy type
        :param user: proxy user
        :param password: proxy password
        """
        pass

    @staticmethod
    def sleep(interval):
        """
        睡眠
        :param interval: 睡眠间隔
        :return:
        """
        if isinstance(interval, (list, tuple)):
            interval = random.randint(interval[0], interval[1])
        elif interval >= 30:
            interval = random.randint(math.ceil(interval / 2), interval)
        time.sleep(interval)

    def parse_proxy(self, *args, **kwargs):
        """
        代理设置解析
        :param init: 初始化代理，default: True
        :param proxy_frequency: 代理使用频率, default: always
        :param addr: proxy addr
        :param type: proxy type
        :param user: proxy user
        :param password: proxy password
        :param proxies: proxy list
        :param proxy_file: proxy file
        :param proxy_url: proxy url
        """
        init = kwargs.get('init', True)
        proxy_frequency = kwargs.get('proxy_frequency', 'always')
        if (not init and proxy_frequency == 'always') or self.proxy_lock is not None:
            return
        proxies = utils.dictunion(kwargs, {'addr': None, 'type': None, 'user': None, 'password': None})
        if 'proxies' in kwargs and kwargs['proxies']:
            proxy_setting = kwargs['proxies']
            if isinstance(proxy_setting, dict):
                proxies.update(kwargs['proxies'])
            elif isinstance(proxy_setting, list):
                proxies['proxies'] = proxy_setting
            else:
                proxies['addr'] = proxy_setting
        elif 'proxy_file' in kwargs and kwargs['proxy_file']:
            if os.path.isfile(kwargs['proxy_file']):
                f = open(kwargs['proxy_file'], 'rb')
                c = f.readline().decode()
                f.close()
                if len(c) > 0:
                    proxies['proxies'] = c.split('|')
        elif 'proxy_url' in kwargs and kwargs['proxy_url']:
            try:
                c = urllib.request.urlopen(kwargs['proxy_url'], timeout=5).read().decode()
                if len(c) > 0:
                    proxies['proxies'] = c.split('|')
            except urllib.error.URLError:
                self.logger.error(traceback.format_exc())
        self.proxy_lock = True
        if 'proxies' in proxies:
            proxies['proxies'].append("127.0.0.1")
            proxy = random.choice(proxies['proxies'])
            if proxy == "127.0.0.1":
                self.set_proxy(None)
                return
            if isinstance(proxy, dict):
                proxies.update(proxy)
            else:
                proxies['addr'] = proxy
        elif len(args) > 0:
            proxies['addr'] = args[0]
        if "addr" in proxies and proxies['addr']:
            self.__proxy_str = proxies['addr']
            g = re.search(r'(\d+\.\d+\.\d+\.\d+):(\d+)', proxies['addr'])
            if g and g.groups():
                gs = g.groups()
                if gs[0] and gs[1]:
                    proxies['addr'] = "%s:%s" % (gs[0], gs[1])
                    proxies.setdefault("type", "http")
                    data = utils.dictunion(proxies, {'addr': None, 'type': None, 'user': None, 'password': None})
                    self.info('Proxy: %s' % data)
                    self.set_proxy(**data)

    @staticmethod
    def broken(type_, message=None):
        """
        中断操作
        :param type_: 中断类型
        :param message: 中断时的消息
        """
        if type_ in BROKEN_EXCEPTIONS:
            raise BROKEN_EXCEPTIONS[type_](message)
        raise CDSpiderCrawlerError("Invalid broken setting")

    def gen_result(self, url, code, headers, cookies, content, start_time, error=None, iframe=None):
        result = dict()
        result['orig_url'] = self._base_url
        result['content'] = utils.decode(content) if content else ''
        result['iframe'] = [utils.decode(item) for item in iframe] if iframe is not None and len(iframe) > 0 else None
        result['headers'] = headers
        result['status_code'] = code
        result['url'] = url
        result['time'] = time.time() - start_time
        result['cookies'] = cookies
        if error:
            result['error'] = utils.text(error)

        raise gen.Return(result)


from .RequestsCrawler import RequestsCrawler
from .SeleniumCrawler import SeleniumCrawler
from .TornadoCrawler import TornadoCrawler
