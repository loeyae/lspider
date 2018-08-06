#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.
import os
import abc
import six
import logging
import urllib.request
import traceback
import random
import re
from http import cookiejar as cookielib
from urllib.parse import *
from cdspider.libs import utils
from cdspider.exceptions import *
from cdspider.libs.tools import *

@six.add_metaclass(abc.ABCMeta)
class BaseCrawler(object):
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

    user_agent = "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36"

    proxies_setting = {
        'proxy_rate': None,
        'proxies': None,
        'proxy_file': None,
        'proxy_url': None,
    }

    def __init__(self, *args, **kwargs):
        """
        实例化类时如果传入参数url, 则会以传入的url为参数执行crawl
        """
        if len(args) > 0:
            kwargs.setdefault('url', args[0])
        self.logger = kwargs.pop('logger', logging.getLogger('crawler'))
        log_level = kwargs.pop('log_level', logging.WARN)
        self.logger.setLevel(log_level)
        self._prepare_setting(**kwargs)

        if "url" in kwargs and kwargs['url']:
            self.crawl(url)

    def _prepare_setting(self, **kwargs):
        """
        基础设置
        """
        headers = {}
        headers['User-Agent'] = self.user_agent
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
                            for k,v in item.items():
                                self.set_cookie(name=k, value=v)
            elif isinstance(kwargs['cookies'], cookielib.Cookie):
                self.set_cookie(name=kwargs['cookies'].name, value=kwargs['cookies'].value,
                    path=kwargs['cookies'].path, domain=kwargs['cookies'].domain)
            elif isinstance(kwargs['cookies'], dict):
                if 'name' in kwargs['cookies'] and 'value' in kwargs['cookies']:
                    self.set_cookie(**kwargs['cookies'])
                else:
                    for k,v in utils.mgkeyconvert(kwargs['cookies'], True).items():
                        self.set_cookie(name=k, value=v)
        kwargs.setdefault('encoding', 'utf-8')
        self._encoding = kwargs['encoding']
        self._proxy = kwargs.get('proxy', None)

    def _join_url(self, url):
        """
        join url
        """
        if self._referer:
            return urljoin(self._referer, url)
        return urljoin(self._base_url, url)

    @abc.abstractmethod
    def crawl(self, *args, **kwargs):
        """
        抓取操作。由于__init__时，如果传入url参数会执行该方法，在实现该方法时默认的第一个参数必须为接收url的参数
        """
        pass

    @abc.abstractmethod
    def wait(self, **kwargs):
        """
        等待操作
        """
        pass

    @abc.abstractmethod
    def get_cookie(self, name = None):
        """
        获取cookie，不指定name时，获取全部cookie
        """
        pass

    @abc.abstractmethod
    def set_cookie(self, name, value, **kwargs):
        """
        设置cookie
        """
        pass

    @abc.abstractmethod
    def get_header(self, name = None):
        """
        获取header，不指定name时，获取全部header
        """
        pass

    @abc.abstractmethod
    def set_header(self, name, value):
        """
        设置header
        """
        pass

    @property
    def page_source(self):
        raise NotImplementedError

    @property
    def content(self):
        return None

    @property
    def final_url(self):
        raise NotImplementedError

    @abc.abstractmethod
    def set_proxy(self, addr, type = 'http', user = None, passwrod = None):
        """
        设置代理
        """
        pass

    def quit(self):
        raise NotImplementedError

    def sleep(self, interval):
        if isinstance(interval, (list, tuple)):
            interval = random.randint(interval[0], interval[1])
        elif interval >= 30:
            interval = random.randint(math.ceil(interval / 2), interval)
        time.sleep(interval)

    def parse_proxy(self, *args, **kwargs):
        """
        代理设置解析
        """
        init = kwargs.get('init', True)
        proxy_rate = kwargs.get('proxy_rate', 'always')
        if not init and proxy_rate == 'always':
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
                c = str(f.readline())
                f.close()
                if len(c) > 0:
                    proxies['proxies'] = c.split('|')
        elif 'proxy_url' in kwargs and kwargs['proxy_url']:
            try:
                c = urllib.request.urlopen(kwargs['proxy_url'], timeout=5).read().decode()
                if len(c) > 0:
                    proxies['proxies'] = c.split('|')
            except:
                self.logger.error(traceback.format_exc())
                pass
        if 'proxies' in proxies:
            proxies['proxies'].append("127.0.0.1")
            proxy = random.choice(proxies['proxies'])
            if proxy == "127.0.0.1":
                return
            if isinstance(proxy, dict):
                proxies.update(proxy)
            else:
                proxies['addr'] = proxy
        elif len(args) > 0:
            proxies['addr'] = args[0]
        if "addr" in proxies and proxies['addr']:
            g = re.search('(\d+\.\d+\.\d+.\d+):(\d+)', proxies['addr'])
            if g and g.groups():
                gs = g.groups()
                if gs[0] and gs[1]:
                    proxies['addr'] = "%s:%s" % (gs[0], gs[1])
                    proxies.setdefault("type", "http")
                    data = utils.dictunion(proxies, {'addr': None, 'type': None, 'user': None, 'password': None})
                    self.logger.info('Proxy: %s' % data)
                    self.set_proxy(**data)

    def broken(self, type, message = None):
        """
        中断操作
        """
        if type in BROKEN_EXCEPTIONS:
            raise BROKEN_EXCEPTIONS[type](message)
        raise CDSpiderCrawlerError("Invalid broken setting")


from .RequestsCrawler import RequestsCrawler
from .SeleniumCrawler import SeleniumCrawler
