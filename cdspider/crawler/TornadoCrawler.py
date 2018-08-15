#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-15 0:54:49
"""
import sys
import time
import copy
import traceback
import pycurl
import six
import functools
import threading
import tornado.ioloop
import tornado.httputil
import tornado.httpclient
from requests import cookies
from tornado import gen
from six.moves import queue, http_cookies
from six.moves.urllib.parse import urljoin, urlsplit
from tornado.curl_httpclient import CurlAsyncHTTPClient
from tornado.simple_httpclient import SimpleAsyncHTTPClient
from cdspider.crawler import BaseCrawler
from cdspider.exceptions import *
from cdspider.libs import utils

def prepare_curl_socks5(curl):
    curl.setopt(pycurl.PROXYTYPE, pycurl.PROXYTYPE_SOCKS5)

class MyCurlAsyncHTTPClient(CurlAsyncHTTPClient):

    def free_size(self):
        return len(self._free_list)

    def size(self):
        return len(self._curls) - self.free_size()


class MySimpleAsyncHTTPClient(SimpleAsyncHTTPClient):

    def free_size(self):
        return self.max_clients - self.size()

    def size(self):
        return len(self.active)


class TornadoCrawler(BaseCrawler):
    """
    Tornado Crawler
    """
    default_options = {
        'method': 'GET',
        'headers': {
        },
        'use_gzip': True,
        'timeout': 120,
        'connect_timeout': 20,
        'validate_cert': False,
    }

    def __init__(self, *args, **kwargs):
        self.fetch = copy.deepcopy(self.default_options)
        self.result = None
        self._cookies = cookies.RequestsCookieJar()
        self.poolsize = kwargs.get('poolsize', 100)
        self.async = kwargs.get('async', True)
        self.ioloop = tornado.ioloop.IOLoop()
        self.max_redirects = 5
        if self.async:
            self.http_client = MyCurlAsyncHTTPClient(max_clients=self.poolsize, io_loop=self.ioloop)
        else:
            self.http_client = tornado.httpclient.HTTPClient(MyCurlAsyncHTTPClient, max_clients=self.poolsize)
        super(TornadoCrawler, self).__init__(*args, **kwargs)

    def close(self):
        pass

    def quit(self):
        pass

    def _prefetch(self, **kwargs):
        if self._proxy:
            self.parse_proxy(**self._proxy)
            self._proxy['init'] = False
        self.max_redirects = kwargs.get('max_redirects', self.max_redirects)
        fetch = copy.deepcopy(self.fetch)
        fetch['method'] = kwargs['method'].upper()
        fetch['url'] = utils.quote_chinese(kwargs['url'])
        fetch['headers'] = tornado.httputil.HTTPHeaders(fetch['headers'])

        if kwargs.get('last_modified', kwargs.get('last_modifed', True)):
            last_modified = kwargs.get('last_modified', kwargs.get('last_modifed', True))
            _t = None
            if isinstance(last_modified, six.string_types):
                _t = last_modified
            if _t and 'If-Modified-Since' not in fetch['headers']:
                fetch['headers']['If-Modified-Since'] = _t
        # timeout
        if 'timeout' in fetch:
            fetch['request_timeout'] = fetch['timeout']
            del fetch['timeout']
        # data rename to body
        if 'data' in fetch:
            fetch['body'] = fetch['data']
            del fetch['data']

        return fetch

    def set_proxy(self, addr, type = 'http', user = None, password = None):
        """
        设置代理
        """
        if addr:
            if user:
                if password:
                    user += ':' + password
                proxy_string = user +'@'+ addr
            else:
                proxy_string = addr
            if type == "socks":
                self.fetch['prepare_curl_callback'] = prepare_curl_socks5
            else:
                self.fetch['proxy_type'] = type
            proxy_string = type +'://' + proxy_string
            proxy_splited = urlsplit(proxy_string)
            self.fetch['proxy_host'] = proxy_splited.hostname
            if proxy_splited.username:
                self.fetch['proxy_username'] = proxy_splited.username
            if proxy_splited.password:
                self.fetch['proxy_password'] = proxy_splited.password
            self.fetch['proxy_port'] = proxy_splited.port or 8080

    def get_header(self, name = None):
        """
        获取header，不指定name时，获取全部header
        """
        if name:
            return self.fetch['headers'].get(name, None)
        return self.fetch['headers']

    def set_header(self, name, value):
        self.fetch['headers'][name] = value

    def get_cookie(self, name = None):
        """
        获取Response cookie，不指定name时，获取全部cookie
        """
        self._cookies.clear_expired_cookies()
        if name:
            return self._cookies.get(name, **kwargs)
        return [item for item in self._cookies]

    def set_cookie(self, name, value, **kwargs):
        """
        设置cookie
        """
        self.info("Requests request set cookie: name:%s, value:%s, %s" % (str(name),
            str(value), str(kwargs)))
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
        return self._cookies.set(name, str(value), **result)

    def crawl(self, *args, **kwargs):
        l = len(args)
        if l > 0:
            kwargs.setdefault('url', args[0])
        if l > 1:
            kwargs.setdefault('method', args[1])
        kwargs.setdefault('method', 'get')
        self._prepare_setting(**kwargs)
        fetch = self._prefetch(**kwargs)
        self.info("Requests crawl params: %s" % kwargs)
        self.result = self.sync_fetch(kwargs['url'], fetch)

    def wait(self, *args, **kwargs):
        pass

    @property
    def page_source(self):
        """
        获取文章源码
        """
        if self.result['status_code'] == 200:
            return utils.decode(self.result['content'])
        raise CDSpiderCrawlerNoResponse(base_url = self.result['orig_url'],
            response = self.result)

    @property
    def final_url(self):
        return self.result['url']

    @property
    def content(self):
        if self.result['status_code'] == 200:
            return self.result['content']
        raise CDSpiderCrawlerNoResponse(base_url = self._base_url,
            response = self._response)

    def handle_error(self, url, start_time, error):
        result = {
            'status_code': getattr(error, 'code',  599),
            'error': utils.text(error),
            'traceback': traceback.format_exc() if sys.exc_info()[0] else None,
            'content': "",
            'time': time.time() - start_time,
            'orig_url': url,
            'url': url,
        }
        return result

    def fetch(self, url, fetch, callback):
        if self.async:
            return self.async_fetch(url, fetch, callback)
        else:
            return self.async_fetch(url, fetch, callback).result()

    def sync_fetch(self, url, fetch):
        '''Synchronization fetch, usually used in xmlrpc thread'''

        return self.ioloop.run_sync(functools.partial(self.async_fetch, url, fetch, lambda r: True))

        wait_result = threading.Condition()
        _result = {}

        def callback(result):
            wait_result.acquire()
            _result['result'] = result
            wait_result.notify()
            wait_result.release()

        wait_result.acquire()
        self.ioloop.add_callback(self.fetch, url, fetch, callback)
        while 'result' not in _result:
            wait_result.wait()
        wait_result.release()
        return _result['result']

    @gen.coroutine
    def async_fetch(self, url, fetch, callback):
        '''Do one fetch'''
        start_time = time.time()
        try:
            result = yield self.http_fetch(url, fetch)
        except Exception as e:
            self.exception(e)
            result = self.handle_error(url, start_time, e)
        callback(result)
        raise gen.Return(result)

    @gen.coroutine
    def http_fetch(self, url, fetch):
        '''HTTP fetcher'''
        start_time = time.time()
        handle_error = lambda x: self.handle_error(url, start_time, x)

        max_redirects = self.max_redirects
        # we will handle redirects by hand to capture cookies
        fetch['follow_redirects'] = False

        # making requests
        while True:
            try:
                request = tornado.httpclient.HTTPRequest(**fetch)
                # if cookie already in header, get_cookie_header wouldn't work
                old_cookie_header = request.headers.get('Cookie')
                if old_cookie_header:
                    del request.headers['Cookie']
                cookie_header = cookies.get_cookie_header(self._cookies, request)
                if cookie_header:
                    request.headers['Cookie'] = cookie_header
                elif old_cookie_header:
                    request.headers['Cookie'] = old_cookie_header
            except Exception as e:
                self.exception(e)
                raise gen.Return(handle_error(e))

            try:
                response = yield gen.maybe_future(self.http_client.fetch(request))
            except tornado.httpclient.HTTPError as e:
                if e.response:
                    response = e.response
                else:
                    raise gen.Return(handle_error(e))

            cookies.extract_cookies_to_jar(self._cookies, response.request, response)
            if (response.code in (301, 302, 303, 307)
                    and response.headers.get('Location')):
                if max_redirects <= 0:
                    error = tornado.httpclient.HTTPError(
                        599, 'Maximum (%d) redirects followed' % task_fetch.get('max_redirects', 5),
                        response)
                    raise gen.Return(handle_error(error))
                if response.code in (302, 303):
                    fetch['method'] = 'GET'
                    if 'body' in fetch:
                        del fetch['body']
                fetch['url'] = utils.quote_chinese(urljoin(fetch['url'], response.headers['Location']))
                fetch['request_timeout'] -= time.time() - start_time
                if fetch['request_timeout'] < 0:
                    fetch['request_timeout'] = 0.1
                max_redirects -= 1
                continue

            result = {}
            result['orig_url'] = url
            result['content'] = response.body or ''
            result['headers'] = dict(response.headers)
            result['status_code'] = response.code
            result['url'] = response.effective_url or url
            result['time'] = time.time() - start_time
            result['cookies'] = self._cookies.get_dict()
            if response.error:
                result['error'] = utils.text(response.error)
            if 200 <= response.code < 300:
                self.info("[%d] %s %.2fs", response.code,
                            url, result['time'])
            else:
                self.warning("[%d] %s %.2fs", response.code,
                               url, result['time'])

            raise gen.Return(result)
