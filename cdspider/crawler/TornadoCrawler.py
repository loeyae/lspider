# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-8-15 0:54:49
"""
import time
import copy
import pycurl
import six
import tornado.ioloop
import tornado.httputil
import tornado.httpclient
from requests import cookies
from tornado import gen
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
        'timeout': 60,
        'connect_timeout': 30,
        'validate_cert': False,
    }

    def __init__(self, *args, **kwargs):
        self.poolsize = kwargs.get('poolsize', 2)
        self.max_redirects = 5
        super(TornadoCrawler, self).__init__(*args, **kwargs)
        if self.async_:
            self.http_client = MyCurlAsyncHTTPClient(max_clients=self.poolsize, io_loop=self.ioloop)
        else:
            self.http_client = tornado.httpclient.HTTPClient(MyCurlAsyncHTTPClient, max_clients=self.poolsize)

    def parse_fetch(self, **kwargs):
        """
        parse fetch
        :param kwargs:
        :return:
        """
        if self._proxy:
            self.parse_proxy(**self._proxy)
            self._proxy['init'] = False
        self.max_redirects = kwargs.get('max_redirects', self.max_redirects)
        fetch_ = copy.deepcopy(self.fetch)
        fetch_['method'] = kwargs['method'].upper()
        fetch_['url'] = utils.quote_chinese(kwargs['url'])
        fetch_['headers'] = tornado.httputil.HTTPHeaders(fetch_['headers'])

        if kwargs.get('last_modified', kwargs.get('last_modifed', True)):
            last_modified = kwargs.get('last_modified', kwargs.get('last_modifed', True))
            _t = None
            if isinstance(last_modified, six.string_types):
                _t = last_modified
            if _t and 'If-Modified-Since' not in fetch_['headers']:
                fetch_['headers']['If-Modified-Since'] = _t
        # timeout
        if 'timeout' in fetch_:
            fetch_['request_timeout'] = fetch_['timeout']
            del fetch_['timeout']
        # data rename to body
        if fetch_['method'] == 'POST' and 'data' in kwargs:
            fetch_['body'] = utils.url_encode(kwargs['data'])

        return fetch_

    def set_proxy(self, addr, type='http', user = None, password = None):
        """
        设置代理
        """
        if addr:
            if user:
                if password:
                    user += ':' + password
                proxy_string = user + '@' + addr
            else:
                proxy_string = addr
            if type == "socks":
                self.fetch['prepare_curl_callback'] = prepare_curl_socks5
            else:
                self.fetch['proxy_type'] = type
            proxy_string = type + '://' + proxy_string
            proxy_splited = urlsplit(proxy_string)
            self.fetch['proxy_host'] = proxy_splited.hostname
            if proxy_splited.username:
                self.fetch['proxy_username'] = proxy_splited.username
            if proxy_splited.password:
                self.fetch['proxy_password'] = proxy_splited.password
            self.fetch['proxy_port'] = proxy_splited.port or 8080

    @gen.coroutine
    def http_fetch(self, url, fetch):
        """
        HTTP fetcher
        """
        start_time = time.time()

        def handle_error(x):
            BaseCrawler.handle_error(url, start_time, x)

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
            except Exception as e:
                raise gen.Return(handle_error(e))
            cookies.extract_cookies_to_jar(self._cookies, response.request, response)

            if response.code in (301, 302, 303, 307) and response.headers.get('Location'):
                if max_redirects <= 0:
                    error = CDSpiderCrawlerBadRequest(
                        599, 'Maximum (%d) redirects followed' % fetch.get('max_redirects', 5),
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
            else:
                error = self._prepare_response(response.code, url)
                if error is not None:
                    raise gen.Return(handle_error(error))
            self.gen_result(
                url=response.effective_url or url,
                code=response.code,
                headers=dict(response.headers),
                cookies=self._cookies.get_dict(),
                content=response.body or '',
                start_time=start_time,
                error=response.error)


if __name__ == "__main__":
    crawler = TornadoCrawler()

    def f(result):
        print(result)
    fetch = {
        "method": "GET",
        "url": "http://www.ip138.com/",
        "callback": f
    }
    crawler.crawl(**fetch)
