# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:39:24
"""
import socks
import requests
import time
import copy
import urllib3
from tornado import gen
from requests import cookies
from urllib3.exceptions import InsecureRequestWarning
from requests.exceptions import *
from cdspider.crawler import BaseCrawler
from cdspider.exceptions import *
from cdspider.libs import utils

urllib3.disable_warnings(InsecureRequestWarning)


class RequestsCrawler(BaseCrawler):
    """
    requests模块爬虫类
    """

    default_options = {
        "method": "GET",
        'headers': {
        },
        'auth': None,
        'timeout': (30, 60),
        'allow_redirects': True,
        'proxies': None,
        'hooks': None,
        'stream': None,
        'verify': False,
        'cert': None,
    }

    def __init__(self, *args, **kwargs):
        """
        init
        :param args:
        :param kwargs:
        """
        super(RequestsCrawler, self).__init__(*args, **kwargs)
        self._ses = requests.sessions.Session()

    def _request(self, url, fetch):
        """
        发送请求并获取结果
        """
        try:
            req = requests.Request(
                method=fetch.get('method'),
                url=fetch.get('url'),
                headers=fetch.get('headers'),
                files=fetch.get('files'),
                data=fetch.get('data'),
                params=fetch.get('params'),
                auth=fetch.get('auth'),
                cookies=self._cookies,
                hooks=fetch.get('hooks'),
                json=fetch.get('json')
            )
            with self._ses as s:
                prepped = s.prepare_request(req)
                return s.send(
                    prepped,
                    stream=fetch.get('stream'),
                    timeout=fetch.get('timeout'),
                    verify=fetch.get('verify'),
                    cert=fetch.get('cert'),
                    proxies=fetch.get('proxies'),
                    allow_redirects=fetch.get('allow_redirects')
                )
        except (TimeoutError, socks.ProxyConnectionError) as e:
            raise CDSpiderCrawlerProxyError(e, self._base_url, url, setting=fetch)
        except ConnectTimeout as e:
            raise CDSpiderCrawlerConnectTimeout(e, self._base_url, url, settings=fetch)
        except ReadTimeout as e:
            raise CDSpiderCrawlerReadTimeout(e, self._base_url, url, settings=fetch)
        except Timeout as e:
            raise CDSpiderCrawlerTimeout(e, self._base_url, url, settings=fetch)
        except ConnectionError as e:
            if self._setting.get('proxies'):
                raise CDSpiderCrawlerProxyExpired(e, self._base_url, url, settings=fetch)
            raise CDSpiderCrawlerConnectionError(e, self._base_url, url, settings=fetch)
        except Exception as e:
            raise CDSpiderCrawlerError(e, self._base_url, url, settings=fetch)

    def parse_fetch(self, **kwargs):
        """
        解析抓取参数
        :param kwargs:
        :return:
        """
        if self._proxy:
            self.parse_proxy(**self._proxy)
            self._proxy['init'] = False
        fetch_ = copy.deepcopy(self.fetch)
        fetch_['method'] = kwargs['method'].upper()
        fetch_['url'] = utils.quote_chinese(kwargs['url'])
        fetch_['data'] = kwargs.get('data')
        fetch_['json'] = kwargs.get('json')
        fetch_['params'] = kwargs.get('params')
        return fetch_

    @gen.coroutine
    def http_fetch(self, url, fetch):
        """
        HTTP fetcher
        """
        start_time = time.time()

        def handle_error(x):
            BaseCrawler.handle_error(url, start_time, x)

        # making requests
        while True:
            try:
                response = yield gen.maybe_future(self._request(url, fetch))
            except HTTPError as e:
                if e.response:
                    response = e.response
                else:
                    raise gen.Return(handle_error(e))
            except Exception as e:
                raise gen.Return(handle_error(e))

            cookies.extract_cookies_to_jar(self._cookies, response.request, response)

            error = self._prepare_response(response.status_code, url)
            if error is not None:
                raise gen.Return(handle_error(error))
            self.gen_result(
                url=response.url or url,
                code=response.status_code,
                headers=dict(response.headers),
                cookies=self._cookies.get_dict(),
                content=response.content or '',
                start_time=start_time)

    def set_proxy(self, addr, type='http', user=None, password=None):
        """
        设置代理
        """
        if addr is None:
            self.fetch['proxies'] = None
        else:
            if user:
                if password:
                    user += ':' + password
                proxy = user + '@' + addr
            else:
                proxy = addr
            if type == "socks":
                proxies = {"http": "socks5://" + proxy, "https": "socks5://" + proxy}
            elif type == "ftp":
                proxies = {"http": "ftp://" + proxy, "https": "ftp://" + proxy}
            elif type == "http":
                proxies = {"http": "http://" + proxy, "https": "http://" + proxy}
            else:
                proxies = {"http": "https://" + proxy, "https": "https://" + proxy}
            self.fetch["proxies"] = proxies


if __name__ == "__main__":
    crawler = RequestsCrawler()

    def f(result):
        print(result)
    fetch = {
        "method": "GET",
        "headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                                  "Chrome/73.0.3683.103 Safari/537.36"},
        "url": "http://www.bast.net.cn/art/2019/4/8/art_16644_401747.html",
        "callback": f
    }
    crawler.crawl(**fetch)