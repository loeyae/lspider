#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-9 17:39:24
"""
import requests
import time
from urllib.parse import *
from pyquery import PyQuery
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.exceptions import *
from cdspider.crawler import BaseCrawler
from cdspider.exceptions import *
from cdspider.libs import utils

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class RequestsCrawler(BaseCrawler):
    """
    requests模块爬虫类
    """

    def __init__(self, *args, **kwargs):
        self._ses = requests.Session()
        self._cookies = self._ses.cookies
        self._headers = self._ses.headers
        self._base_url = None
        self._referer = None
        super(RequestsCrawler, self).__init__(*args, **kwargs)
        self._setting = {
            'auth':None,
            'timeout': (60, 90),
            'allow_redirects': True,
            'proxies': None,
            'hooks': None,
            'stream': None,
            'verify': False,
            'cert': None
        }
        for i in self._setting:
            if i in kwargs:
                self._setting[i] = kwargs[i]
        self._encoding = 'utf-8'
        if 'encoding' in kwargs:
            self._encoding = kwargs['encoding']
        self._response = None

    def __del__(self):
        self.quit()

    def quit(self):
        if isinstance(self._response, requests.Response):
            self._response.close()
        if isinstance(self._ses, requests.Session):
            self._ses.close()
        self._ses = None

    def _prepare_request(self, url):
        """
        预处理，构造header
        """
        if isinstance(self._response, requests.Response):
            self._response.close()
        if self._proxy:
            self.parse_proxy(**self._proxy)
            self._proxy['init'] = False
        if not self._base_url:
            self._base_url = url
        if not self._referer:
            self._referer = self._base_url
        if self._referer != url and urlparse(self._referer).netloc == urlparse(url).netloc:
            self._headers.update({'Referer': self._referer})

    def _request(self, method, url, data = None, params = None, files = None, json_data = None):
        """
        发送请求并获取结果
        """
        self._prepare_request(url)
        url = self._join_url(url)
        auth = self._setting.get('auth')
        timeout = self._setting.get('timeout')
        allow_redirects = self._setting.get('allow_redirects')
        proxies = self._setting.get('proxies')
        hooks = self._setting.get('hooks')
        stream = self._setting.get('stream')
        verify = self._setting.get('verify')
        cert = self._setting.get('cert')
        self.logger.info("Requests request url: %s" % url)
        self.logger.info("Requests request data: %s" % str(data))
        self.logger.info("Requests request params: %s" % str(params))
        self.logger.info("Requests request setting: %s" % str(self._setting))
        try:
            req = requests.Request(method.upper(), url,
                data=data,
                files=files,
                json=json_data,
                params=params,
                auth=auth,
                hooks=hooks,
            )
            with self._ses as s:
                prepped = s.prepare_request(req)
                self._response = s.send(
                    prepped,
                    stream=stream,
                    verify=verify,
                    proxies=proxies,
                    cert=cert,
                    allow_redirects=allow_redirects,
                    timeout=timeout,
                )
        except ConnectTimeout as e:
            raise CDSpiderCrawlerConnectTimeout(e, self._base_url, url, settings = self._setting, data = data, params = params, json = json_data, files = files)
        except ReadTimeout as e:
            raise CDSpiderCrawlerReadTimeout(e, self._base_url, url, settings = self._setting, data = data, params = params, json = json_data, files = files)
        except Timeout as e:
            raise CDSpiderCrawlerTimeout(e, self._base_url, url, settings = self._setting, data = data, params = params, json = json_data, files = files)
        except ConnectionError as e:
            if self._setting.get('proxy'):
                raise CrawlerProxyExpored(e, self._base_url, url, settings = self._setting, data = data, params = params, json = json_data, files = files)
            raise CDSpiderCrawlerConnectionError(e, self._base_url, url, settings = self._setting, data = data, params = params, json = json_data, files = files)
        except Exception as e:
            raise CDSpiderCrawlerError(e, self._base_url, url, settings = self._setting, data = data, params = params, json = json_data, files = files)

    def _prepare_response(self, referer):
        """
        预处理response
        """

        if not isinstance(self._response, requests.Response):
            raise CDSpiderCrawlerNoResponse(base_url = self._base_url)
        self._cookies = requests.cookies.merge_cookies(self._cookies, self._response.cookies)
        self._status_code = self._response.status_code
        self.logger.info('Requests response status: %s' % self._status_code)
        self.logger.info('Requests response cookies: %s' % self._cookies)
        url = self._response.url
        if isinstance(self._response.reason, bytes):
            try:
                reason = self._response.reason.decode('utf-8')
            except UnicodeDecodeError:
                reason = self._response.reason.decode('iso-8859-1')
        else:
            reason = self._response.reason
        if self._status_code == self.STATUS_CODE_NOT_FOUND:
            raise CDSpiderCrawlerNotFound(reason, self._base_url, url)
        elif self._status_code == self.STATUS_CODE_FORBIDDEN:
            raise CDSpiderCrawlerForbidden(reason, self._base_url, url)
        elif self._status_code == self.STATUS_CODE_INTERNAL_ERROR:
            raise CDSpiderCrawlerRemoteServerError(reason, self._base_url, url)
        elif self._status_code == self.STATUS_CODE_BAD_REQUEST:
            raise CDSpiderCrawlerBadRequest(reason, self._base_url, url)
        elif self._status_code == self.STATUS_CODE_GATEWAY_TIMEOUT:
            raise CDSpiderCrawlerConnectTimeout(reason, self._base_url, url)
        elif self._status_code != self.STATUS_CODE_OK:
            raise CDSpiderCrawlerError(reason, self._base_url, url, status_code=self._status_code)
        if referer == 1:
            self._referer = url

    def crawl(self, *args, **kwargs):
        """
        抓取操作
        :param method: 请求方式get/post/option/delete
        :param url: 请求的url
        :param data: 请求时发送的数据
        :param params: 请求时携带的参数
        :param files: 请求时发送的文件
        :param json: 请求时发送的json数据
        :param ajax: 是否发送ajax请求True/False
        :param headers: 请求时发送的header {name:value}
        :param cookies: 请求时携带的cookie
            [{name:cookie_name, value: cookie_value, path: /, domain: domain}]
        :param proxy: 请求时的代理设置
            {
                proxy_rate: always/every,  always/every二选1
                proxies: [host1:port1, host2:port2,....],  该项存在时忽略proxy_file
                和proxy_url设置
                proxy_file: /tmp/iplist.txt, 代理ip文件,文件内容为以"|"分割的host:port
                设置,该项存在时忽略proxy_file的设置
                proxy_url: http://localhost/ip_list.html,  代理ip在线列表, proxies
                和proxy_file都不存在时启用改设置
                addr: host:ip, 单一固定代理时设置
                type: http/socks/ftp/ssl, 代理类型
                user: username, 代理需要的用户
                password: password, 代理需要的密码
            }
        :param referer: 是否更新header的referer设置
        :param auth: 请求时发送的认证信息
        :param timeout: 请求时的超时时间设置, 默认值(60, 90)
        :param allow_redirects: 请求时是否允许自动跳转，默认为True
        :param hooks: handle hook
        :param stream: stream
        :param verify: 是否验证证书
        :param cert: 证书 if String, path to ssl client cert file (.pem).
            If Tuple, ('cert', 'key') pair.
        """
        l = len(args)
        if l > 0:
            kwargs.setdefault('url', args[0])
        if l > 1:
            kwargs.setdefault('method', args[1])
        kwargs.setdefault('method', 'get')
        self.logger.info("Requests crawl params: %s" % kwargs)
        self._prepare_setting(**kwargs)
        if kwargs.get('ajax', False):
            self.set_header('x-requested-with', 'XMLHttpRequest')
        kws = utils.dictunion(kwargs, {'method': None, 'url': None, 'data': None,
            'params': None, 'files': None, 'json': None})
        self._request(**kws)
        self._prepare_response(kwargs.get('referer', True))

    def wait(self, item, wait_time=1, intval=0.5, type = None, broken = None):
        """
        等待操作
        """
        end_time = time.time() + wait_time
        if not isinstance(item, list):
            item = [item]
        if type != None and not isinstance(type, list):
            type = [type]
        if broken != None and not isinstance(broken, list):
            broken = [broken]
        while True:
            i = 0
            pq = PyQuery(self.page_source)
            for it in item:
                value = pq.find(it)
                if value:
                    if type and len(type) > i and type[i]:
                        r = getattr(value, type[i])()
                        if not r:
                            continue
                    if broken and len(broken) > i and broken[i]:
                        raise BROKEN_EXCEPTIONS[broken[i]]
                    return value
                i += 1
            time.sleep(intval)
            if time.time() > end_time:
                break
        raise CDSpiderCrawlerWaitError('timeout for wait: %s' % (str(item)))

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
        self.logger.info("Requests request set cookie: name:%s, value:%s, %s" % (str(name),
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

    def get_header(self, name = None):
        """
        获取header，不指定name时，获取全部header
        """
        if name:
            return self._headers.get(name, None)
        return self._headers

    def set_header(self, name, value):
        """
        设置header
        """
        self.logger.info("Requests set header: %s => %s" % (str(name), str(value)))
        self._headers.update({name: value})

    @property
    def page_source(self):
        """
        获取文章源码
        """
        if isinstance(self._response, requests.Response):
            try:
                content = self._response.content
                content = utils.decode(content)
                return content
            except:
                return self._response.text
        raise CDSpiderCrawlerNoResponse(base_url = self._base_url,
            response = self._response)

    @property
    def final_url(self):
        return self._response.url

    @property
    def content(self):
        if isinstance(self._response, requests.Response):
            return self._response.content
        raise CDSpiderCrawlerNoResponse(base_url = self._base_url,
            response = self._response)

    def set_proxy(self, addr, type = 'http', user = None, password = None):
        """
        设置代理
        """
        if addr == None:
            self._setting['proxies'] = None
        else:
            if user:
                if password:
                    user += ':' + password
                proxy = user +'@'+ addr
            else:
                proxy = addr
            if type == "socks":
                proxies = {"http": "socks5://" + proxy, "https": "socks5://" + proxy}
            elif type == "ftp":
                proxies = {"http": "ftp://" + proxy, "https": "ftp://" + proxy}
            elif type == "http":
                proxies = {"http": "http://" + proxy, "https": "http://" + proxy}
            elif type == "ssl":
                proxies = {"http": "https://" + proxy, "https": "https://" + proxy}
            self._setting["proxies"] = proxies
