#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-19 9:13:51
"""
import os
import six
import time
from urllib.parse import *
from http.client import BadStatusLine
from urllib.error import URLError
from selenium import webdriver
from selenium.common.exceptions import *
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from urllib.parse import *
from requests.exceptions import *
from cdspider.crawler import BaseCrawler, RequestsCrawler
from cdspider.exceptions import *
from cdspider.libs import utils

IGNORED_EXCEPTIONS = (NoSuchElementException, BadStatusLine, URLError, )

class SeleniumCrawler(BaseCrawler):
    """
    Selenium爬虫类
    """
    ID = "ID"
    XPATH = "XPATH"
    LINK_TEXT = "LINK_TEXT"
    PARTIAL_LINK_TEXT = "PARTIAL_LINK_TEXT"
    NAME = "NAME"
    TAG_NAME = "TAG_NAME"
    CLASS_NAME = "CLASS_NAME"
    CSS_SELECTOR = "CSS_SELECTOR"

    def __init__(self, *args, **kwargs):
        self._driver = None
        self._base_url = None
        self._referer = None
        self._request_crawler = None
        self._cookie = []
        self._cap = self._init_cap()
#        path = os.path.join(os.path.dirname(__file__), os.pardir)
        self.service_args  = {"--webdriver-loglevel": "WARN", "--web-security": "false", "--ignore-ssl-errors": "true"}
        if os.path.exists('/mnt/server/phantomjs/bin/phantomjs'):
            self.execut = '/mnt/server/phantomjs/bin/phantomjs'
        elif os.path.exists('/usr/bin/phantomjs'):
            self.execut = '/usr/bin/phantomjs'
        elif os.path.exists('/usr/local/bin/phantomjs'):
            self.execut = '/usr/local/bin/phantomjs'
        else:
            self.execut = 'phantomjs'
        super(SeleniumCrawler, self).__init__(*args, **kwargs)
        self._encoding = 'utf-8'
        if 'encoding' in kwargs:
            self._encoding = kwargs['encoding']
        self._response = None

    def __del__(self):
        self.close()
        self.quit()
        self._driver = None
        super(SeleniumCrawler, self).__del__()

    def close(self):
        if hasattr(self._driver, "close"):
            self._driver.close()

    def quit(self):
        if hasattr(self._driver, "quit"):
            self._driver.quit()

    def _init_cap(self):
        cap = webdriver.DesiredCapabilities.PHANTOMJS.copy()
#        cap["phantomjs.page.settings.resourceTimeout"] = 1000
#        cap["phantomjs.page.customHeaders.Accept"] = '*/*'
#        cap["phantomjs.page.customHeaders.accept"] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
#        cap["phantomjs.page.customHeaders.Accept-Encoding"] = 'gzip, deflate, sdch, br'
#        cap["phantomjs.page.customHeaders.accept-language"] = 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3'
#        cap["phantomjs.page.customHeaders.x-insight"] = '1'
#        cap["phantomjs.page.customHeaders.upgrade-insecure-requests"] = '1'
#        cap["phantomjs.page.customHeaders.Connection"] = 'Keep-Alive'
        cap["browserName"] = "chrome"
        cap["browserVersion"] = "54.0.2840.71"
        cap["platformName"] = "Windows"
        cap["phantomjs.page.settings.loadImages"] = False
#        cap["phantomjs.page.settings.localToRemoteUrlAccessEnabled"] = True
        return cap

    def _init_driver(self):
        self.quit()
        if self._proxy:
            self.parse_proxy(**self._proxy)
            self._proxy['init'] = False
        service_args = None
        if self.service_args:
            service_args = [ k +"="+ v for k,v in self.service_args.items()]
            self.info("Selenium set service_args: %s" % (str(service_args)))
        self._driver = webdriver.PhantomJS(executable_path=self.execut, service_args=service_args)

    def start(self):
        self._driver.start_session(self._cap)
        self._driver.set_window_size(1024, 768)
        self._driver.set_script_timeout(30)
        self._driver.set_page_load_timeout(120)

    def _prepare_request(self, url):
        """
        预处理，构造header
        """
        if not self._base_url:
            self._base_url = url
        if not self._referer:
            self._referer = self._base_url
        if self._referer != url and urlparse(self._referer).netloc == urlparse(url).netloc:
            self.set_header('Referer', self._referer)
        self.service_args['--output-encoding'] = self._encoding
        self.service_args['--script-encoding'] = self._encoding
        self._init_driver()

    def crawl(self, *args, **kwargs):
        """
        抓取操作
        :param method: 请求方式,本class中方法
        :param 其他参数参考相应函数, get/post/delete/option/head 参考request方法
        """
        l = len(args)
        if l > 0:
            kwargs.setdefault('url', args[0])
        if l > 1:
            kwargs.setdefault('method', args[1])
        kwargs.setdefault('method', 'open')
        self.info("Selenium crawl params: %s" % kwargs)
        if not hasattr(self, kwargs['method']):
            self.request(**kwargs)
        else:
            method = kwargs.pop('method')
            getattr(self, method)(**kwargs)

    def open(self, *args, **kwargs):
        """
        get请求
        :param url: 请求的url
        :param headers: 请求时发送的header {name:value}
        :param cookies: 请求时携带的cookie
        """
        self._prepare_setting(**kwargs)
        curl = self._join_url(kwargs['url'])
        self._prepare_request(curl)
        self.start()
        self._request_crawler = None
        self.info("Selenium request url: %s" % curl)
        try:
            self._driver.get(curl)
        except TimeoutException as e:
            raise CDSpiderCrawlerConnectTimeout(e, self._base_url, curl)
        except Exception as e:
            raise CDSpiderCrawlerError(traceback.format_exc(), self._base_url, curl)
        self._referer = curl
        return self

    def request(self, *args, **kwargs):
        """
        调用req引擎执行request操作
        :param method: 请求方式get/post/option/delete/head
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
        self._init_request_crawler()
        self._request_crawler.crawl(*args, **kwargs)

    def _init_request_crawler(self):
        if not self._request_crawler:
            self._request_crawler = RequestsCrawler()
            cookies = self.get_cookie()
            if cookies:
                for item in cookies:
                    self._request_crawler.set_cookie(**item)
            headers = self.get_header()
            if headers:
                for k, v in headers.items():
                    self._request_crawler.set_header(k, v)

    def chains(self):
        return webdriver.ActionChains(self._driver)

    def click(self, *args, **kwargs):
        """
        点击某个元素
        """
        kwargs['is_enabled'] = True
        elements = self.filter(*args, **kwargs)
        if isinstance(elements, list):
            for item in elements:
                self.switch_position(item)
                item.click()
        else:
            self.switch_position(elements)
            elements.click()
        if 'wait' in kwargs:
            if not isinstance(kwargs['wait'], dict):
                    raise CDSpiderCrawlerSettingError('Invalid wait setting')
            self.wait_element(**kwargs['wait'])
        self.switch_window()
        return self

    def execute_js(self, js, *args, **kwargs):
        """
        js点击某个元素
        js = "arguments[0].click()"
        """
        kwargs['is_enabled'] = True
        elements = self.filter(*args, **kwargs)
        if isinstance(elements, list):
            for item in elements:
                self._driver.execute_script(js, item)
        else:
            self._driver.execute_script(js, elements)
        if 'wait' in kwargs:
            if not isinstance(kwargs['wait'], dict):
                    raise CDSpiderCrawlerSettingError('Invalid wait setting')
            self.wait_element(**kwargs['wait'])
        self.switch_window()
        return self

    def wait_element(self, *args, **kwargs):
        timeout = kwargs.get('timeout', 15)
        intval = kwargs.get('intval', 0.5)
        ignored_exceptions = kwargs.get('ignored_exceptions', None)
        wait = WebDriverWait(self._driver, timeout, poll_frequency = intval, ignored_exceptions = ignored_exceptions)

        params = kwargs.get('params', [])
        if 'locator' in kwargs:
            locator = (getattr(By, 'CSS_SELECTOR'), kwargs.get('locator'))
            params.insert(0, locator)
        if 'text' in kwargs:
            ele = self.filter(**kwargs['text'])
            params.append(ele.text)
        if 'until_not' in kwargs:
            condition = kwargs.get('until_not')
            method = getattr(EC, condition)(*params)
            wait.until_not(method)
        else:
            condition = kwargs.get('until', 'presence_of_element_located')
            method = getattr(EC, condition)(*params)
            wait.until(method)

    def submit(self, *args, **kwargs):
        """
        提交表单
        """
        elements = self.filter(*args, **kwargs)
        if isinstance(elements, list):
            item = elements[0]
        else:
            item = elements
        self.switch_position(item)
        item.submit()
        if 'wait' in kwargs:
            if not isinstance(kwargs['wait'], dict):
                    raise CDSpiderCrawlerSettingError('Invalid wait setting')
            self.wait_element(**kwargs['wait'])
        self.switch_window()
        return self

    def dbclick(self, *args, **kwargs):
        """
        双击元素
        """
        elements = self.filter(*args, **kwargs)
        if isinstance(elements, list):
            for item in elements:
                self.switch_position(item)
        #        self.chains().double_click(item)
                item.double_click()
        else:
            self.switch_position(elements)
    #        self.chains().double_click(item)
            elements.double_click()
        if 'wait' in kwargs:
            if not isinstance(kwargs['wait'], dict):
                    raise CDSpiderCrawlerSettingError('Invalid wait setting')
            self.wait_element(**kwargs['wait'])
        self.switch_window()
        return self

    def keydown(self, value, selector = None, index = None):
        """
        在某个元素上按下键盘的某个按键
        """
        if selector:
            if index is not None:
                item = self.find_by_eq(selector, index)
            else:
                item = self.find(selector)
                self.switch_position(item)
            self.chains().key_down(value, item)
        else:
            self.chains().key_down(value)
        if 'wait' in kwargs:
            if not isinstance(kwargs['wait'], dict):
                    raise CDSpiderCrawlerSettingError('Invalid wait setting')
            self.wait_element(**kwargs['wait'])
        self.switch_window()
        return self

    def keyup(self, value, selector = None, index = None):
        """
        在某个元素上松开某个按键
        """
        if selector:
            if index is not None:
                item = self.find_by_eq(selector, index)
            else:
                item = self.find(selector)
                self.switch_position(item)
            self.chains().key_up(value, item)
        else:
            self.chains().key_up(value)
        if 'wait' in kwargs:
            if not isinstance(kwargs['wait'], dict):
                    raise CDSpiderCrawlerSettingError('Invalid wait setting')
            self.wait_element(**kwargs['wait'])
        self.switch_window()
        return self

    def fill(self, *args, **kwargs):
        """
        在元素内填入值
        """
        if (len(args) >= 1 and
                (not PY3k and isinstance(args[0], basestring) or
                (PY3k and isinstance(args[0], str)))):
            kwargs['value'] = args[0]
        args = []
        if not 'value' in kwargs:
            raise CDSpiderCrawlerSettingError('value must be not none', self._base_url, self._curl, rule=kwargs)
        value = kwargs['value']
        del kwargs['value']
        elements = self.filter(*args, **kwargs)
        if isinstance(elements, list):
            item = elements[0]
        else:
            item = elements
        self.switch_position(item)
        item.send_keys(value)
        if 'wait' in kwargs:
            if not isinstance(kwargs['wait'], dict):
                    raise CDSpiderCrawlerSettingError('Invalid wait setting')
            self.wait_element(**kwargs['wait'])
        self.switch_window()
        return self

    def switch_position(self, element):
        element.location_once_scrolled_into_view

    def switch_window(self):
        handles = self._driver.window_handles
        if len(handles) > 1:
            self._driver.switch_to.window(handles[-1])
            self._driver.set_window_size(1920, 1080)

    def wait(self, item, wait_time=1, intval=0.5, type = None, broken = None, mode=CSS_SELECTOR):
        """
        等待操作
        """
        method_pool = []
        end_time = time.time() + wait_time
        if not isinstance(item, list):
            item = [item]
        if type != None and not isinstance(type, list):
            type = [type]
        if broken != None and not isinstance(broken, list):
            broken = [broken]
        for it in item:
            locator = (getattr(By, mode), it)
            method = EC.presence_of_element_located(locator)
            method_pool.append(method)
        while True:
            i = 0
            for method in method_pool:
                self.info("Selenium wait item: %s time: %s end_time: %s" % (method, time.time(), end_time))
                try:
                    value = method(self._driver)
                    if value:
                        if type and len(type) > i and type[i]:
                            r = getattr(value, type[i])
                            if not r:
                                continue
                        if broken and len(broken) > i and broken[i]:
                            raise BROKEN_EXCEPTIONS[broken[i]]
                        return value
                except IGNORED_EXCEPTIONS:
                    continue
                i += 1
            time.sleep(intval)
            if time.time() > end_time:
                break
        raise CDSpiderCrawlerWaitError('timeout for wait: %s' % (str(item)))

    def get_cookie(self, name = None):
        """
        获取Response cookie，不指定name时，获取全部cookie
        """
        if not self._driver:
            return None
        if not name:
            self._driver.get_cookies()
        return self._driver.get_cookie(name)

    def set_cookie(self, name, value, **kwargs):
        """
        设置cookie
        """
        self.info("Selenium request set cookie: name:%s, value:%s, %s" % (str(name),
            str(value), str(kwargs)))

        kwargs['name'] = name
        kwargs['value'] = str(value)
        assert kwargs['domain'], "cookie 必须设置domain"
        return self._cookie.append(kwargs)

    def get_header(self, name = None):
        """
        获取header，不指定name时，获取全部header
        """
        if name:
            return self._cap.get('phantomjs.page.customHeaders.{}'.format(name), None)
        headers = {}
        for k,v in self._cap.items():
            names = k.split("phantomjs.page.customHeaders.", 1)
            if names > 1:
                headers[names[-1]] = v
        return headers

    def set_header(self, name, value):
        """
        设置header
        """
        self.info("Selenium set header: %s => %s" % (str(name), str(value)))
        self._cap["phantomjs.page.customHeaders."+ name] = utils.quote_chinese(value)

    @property
    def page_source(self):
        """
        获取文章源码
        """
        if self._request_crawler:
            return self._request_crawler.page_source
        return self._driver.page_source
        raise CDSpiderCrawlerNoResponse(base_url = self._base_url)

    @property
    def final_url(self):
        if self._request_crawler:
            return self._request_crawler.final_url
        return self._driver.current_url

    def set_proxy(self, addr, type = 'http', user = None, password = None):
        """
        设置代理
        """
        if addr == None:
            if "--proxy-type" in self.service_args:
                del self.service_args['--proxy-type']
            if "--proxy" in self.service_args:
                del self.service_args['--proxy']
            if "--proxy-auth" in self.service_args:
                del self.service_args['--proxy-auth']
#            if "phantomjs.page.settings.proxyType" in self._cap:
#                del self._cap['phantomjs.page.settings.proxyType']
#            if "phantomjs.page.settings.proxy" in self._cap:
#                del self._cap['phantomjs.page.settings.proxy']
#            if "phantomjs.page.settings.proxyAuth" in self._cap:
#                del self._cap['phantomjs.page.settings.proxyAuth']
        else:
            setting = {}
            if type == 'socks':
                setting['--proxy-type'] = "socks5"
                setting['--proxy'] = addr
#                setting['phantomjs.page.settings.proxyType'] = "socks5"
#                setting['phantomjs.page.settings.proxy'] = addr
            elif type == 'http':
                setting['--proxy-type'] = "http"
                setting['--proxy'] = addr
#                setting['phantomjs.page.settings.proxyType'] = "http"
#                setting['phantomjs.page.settings.proxy'] = addr
            elif type == 'ftp':
                setting['--proxy-type'] = "http"
                setting['--proxy'] = addr
#                setting['phantomjs.page.settings.proxyType'] = "http"
#                setting['phantomjs.page.settings.proxy'] = addr
            elif type == 'ssl':
                setting['--proxy-type'] = "http"
                setting['--proxy'] = addr
#                setting['phantomjs.page.settings.proxyType'] = "http"
#                setting['phantomjs.page.settings.proxy'] = addr
            if user:
                auths = user
                if password:
                    auths += ':'+ password
                setting["--proxy-auth"] = auths
#                setting["phantomjs.page.settings.proxyAuth"] = auths
            self.service_args.update(setting)
#            self._cap.update(setting)

    def find(self, selector):
        """
        通过css选择器获取页面内容
        """
        try:
            return self._driver.find_element_by_css_selector(selector)
        except Exception as e:
            raise CDSpiderCrawlerNotEelements(e, self._base_url, self.final_url, rule=selector)

    def find_all(self, selector):
        """
        通过css选择器获取符合的所有页面内容
        """
        try:
            return self._driver.find_elements_by_css_selector(selector)
        except Exception as e:
            raise CDSpiderCrawlerNotEelements(e, self._base_url, self.final_url, rule=selector)

    def find_by_eq(self, selector, index = 0):
        """
        获取给定index的符合css选择器内容
        """
        elements = self.find_all(selector)
        i = 0
        for element in elements:
            if i == index:
                return element
            i += 1
        return None

    def filter(self, *args, **kwargs):
        """
        按条件过滤页面内容
        """
        if (len(args) >= 1 and  isinstance(args[0], six.string_types)):
            kwargs['selector'] = args[0]
            if (len(args) >= 2):
                kwargs['eq'] = args[1]
        args = []
        if not 'selector' in kwargs:
            raise CDSpiderSettingError('Selenium selector must be not none', self._base_url, self.final_url, rule=kwargs)
        kwargs.setdefault('is_selected', False)
        kwargs.setdefault('is_enabled', False)
        kwargs.setdefault('is_displayed', False)
        kwargs.setdefault('by', By.CSS_SELECTOR)
        _kwargs = {}
        _kwargs['value'] = kwargs['selector']
        _kwargs['by'] = kwargs['by']
        cursor = self._driver.find_elements(**_kwargs)
        fkwargs = utils.dictunion(kwargs, {"eq": None, "text": None, "attr": None, "property": None, "css": None})
        elements = utils.iterator2list(cursor)
        if not fkwargs:
            return elements
        return self._filter(elements, **kwargs)

    def _filter(self, elements, **kwargs):
        """
        过滤元素
        """
        element = None
        if 'eq' in kwargs:
            eq = int(kwargs['eq'])
            l = len(elements)
            if eq < 0:
                eq = l + eq
            if eq < l:
                element = elements[eq]
                if 'match' in kwargs and kwargs['match']:
                    rule = kwargs.copy()
                    del rule['eq']
                    return self._filter([element], **rule)
                if self._getable(kwargs, element):
                    return element

        if 'text' in kwargs:
            element = self._text(elements, **kwargs)

        if 'attr' in kwargs:
            element = self._attr(elements, **kwargs)

        if 'property' in kwargs:
            element = self._property(elements, **kwargs)

        if 'css' in kwargs:
            element = self._css(elements, **kwargs)
        if not element:
            raise CDSpiderCrawlerNotEelements('Selenium elements not found', self._base_url, self.final_url, rule=kwargs['selector'])
        return element

    def _getable(self, kwargs, element):
        if kwargs['is_selected']:
            return element.tag_name == 'select' and element.is_selected()
        if kwargs['is_enabled']:
            return element.is_enabled()
        if kwargs['is_displayed']:
            return element.is_displayed()
        return True

    def _text(self, elements, **kwargs):
        for element in elements:
            text = element.text
            if 'match' in kwargs and kwargs['match']:
                pattern = pcre2re(kwargs['text'])
                if (text and pattern.search(text) and self._getable(kwargs, element)):
                    return element
            elif 'partial' in kwargs and kwargs['partial']:
                if (text and text.find(kwargs['text']) != -1 and self._getable(kwargs, element)):
                    return element
            else:
                if (text == kwargs['text'] and self._getable(kwargs, element)):
                    return element

    def _attr(self, elements, **kwargs):
        if not 'val' in kwargs:
            raise CDSpiderSettingError('Selenium val must be not none', self._base_url, self.final_url, rule=kwargs)
        for element in elements:
            attr = element.get_attribute(kwargs['attr'])
            if 'match' in kwargs and kwargs['match']:
                pattern = pcre2re(kwargs['val'])
                if (attr and pattern.search(attr) and self._getable(kwargs, element)):
                    return element
            elif 'partial' in kwargs and kwargs['partial']:
                if (attr and attr.find(kwargs['val']) != -1 and self._getable(kwargs, element)):
                    return element
            else:
                if (attr == kwargs['val'] and self._getable(kwargs, element)):
                    return element

    def _property(self, elements, **kwargs):
        if not 'val' in kwargs:
            raise CDSpiderSettingError('Selenium val must be not none', self._base_url, self.final_url, rule=kwargs)
        for element in elements:
            property = element.get_property(kwargs['property'])
            if 'match' in kwargs and kwargs['match']:
                pattern = pcre2re(kwargs['val'])
                if (property and pattern.search(property) and self._getable(kwargs, element)):
                    return element
            elif 'partial' in kwargs and kwargs['partial']:
                if (property and property.find(kwargs['val']) != -1 and self._getable(kwargs, element)):
                    return element
            else:
                if (property == kwargs['val'] and self._getable(kwargs, element)):
                    return element

    def _css(self, elements, **kwargs):
        if not 'val' in kwargs:
            raise CDSpiderSettingError('Selenium val must be not none', self._base_url, self.final_url, rule=kwargs)
        for element in elements:
            css = element.value_of_css_property(kwargs['css'])
            if 'match' in kwargs and kwargs['match']:
                pattern = pcre2re(kwargs['val'])
                if (css and pattern.search(css) and self._getable(kwargs, element)):
                    return element
            elif 'partial' in kwargs and kwargs['partial']:
                if (css and css.find(kwargs['val']) != -1 and self._getable(kwargs, element)):
                    return element
            else:
                if (css == kwargs['val'] and self._getable(kwargs, element)):
                    return element
