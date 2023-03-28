# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-1-19 9:13:51
"""
import json
import os
import time
import traceback
from http.client import BadStatusLine
from urllib.error import URLError

import six
from selenium import webdriver
from selenium.common.exceptions import *
from selenium.webdriver.chrome.options import Options as ChromeOpts
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from tornado import gen

from cdspider.crawler import BaseCrawler
from cdspider.exceptions import *
from cdspider.libs import utils
from cdspider.libs.constants import BROKEN_EXCEPTIONS

IGNORED_EXCEPTIONS = (NoSuchElementException, BadStatusLine, URLError,)


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
        """
        init
        :param args:
        :param kwargs:
        """
        self._driver = None
        config = kwargs.pop('config', {})
        if isinstance(config, six.string_types):
            config = json.loads(json.dumps(eval(config)))
        self.engine = config.get('selenium', {}).get('engine', 'remote')
        self.exec_path = config.get('selenium', {}).get('exec_path', None)
        self._cap = self._init_cap(self.engine)
        # path = os.path.join(os.path.dirname(__file__), os.pardir)
        self.service_args = {"--webdriver-loglevel": "WARN", "--web-security": "false", "--ignore-ssl-errors": "true"}
        self._init_exec_path()
        self._encoding = "UTF-8"
        super(SeleniumCrawler, self).__init__(*args, **kwargs)

    def __del__(self):
        """
        del
        :return:
        """
        self._driver = None
        super(SeleniumCrawler, self).__del__()

    def close(self):
        """
        关闭driver
        :return:
        """
        if hasattr(self._driver, "close"):
            self._driver.close()

    def quit(self):
        """
        退出driver
        :return:
        """
        if hasattr(self._driver, "quit"):
            self._driver.quit()

    @staticmethod
    def _init_cap(engine):
        """
        初始化DesiredCapabilities
        :return:
        """
        if engine == 'remote':
            cap = webdriver.DesiredCapabilities.CHROME.copy()
            # cap["phantomjs.page.settings.resourceTimeout"] = 1000
            # cap["phantomjs.page.customHeaders.Accept"] = '*/*'
            # cap["phantomjs.page.customHeaders.accept"] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            # cap["phantomjs.page.customHeaders.Accept-Encoding"] = 'gzip, deflate, sdch, br'
            # cap["phantomjs.page.customHeaders.accept-language"] = 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3'
            # cap["phantomjs.page.customHeaders.x-insight"] = '1'
            # cap["phantomjs.page.customHeaders.upgrade-insecure-requests"] = '1'
            # cap["phantomjs.page.customHeaders.Connection"] = 'Keep-Alive'
            cap["browserName"] = "chrome"
            cap["platform"] = "WIN10"
            cap["version"] = "108"
            # cap["phantomjs.page.settings.localToRemoteUrlAccessEnabled"] = True
            return cap
        else:
            cap = webdriver.DesiredCapabilities.PHANTOMJS.copy()
            # cap["phantomjs.page.settings.resourceTimeout"] = 1000
            # cap["phantomjs.page.customHeaders.Accept"] = '*/*'
            # cap["phantomjs.page.customHeaders.accept"] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            # cap["phantomjs.page.customHeaders.Accept-Encoding"] = 'gzip, deflate, sdch, br'
            # cap["phantomjs.page.customHeaders.accept-language"] = 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3'
            # cap["phantomjs.page.customHeaders.x-insight"] = '1'
            # cap["phantomjs.page.customHeaders.upgrade-insecure-requests"] = '1'
            # cap["phantomjs.page.customHeaders.Connection"] = 'Keep-Alive'
            cap["browserName"] = "chrome"
            cap["version"] = "10"
            cap["platform"] = "Windows"
            cap["phantomjs.page.settings.loadImages"] = False
            # cap["phantomjs.page.settings.localToRemoteUrlAccessEnabled"] = True
            return cap

    def _init_exec_path(self):
        if self.exec_path is None:
            if self.engine == 'phantomjs':
                if os.path.exists('/usr/bin/phantomjs'):
                    self.exec_path = '/usr/bin/phantomjs'
                elif os.path.exists('/usr/local/bin/phantomjs'):
                    self.exec_path = '/usr/local/bin/phantomjs'
                else:
                    self.exec_path = 'phantomjs'
            elif self.engine == 'remote':
                if self.exec_path is None:
                    self.exec_path = 'http://127.0.0.1:4444/wd/hub/'

    def _init_driver(self):
        """
        初始化driver
        :return:
        """
        self.quit()
        if self._proxy:
            self.parse_proxy(**self._proxy)
            self._proxy['init'] = False
        service_args = None
        options = None
        if self.engine == 'remote':
            options = ChromeOpts()
            # options.add_argument('--no-sandbox')
            if '--proxy-server' in self.service_args and self.service_args['--proxy-server']:
                options.add_argument('--proxy-server=' + self.service_args['--proxy-server'])

        if self.service_args:
            service_args = [k + "=" + v for k, v in self.service_args.items()]
            self.info("Selenium set service_args: %s" % (str(service_args)))
        self.info("Selenium load driver: %s => %s" % (self.engine, self.exec_path))
        if self.engine == 'remote':
            self._driver = webdriver.Remote(command_executor=self.exec_path,
                                            desired_capabilities=options.to_capabilities())
        else:
            self._driver = webdriver.PhantomJS(executable_path=self.exec_path, service_args=service_args)

    def start(self):
        headers = self.fetch.get('headers')
        if headers:
            for k, v in headers.items():
                if self.engine == 'chrome':
                    # self._cap["chrome.page.customHeaders." + k] = utils.quote_chinese(v)
                    pass
                else:
                    self._cap["phantomjs.page.customHeaders." + k] = utils.quote_chinese(v)

        if self.engine == 'chrome':
            self.close()
            self._driver.start_session(capabilities=self._cap)
        elif self.engine == 'firefox':
            self.close()
            self._driver.start_session(capabilities=self._cap)
        else:
            self.close()
            self._driver.start_session(capabilities=self._cap)

        for cookie in self._cookies:
            self._driver.add_cookie(dict(cookie))

        self._driver.set_window_size(1024, 768)
        self._driver.set_script_timeout(30)
        self._driver.set_page_load_timeout(60)

    def _prepare_request(self, url):
        """
        预处理
        :param url: base url
        """
        if not self._base_url:
            self._base_url = url
        self.service_args['--output-encoding'] = self._encoding
        self.service_args['--script-encoding'] = self._encoding
        self._init_driver()

    @gen.coroutine
    def http_fetch(self, url, fetch):
        """
        抓取操作
        :param url: 请求方url
        :param  fetch: 其他参数
        """
        start_time = time.time()

        def handle_error(x):
            BaseCrawler.handle_error(url, start_time, x)

        # making requests
        while True:
            try:
                response = yield gen.maybe_future(self.open(**fetch))
            except Exception as e:
                raise gen.Return(handle_error(e))

            self.gen_result(
                url=response["url"] or url,
                code=200,
                headers=self.fetch.get("headers", {}),
                cookies=response["cookies"],
                content=response["content"],
                iframe=response.get("iframe", []),
                start_time=start_time)

    def open(self, **kwargs):
        """
        get请求
        :param url: 请求的url
        :param headers: 请求时发送的header {name:value}
        :param cookies: 请求时携带的cookie
        """
        self._prepare_setting(**kwargs)
        curl = self._join_url(kwargs['url'])
        self._prepare_request(curl)
        # self.start()
        self.info("Selenium request url: %s" % curl)
        try:
            self._driver.get(curl)
            if 'wait' in kwargs and kwargs['wait']:
                wait = kwargs['wait']
                if isinstance(wait, six.string_types):
                    wait = json.loads(json.dumps(eval(wait)))
                self.wait(**wait)

            result = {
                "url": self._driver.current_url,
                "cookies": self._cookies.get_dict()
            }

            iframes = self._driver.find_elements(By.XPATH, '//iframe')
            if iframes is not None and len(iframes) > 0:
                iframe_result = []
                for iframe in iframes:
                    if iframe.is_enabled:
                        try:
                            self._driver.switch_to_frame(iframe)
                            iframe_result.append(self._driver.page_source)
                        except Exception as ignore:
                            pass
                        time.sleep(0.1)
                result['iframe'] = iframe_result

            cookies_ = self._driver.get_cookies()
            if cookies_:
                for cookie in cookies_:
                    self.set_cookie(**cookie)
            self._driver.switch_to_default_content()
            result['content'] = self._driver.page_source,
            return result
        except TimeoutException as e:
            raise CDSpiderCrawlerConnectTimeout(e, self._base_url, curl)
        except Exception as exc:
            raise CDSpiderCrawlerError(traceback.format_exc(), self._base_url, curl)

    def chains(self):
        """
        chains
        :return:
        """
        return webdriver.ActionChains(self._driver)

    def click(self, *args, **kwargs):
        """
        点击某个元素
        :param args: see filter
        :param kwargs: set filter
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
                raise CDSpiderSettingError('Invalid wait setting')
            self.wait_element(**kwargs['wait'])
        self.switch_window()
        return self

    def execute_js(self, js, *args, **kwargs):
        """
        js点击某个元素
        js = "arguments[0].click()"
        :param wait: see wait_element
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
                raise CDSpiderSettingError('Invalid wait setting')
            self.wait_element(**kwargs['wait'])
        self.switch_window()
        return self

    def wait_element(self, **kwargs):
        """
        等待元素
        :param timeout: 超时时间
        :param intval: 检查间隔
        :param ignored_exceptions: 忽略的错误
        :param locator: css 选择器
        :param text: text
        :param util: utile
        :param util_not: util not
        :return:
        """
        timeout = kwargs.get('timeout', 15)
        intval = kwargs.get('intval', 0.5)
        ignored_exceptions = kwargs.get('ignored_exceptions', None)
        wait = WebDriverWait(self._driver, timeout, poll_frequency=intval, ignored_exceptions=ignored_exceptions)

        params = kwargs.get('params', [])
        if 'locator' in kwargs:
            locator = (getattr(By, 'CSS_SELECTOR'), kwargs.get('locator'))
            params.insert(0, locator)
        if 'text' in kwargs:
            ele = self.filter(**kwargs['text'])
            params.append(ele.text)
        if 'util_not' in kwargs:
            condition = kwargs.get('util_not')
            method = getattr(EC, condition)(*params)
            wait.until_not(method)
        else:
            condition = kwargs.get('until', 'presence_of_element_located')
            method = getattr(EC, condition)(*params)
            wait.until(method)

    def submit(self, *args, **kwargs):
        """
        提交表单
        :param args: see filter
        :param kwargs: see filter
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
                raise CDSpiderSettingError('Invalid wait setting')
            self.wait_element(**kwargs['wait'])
        self.switch_window()
        return self

    def dbclick(self, *args, **kwargs):
        """
        双击元素
        :param args: see filter
        :param kwargs: see filter
        """
        elements = self.filter(*args, **kwargs)
        if isinstance(elements, list):
            for item in elements:
                self.switch_position(item)
                # self.chains().double_click(item)
                item.double_click()
        else:
            self.switch_position(elements)
            # self.chains().double_click(item)
            elements.double_click()
        if 'wait' in kwargs:
            if not isinstance(kwargs['wait'], dict):
                raise CDSpiderSettingError('Invalid wait setting')
            self.wait_element(**kwargs['wait'])
        self.switch_window()
        return self

    def keydown(self, value, selector=None, index=None, **kwargs):
        """
        在某个元素上按下键盘的某个按键
        :param value: 键名
        :param selector: css 选择器
        :param index: index
        :param wait: see wait_element
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
                raise CDSpiderSettingError('Invalid wait setting')
            self.wait_element(**kwargs['wait'])
        self.switch_window()
        return self

    def keyup(self, value, selector=None, index=None, **kwargs):
        """
        在某个元素上松开某个按键
        :param value: 键名
        :param selector: css 选择器
        :param index: index
        :param wait: see wait_element
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
                raise CDSpiderSettingError('Invalid wait setting')
            self.wait_element(**kwargs['wait'])
        self.switch_window()
        return self

    def fill(self, *args, **kwargs):
        """
        在元素内填入值
        :param value: 填入的值
        :param args: see filter
        :param kwargs: see filter
        """
        if len(args) >= 1 and isinstance(args[0], str):
            kwargs['value'] = args[0]
        args = []
        if 'value' not in kwargs:
            raise CDSpiderSettingError('value must be not none', self._base_url, self._curl, rule=kwargs)
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
                raise CDSpiderSettingError('Invalid wait setting')
            self.wait_element(**kwargs['wait'])
        self.switch_window()
        return self

    @staticmethod
    def switch_position(element):
        """
        switch_position
        :param element:
        :return:
        """
        element.location_once_scrolled_into_view

    def switch_window(self):
        """
        switch_window
        :return:
        """
        handles = self._driver.window_handles
        if len(handles) > 1:
            self._driver.switch_to.window(handles[-1])
            self._driver.set_window_size(1920, 1080)

    def wait(self, item, wait_time=1, intval=0.5, type=None, broken=None, mode=CSS_SELECTOR):
        """
        等待操作
        :param item:
        :param wait_time:
        :param intval:
        :param type:
        :param broken:
        :param mode:
        :return:
        """
        method_pool = []
        end_time = time.time() + wait_time
        if not isinstance(item, list):
            item = [item]
        if type is not None and not isinstance(type, list):
            type = [type]
        if broken is not None and not isinstance(broken, list):
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

    def set_proxy(self, addr, type='http', user=None, password=None):
        """
        设置代理
        """
        if addr is None:
            if "--proxy-type" in self.service_args:
                del self.service_args['--proxy-type']
            if "--proxy" in self.service_args:
                del self.service_args['--proxy']
            if "--proxy-auth" in self.service_args:
                del self.service_args['--proxy-auth']
            if "--proxy-sever" in self.service_args:
                del self.service_args['--proxy-sever']
            # if "phantomjs.page.settings.proxyType" in self._cap:
            # del self._cap['phantomjs.page.settings.proxyType']
            # if "phantomjs.page.settings.proxy" in self._cap:
            # del self._cap['phantomjs.page.settings.proxy']
            # if "phantomjs.page.settings.proxyAuth" in self._cap:
            # del self._cap['phantomjs.page.settings.proxyAuth']
        else:
            setting = {}
            if type == 'socks':
                setting['--proxy-type'] = "socks5"
                setting['--proxy'] = addr
                # setting['phantomjs.page.settings.proxyType'] = "socks5"
                # setting['phantomjs.page.settings.proxy'] = addr
            elif type == 'http':
                setting['--proxy-type'] = "http"
                setting['--proxy'] = addr
                # setting['phantomjs.page.settings.proxyType'] = "http"
                # setting['phantomjs.page.settings.proxy'] = addr
            elif type == 'ftp':
                setting['--proxy-type'] = "http"
                setting['--proxy'] = addr
                # setting['phantomjs.page.settings.proxyType'] = "http"
                # setting['phantomjs.page.settings.proxy'] = addr
            elif type == 'ssl':
                setting['--proxy-type'] = "http"
                setting['--proxy'] = addr
                # setting['phantomjs.page.settings.proxyType'] = "http"
                # setting['phantomjs.page.settings.proxy'] = addr
            if user:
                auths = user
                if password:
                    auths += ':' + password
                setting["--proxy-auth"] = auths
                setting["--proxy-server"] = setting['--proxy-type'] + '://' + setting['--proxy-auth'] + '@' + setting[
                    '--proxy']
                # setting["phantomjs.page.settings.proxyAuth"] = auths
            else:
                setting["--proxy-server"] = setting['--proxy-type'] + '://' + setting['--proxy']
            self.service_args.update(setting)
            # self._cap.update(setting)

    def find(self, selector):
        """
        通过css选择器获取页面内容
        """
        try:
            return self._driver.find_element_by_css_selector(selector)
        except Exception as e:
            raise CDSpiderCrawlerNotFoundEelement(e, self._base_url, self.final_url, rule=selector)

    def find_all(self, selector):
        """
        通过css选择器获取符合的所有页面内容
        """
        try:
            return self._driver.find_elements_by_css_selector(selector)
        except Exception as e:
            raise CDSpiderCrawlerNotFoundEelement(e, self._base_url, self.final_url, rule=selector)

    def find_by_eq(self, selector, index=0):
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
        :param selector: 选择器
        :param eq: index
        :param is_selected: is selected
        :param is_enabled: is enabled
        :param is_displayed: is displayed
        :param by: 选择器模式，default CSS_SELECTOR
        """
        if len(args) >= 1 and isinstance(args[0], six.string_types):
            kwargs['selector'] = args[0]
            if len(args) >= 2:
                kwargs['eq'] = args[1]
        if 'selector' not in kwargs:
            raise CDSpiderSettingError('Selenium selector must be not none', self._base_url, self.final_url,
                                       rule=kwargs)
        kwargs.setdefault('is_selected', False)
        kwargs.setdefault('is_enabled', False)
        kwargs.setdefault('is_displayed', False)
        kwargs.setdefault('by', By.CSS_SELECTOR)
        _kwargs = dict()
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
        :param eq: index
        :param match: 匹配规则
        :param text: 获取text
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
            raise CDSpiderCrawlerNotFoundEelement('Selenium elements not found', self._base_url, self.final_url,
                                                  rule=kwargs['selector'])
        return element

    @staticmethod
    def _getable(kwargs, element):
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
                pattern = utils.pcre2re(kwargs['text'])
                if text and pattern.search(text) and self._getable(kwargs, element):
                    return element
            elif 'partial' in kwargs and kwargs['partial']:
                if text and text.find(kwargs['text']) != -1 and self._getable(kwargs, element):
                    return element
            else:
                if text == kwargs['text'] and self._getable(kwargs, element):
                    return element

    def _attr(self, elements, **kwargs):
        if 'val' not in kwargs:
            raise CDSpiderSettingError('Selenium val must be not none', self._base_url, self.final_url, rule=kwargs)
        for element in elements:
            attr = element.get_attribute(kwargs['attr'])
            if 'match' in kwargs and kwargs['match']:
                pattern = utils.pcre2re(kwargs['val'])
                if attr and pattern.search(attr) and self._getable(kwargs, element):
                    return element
            elif 'partial' in kwargs and kwargs['partial']:
                if attr and attr.find(kwargs['val']) != -1 and self._getable(kwargs, element):
                    return element
            else:
                if attr == kwargs['val'] and self._getable(kwargs, element):
                    return element

    def _property(self, elements, **kwargs):
        if 'val' not in kwargs:
            raise CDSpiderSettingError('Selenium val must be not none', self._base_url, self.final_url, rule=kwargs)
        for element in elements:
            property_ = element.get_property(kwargs['property'])
            if 'match' in kwargs and kwargs['match']:
                pattern = utils.pcre2re(kwargs['val'])
                if property_ and pattern.search(property_) and self._getable(kwargs, element):
                    return element
            elif 'partial' in kwargs and kwargs['partial']:
                if property_ and property_.find(kwargs['val']) != -1 and self._getable(kwargs, element):
                    return element
            else:
                if property_ == kwargs['val'] and self._getable(kwargs, element):
                    return element

    def _css(self, elements, **kwargs):
        if 'val' not in kwargs:
            raise CDSpiderSettingError('Selenium val must be not none', self._base_url, self.final_url, rule=kwargs)
        for element in elements:
            css = element.value_of_css_property(kwargs['css'])
            if 'match' in kwargs and kwargs['match']:
                pattern = utils.pcre2re(kwargs['val'])
                if css and pattern.search(css) and self._getable(kwargs, element):
                    return element
            elif 'partial' in kwargs and kwargs['partial']:
                if css and css.find(kwargs['val']) != -1 and self._getable(kwargs, element):
                    return element
            else:
                if css == kwargs['val'] and self._getable(kwargs, element):
                    return element


if __name__ == "__main__":
    settings = {
        "config": {
            "selenium": {
                "engine": "remote",
                # "exec_path": "E:/Application/phantomjs/bin/phantomjs"
                "exec_path": "http://127.0.0.1:4444/wd/hub"
            }
        }
    }
    crawler = SeleniumCrawler(**settings)

    # crawler.set_proxy(addr="192.168.163.90:8888", type="http")

    def f(result):
        print(result)


    fetch = {
        "method": "GET",
        "url": "https://www.amazon.cn/dp/B09TR9CX4M/ref=sr_1_1?keywords=%E9%92%88%E7%BB%87%E4%B8%8A%E8%A1%A3&qid=1679497347&sr=8-1&th=1&psc=1",
        "callback": f
    }
    crawler.crawl(**fetch)
    crawler.quit()
