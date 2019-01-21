#-*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-6-20 9:54:45
"""
import six
import time
import re
import copy
import logging
import random
from cdspider import Component
from urllib.parse import urljoin
from cdspider.libs import utils
from cdspider.exceptions import *

class UrlBuilder(Component):
    """
    url builder
    """
    def __init__(self, parser, logger = None, log_level=logging.DEBUG):
        logger = logger or logging.getLogger('spider')
        self._max = 0
        self.log_level = log_level
        self.parser = parser
        super(UrlBuilder, self).__init__(logger, log_level)

    def build(self, kwargs, source, crawler, save):
        """
        解析配置的内容
        """
        if not kwargs or not 'url' in kwargs:
            return kwargs
        self.info("UrlBuilder parse params: %s, save: %s" % (kwargs, save))
        _max = int(kwargs.pop('max', 0) or 0)
        page = int(save.get('page', 1) or 1)
        if _max > 0 and page > _max:
            raise CDSpiderCrawlerMoreThanMaximum("Crawler more than max page: %s" % _max, base_url = save['base_url'])
        data = save.get("post_data", None)
        if data:
            if 'data' in kwargs:
                kwargs['data'].update(data)
            else:
                kwargs['data'] = data
        kwargs['pas'] = []
        kwargs['sub'] = []
        kwargs['fmtdata']= {}

        self._parse_match_code(kwargs, source, save)
        #自增变量设置
        self._parse_incr_data(kwargs, save)
        #随机数变量设置
        self._parse_random_data(kwargs)
        #cookie变量设置
        self._parse_cookie_data(kwargs, crawler, save)
        if (not 'hard_code' in kwargs or not kwargs['hard_code']) and 'hard_code_list' in kwargs and kwargs['hard_code_list']:
            hard_code = random.choice(kwargs['hard_code_list'])
            kwargs['hard_code'] = hard_code
        #自定义常量设置
        self._parse_hard_code(kwargs, save)


        #url组合，内含从页面获取数据解析，从基本url获取数据解析
        url = self._parse_url(kwargs, source, save)
        if (not 'hearders' in kwargs or not kwargs['headers']) and 'headers_list' in kwargs and kwargs['headers_list']:
            headers = random.choice(kwargs['headers_list'])
            kwargs['headers'] = headers
        if (not 'cookies' in kwargs or not kwargs['cookies']) and 'cookies_list' in kwargs and kwargs['cookies_list']:
            cookies = random.choice(kwargs['cookies_list'])
            kwargs['cookies'] = cookies

        keys = ['method', 'data', 'params', 'files', 'json', 'headers', 'cookies']
        params = {"url": url}
        for k,v in kwargs.items():
            if k in keys:
                params[k] = v
        self.info("UrlBuilder parsed params: %s" % params)
        return params

    def _parse_url(self, kwargs, source, save):
        """
        """
        base_url = save.get('base_url')
        if int(save.get('page', 1) or 1) == 1 and int(kwargs.get('first', 0)) == 0:
            url = base_url
        else:
            if isinstance(kwargs['url'], six.string_types):
                if kwargs['url'] == 'base_url':
                    url = base_url
                elif kwargs['url'] == 'parent_url':
                    url = save.get('parent_url')
                else:
                    url = kwargs['url']
            elif isinstance(kwargs['url'], dict):
                setting = kwargs['url']
                _type = setting.get('type')
                if _type == 'base':
                    url = base_url
                elif _type == 'parent':
                    url = save.get('parent_url')
                else:
                    parser = self.parser(source=source, ruleset={"url": {"filter": setting['filter']}}, log_level=self.log_level, url=save['base_url'])
                    parsed = parser.parse()
                    url = parsed['url']
            elif isinstance(kwargs['url'], list):
                setting = kwargs['url']
                url = setting[0].format(self._run_parse(setting[1], source, base_url))
        if not url:
            raise CDSpiderNotUrlMatched('Url not exists', base_url, rule=kwargs)
        return self._complate_url(url, kwargs, save)

    def _parse_match_code(self, kwargs, source, save):
        rule = kwargs.get('match_data')
        if not rule:
            return
        self.info("UrlBuilder run parse start")
        self.debug("UrlBuilder parse match code rule: %s" % str(rule))
        try:
            parser = self.parser(source=source, ruleset=copy.deepcopy(rule), log_level=self.log_level, url=save['base_url'])
            parsed = parser.parse()
            self.debug("UrlBuilder parse match code data: %s" % str(parsed))
            if parsed:
                for k, v in parsed.items():
                    item = rule[k]
                if 'mode' in item and item['mode']:
                    if item['mode'] == 'get':
                        item.setdefault('type', 'url')
                    elif item['mode'] == 'post':
                        item.setdefault('type', 'data')
                    else:
                        item.setdefault('type', item['mode'])

                if int(save.get('page', 1) or 1) == 1 and bool(int(item.get('first') or 0)):
                    self._append_kwargs_data(kwargs, item['type'], k, v)
                else:
                    self._append_kwargs_data(kwargs, item['type'], k, v)
        finally:
            self.info("UrlBuilder run parse end")

    def _parse_hard_code(self, kwargs, save):
        """
        固定值
        """
        data = save.get("hard_code", None)
        if not data and 'hard_code' in kwargs and kwargs['hard_code']:
            self.debug("UrlBuilder parse hard code rule: %s" % str(kwargs['hard_code']))
            data = []
            for item in kwargs['hard_code']:
                if 'mode' in item and item['mode']:
                    if item['mode'] == 'get':
                        item.setdefault('type', 'url')
                    elif item['mode'] == 'post':
                        item.setdefault('type', 'data')
                    else:
                        item.setdefault('type', item['mode'])
                if 'attr' in item:
                    if item['attr'] == 'referer':
                        if not save.get("referer"):
                            data.append({"name": item['name'], "type": item['type'], "value": save.get("base_url")})
                        else:
                            data.append({"name": item['name'], "type": item['type'], "value": save.get("referer")})
                    else:
                        if item['attr'] in save.get('hard_data', {}):
                            data.append({"name": item['name'], "type": item['type'], "value": save.get("hard_data").get(item['attr'])})
                else:
                    data.append({"name": item['name'], "type": item['type'], "value": item['value']})
            save['hard_code'] = data
        if data:
            self.debug("UrlBuilder parse hard code data: %s" % str(data))
            for item in data:
                if item['name']:
                    self._append_kwargs_data(kwargs, item['type'], item['name'], item['value'])

    def _parse_cookie_data(self, kwargs, crawler, save):
        """
        cookie值获取
        """
        data = save.get("cookie_data", None)
        if not data and 'cookie' in kwargs and kwargs['cookie']:
            self.debug("UrlBuilder parse cookie data rule: %s" % str(kwargs['cookie']))
            data = []
            for item in kwargs['cookie']:
                if 'mode' in item and item['mode']:
                    if item['mode'] == 'get':
                        item.setdefault('type', 'url')
                    elif item['mode'] == 'post':
                        item.setdefault('type', 'data')
                    else:
                        item.setdefault('type', item['mode'])
                if 'params' in item and item['params']:
                    cookie_value = crawler.get_cookie(item['value'], **item['params'])
                else:
                    cookie_value = crawler.get_cookie(item['value'])
                value = str(cookie_value)
                value = utils.patch_result(value, item)
                data.append({"name": item['name'], "type": item['type'], "value": value})
            save['cookie_data'] = data
        if data:
            self.debug("UrlBuilder parse cookie data data: %s" % str(data))
            for item in data:
                self._append_kwargs_data(kwargs, item['type'], item['name'], value)

    def _parse_random_data(self, kwargs):
        """
        随机参数设置
        """
        if 'random' in kwargs and kwargs['random']:
            self.debug("UrlBuilder parse random data rule: %s" % str(kwargs['random']))
            for item in kwargs['random']:
                if not item['name']:
                    continue
                assert 'value' in item, "invalid setting value of random data"
                if 'mode' in item and item['mode']:
                    if item['mode'] == 'get':
                        item.setdefault('type', 'url')
                    elif item['mode'] == 'post':
                        item.setdefault('type', 'data')
                    else:
                        item.setdefault('type', item['mode'])
                item.setdefault('type', 'url')
                rndkey = item['name']
                rndtype = item['value']
                if rndtype == 'ms':
                    rndval = "%.4f" % time.time()
                elif rndtype == 'ms_':
                    rndval = ("%.4f" % time.time()).replace('.', '_')
                elif rndtype == 's':
                    rndval = "%.0f" % time.time()
                else:
                    len_ = int(rndtype)
                    if len_ > 20:
                        len_ = 20
                    rndval = ("%.0f" % (time.time() * (len_ >= 10 and 10 ** (len_ - 10) or 10000)))[(0-len_):]
                rndval = utils.patch_result(rndval, item)
                self._append_kwargs_data(kwargs, item['type'], rndkey, rndval)

    def _parse_incr_data(self, kwargs, save):
        """
        自增参数设置
        """
        if 'incr_data' in kwargs and kwargs['incr_data']:
            self.debug("UrlBuilder parse incr data rule: %s" % str(kwargs['incr_data']))
            incr_data = copy.deepcopy(kwargs['incr_data'])
            if not isinstance(incr_data, list):
                incr_data = [incr_data]
            page = int(save['page'])
            for i in range(len(incr_data)):
                assert 'name' in incr_data[i] and incr_data[i]['name'], "invalid setting name of incr_data"
                assert 'value' in incr_data[i], "invalid setting value of incr_data"
                item = copy.deepcopy(incr_data[i])
                step = int(item.get('step', 1))
                value = str(int(item['value']) + (page - 1) * step)
                value = utils.patch_result(value, item)
                if 'mode' in item and item['mode']:
                    if item['mode'] == 'get':
                        item.setdefault('type', 'url')
                    elif item['mode'] == 'post':
                        item.setdefault('type', 'data')
                    else:
                        item.setdefault('type', item['mode'])
                if page == 1 and bool(int(item.get('first') or 0)):
                    self._append_kwargs_data(kwargs, item['type'], item['name'], value)
                else:
                    self._append_kwargs_data(kwargs, item['type'], item['name'], value)

    def _append_kwargs_data(self, kwargs, type, name, value):
        """
        增加参数
        """
        if type == 'url':
            kwargs['pas'].append(str(name) +'='+ str(value))
        elif type == 'format':
            kwargs['fmtdata'][str(name)] = str(value)
        elif type == 'replace':
            replace, subject = utils.rule2subitem(str(name), str(value))
            kwargs['sub'].append({'pattern': replace, 'repl': subject})
        else:
            if type in kwargs:
                kwargs[type][str(name)] = str(value)
            else:
                kwargs[type] = {str(name): str(value)}

    def _complate_url(self, url, kwargs, save):
        """
        组合url
        """
        url = self._join_url(url, save)
        if 'fmtdata' in kwargs and kwargs['fmtdata']:
            url = url.format(**kwargs['fmtdata'])
        if 'pas' in kwargs and kwargs['pas']:
            url = utils.build_query(url, '&'.join(kwargs['pas']))
        if 'path' in kwargs and kwargs['path']:
            url += '/' + '/'.join(kwargs['path'].values())
        if 'sub' in kwargs and kwargs['sub']:
            for item in kwargs['sub']:
                url = re.sub(item['pattern'], item['repl'], url)
        return url and utils.remove_whtiespace_plus(url) or url

    def _join_url(self, url, save):
        """
        join url
        """
        if "referer" in save and save["referer"]:
            return urljoin(save["referer"], url)
        return urljoin(save["base_url"], url)
