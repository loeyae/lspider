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
import logging
import random
from urllib.parse import urljoin
from cdspider.libs import utils
from cdspider.exceptions import *

class UrlBuilder():
    """
    url builder
    """
    def __init__(self, logger = None, log_level=logging.DEBUG):
        self.logger = logger or logging.getLogger('spider')
        self.log_level = log_level
        self.logger.setLevel(log_level)

    def build(self, kwargs, source, crawler, save):
        """
        解析配置的内容
        """
        if not kwargs or not 'url' in kwargs:
            return kwargs
        self.logger.info("UrlBuilder parse params: %s" % kwargs)
        data = save.get("view_data", None)
        if not data and 'view_data' in kwargs:
            data = self._run_parse(kwargs['view_data'], source, save.get('base_url'))
            if 'data' in kwargs:
                kwargs['data'].update(data)
            else:
                kwargs['data'] = data
        elif data and 'view_data' in kwargs:
            data = utils.dictunion(data, kwargs['view_data'])
            if 'data' in kwargs:
                kwargs['data'].update(data)
            else:
                kwargs['data'] = data

        kwargs['pas'] = []
        kwargs['sub'] = []
        kwargs['fmtdata']= {}
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
        self.logger.info("UrlBuilder parsed params: %s" % params)
        return params

    def _run_parse(self, rule, source, url=None):
        self.logger.info("UrlBuilder run parse start")
        try:
            data = {}
            for k, item in rule.items():
                self.logger.info("UrlBuilder run parse: %s => %s" % (k, item))
                for parser_name, r in item.items():
                    parser = utils.load_parser(parser_name, source=source, ruleset={k: r}, log_level=self.log_level, url=url)
                    parsed = parser.parse()
                    self.logger.info("UrlBuilder run parse matched data: %s" % str(parsed))
                    if parsed:
                        data.update(parsed)
                        break
        finally:
            self.logger.info("UrlBuilder run parse end")
        return data

    def _parse_url(self, kwargs, source, save):
        """
        """
        base_url = save.get('base_url')
        url = None
        if isinstance(kwargs['url'], six.string_types):
            if kwargs['url'] == 'base_url':
                url = base_url
            elif kwargs['url'] == 'parent_url':
                url = save.get('parent_url')
            else:
                url = kwargs['url']
        elif isinstance(kwargs['url'], dict):
            setting = kwargs['url']
            view_data = save.get('view_data', None)
            if not view_data and 'view_data' in setting:
                view_data = self._run_parse(setting['view_data'], source, base_url)
                view_data = utils.filter(view_data)
                if not view_data:
                    raise CDSpiderNotUrlMatched('Invalid view data', base_url, kwargs['url']['base'])
                save['view_data'] = view_data
            elif view_data and 'view_data' in setting:
                view_data = utils.dictunion(view_data, setting['view_data'])
            match_data = save.get('match_data', None)
            if not match_data and 'match' in setting:
                match_data = {}
                for k,v in setting['match'].items():
                    pattern, key = utils.rule2pattern(v)
                    mat = re.search(pattern, base_url)
                    if not mat:
                        raise CDSpiderNotUrlMatched('Invalid match data', base_url, url)
                    match_data[k] = mat.group(key)
                save['match_data'] = match_data
            if 'base' in setting:
                if setting['base'] == 'base_url':
                    url = base_url
                else:
                    url = setting['base']
                if view_data:
                    for k,v in view_data.items():
                        self._append_kwargs_data(kwargs, 'format', k, v)
                if match_data:
                    for k,v in match_data.items():
                        self._append_kwargs_data(kwargs, 'format', k, v)
            elif 'element' in setting:
                elementdata = self._run_parse({'element': setting['element']}, source, base_url)
                if elementdata and elementdata['element']:
                    url = elementdata['element']
                else:
                    raise CDSpiderNotUrlMatched('Invalid element data', base_url, rule={'element': setting['element']})
                if view_data:
                    for k,v in view_data.items():
                        self._append_kwargs_data(kwargs, 'url', k, v)
                if match_data:
                    for k,v in match_data.items():
                        self._append_kwargs_data(kwargs, 'url', k, v)
            else:
                raise CDSpiderNotUrlMatched('Invalid url setting', base_url, rule={'url': kwargs['url']})

            if 'response_data' in setting:
                try:
                    response_data = self._run_parse(setting['response_data'], source, base_url)
                    response_data = utils.filter(response_data)
                    if response_data:
                        for k,v in response_data.items():
                            self._append_kwargs_data(kwargs, 'url', k, v)
                except:
                    pass
        elif isinstance(kwargs['url'], list):
            setting = kwargs['url']
            url = setting[0].format(self._run_parse(setting[1], source, base_url))
        if not url:
            raise CDSpiderNotUrlMatched('Url not exists', base_url, rule=kwargs)
        return self._complate_url(url, kwargs, save)

    def _parse_hard_code(self, kwargs, save):
        """
        固定值
        """
        data = save.get("hard_code", None)
        if not data and 'hard_code' in kwargs and kwargs['hard_code']:
            self.logger.debug("UrlBuilder parse hard code rule: %s" % str(kwargs['hard_code']))
            data = []
            for item in kwargs['hard_code']:
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
            self.logger.debug("UrlBuilder parse hard code data: %s" % str(data))
            for item in data:
                self._append_kwargs_data(kwargs, item['type'], item['name'], item['value'])

    def _parse_cookie_data(self, kwargs, crawler, save):
        """
        cookie值获取
        """
        data = save.get("cookie_data", None)
        if not data and 'cookie' in kwargs and kwargs['cookie']:
            self.logger.debug("UrlBuilder parse cookie data rule: %s" % str(kwargs['cookie']))
            data = []
            for item in kwargs['cookie']:
                if 'params' in item and item['params']:
                    cookie_value = crawler.get_cookie(item['value'], **item['params'])
                else:
                    cookie_value = crawler.get_cookie(item['value'])
                value = str(cookie_value)
                if 'prefix' in item and item['prefix']:
                    value = item['prefix'] + value
                if 'suffix' in item and item['suffix']:
                    value += item['suffix']
                data.append({"name": item['name'], "type": item['type'], "value": value})
            save['cookie_data'] = data
        if data:
            self.logger.debug("UrlBuilder parse cookie data data: %s" % str(data))
            for item in data:
                self._append_kwargs_data(kwargs, item['type'], item['name'], value)

    def _parse_random_data(self, kwargs):
        """
        随机参数设置
        """
        if 'random' in kwargs and kwargs['random']:
            self.logger.debug("UrlBuilder parse random data rule: %s" % str(kwargs['random']))
            for item in kwargs['random']:
                assert 'name' in item and item['name'], "invalid setting name of random data"
                assert 'value' in item, "invalid setting value of random data"
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
                    len = int(rndtype)
                    rndval = ("%.0f" % (time.time() * (len >= 10 and 10 ** (len - 10) or 10000)))[(0-len):]
                if 'base' in item:
                    rndval = "%s%s" % (item['base'], rndval)
                if 'prefix' in item and item['prefix']:
                    rndval = item['prefix'] + str(rndval)
                if 'suffix' in item and item['suffix']:
                    rndval = str(rndval) + item['suffix']
                self._append_kwargs_data(kwargs, item['type'], rndkey, rndval)

    def _parse_incr_data(self, kwargs, save):
        """
        自增参数设置
        """
        if 'incr_data' in kwargs and kwargs['incr_data']:
            self.logger.debug("UrlBuilder parse incr data rule: %s" % str(kwargs['incr_data']))
            incr_data = kwargs['incr_data'].copy()
            if not isinstance(incr_data, list):
                incr_data = [incr_data]
            if not "incr_data" in save or not save['incr_data']:
                for i in range(len(incr_data)):
                    assert 'name' in incr_data[i] and incr_data[i]['name'], "invalid setting name of incr_data"
                    assert 'value' in incr_data[i], "invalid setting value of incr_data"
                    if not incr_data[i]['value']:
                        incr_data[i]['value'] = 1
                    if not 'step' in incr_data[i] or not incr_data[i]['step']:
                        incr_data[i]['step'] = 1
                    incr_data[i]['base_page'] = int(incr_data[i]['value'])
                    if abs(int(incr_data[i].get('step', 1))) > 1 and incr_data[i]['base_page'] > 0:
                        incr_data[i]['value'] = (int(incr_data[i].get('value', 0)) - 1) * abs(int(incr_data[i].get('step', 1)))
                    incr_data[i].setdefault('type', 'url')
                save['incr_data'] = incr_data
            for i in range(len(save['incr_data'])):
                item = save['incr_data'][i]
                sitem = incr_data[i]
                if not 'max' in item or not item['max']:
                    item['max'] = 0
                step = int(item.get('step', 1))
                if not item.get('isfirst', True):
                    item['value'] = int(item['value']) + step
                if "max" in item and int(item["max"]) > 0:
                    page = (int(item['value']) - int(item['base_page']) * abs(step)) / step + 1
                    if page > int(item['max']):
                        raise CDSpiderCrawlerMoreThanMaximum("Crawler more than max page: %s" % item['max'],
                                base_url = save['base_url'], incr_data = item)
                value = str(item['value'])
                if 'prefix' in item and item['prefix']:
                    value = item['prefix'] + value
                if 'suffix' in item and item['suffix']:
                    value += item['suffix']
                save['incr_data'][i]['value'] = item['value']
                if not item.get('isfirst', True):
                    self._append_kwargs_data(kwargs, item['type'], item['name'], value)
                elif item.get('first', False):
                    self._append_kwargs_data(kwargs, item['type'], item['name'], item['base_page'])
                save['incr_data'][i]['isfirst'] = False

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
