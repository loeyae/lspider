#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-11-21 20:45:56
"""
import copy
import time
from . import BaseHandler
from urllib.parse import urljoin, urlparse, urlunparse
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.libs.constants import *
from cdspider.parser import ListParser
from cdspider.parser.lib import TimeParser

class GeneralListHandler(BaseHandler):
    """
    general list handler
    """

    def route(self, mode, save):
        if not "id" in save:
            save['id'] = 0
        if mode == ROUTER_MODE_PROJECT:
            for item in self.db['ProjectsDB'].get_new_list(save['id'], select=["uuid"]):
                if item['uuid'] > save['id']:
                    save['id'] = item['uuid']
                yield item['uuid']
        elif mode == ROUTER_MODE_SITE:
            if not "pid" in save:
                save["pid"] = 0
            for item in self.db['ProjectsDB'].get_new_list(save['pid'], select=["uuid"]):
                for each in self.db['SitesDB'].get_new_list(save['id'], item['uuid'], select=["uuid"]):
                    if each['uuid'] > save['id']:
                        save['id'] = each['uuid']
                    yield each['uuid']
                if item['uuid'] > save['pid']:
                    save['pid'] = item['uuid']

    def schedule(self, message, save):
        mode = message['mode']
        if not 'id' in save:
            save['id'] = 0
        if mode == ROUTER_MODE_PROJECT:
            for item in self.db['UrlsDB'].get_new_list_by_pid(save['id'], message['item'], select=['uuid', 'url']):
                yield item
                if item['uuid'] > save['id']:
                    save['id'] = item['uuid']
        elif mode == ROUTER_MODE_SITE:
            for item in self.db['UrlsDB'].get_new_list(save['id'], message['item'], select=['uuid', 'url']):
                yield item
                if item['uuid'] > save['id']:
                    save['id'] = item['uuid']

    def get_scripts(self):
        if "listRule" in self.task and self.task['listRule']:
            rule = copy.deepcopy(self.task['listRule'])
        else:
            urls = self.db['UrlsDB'].get_detail(self.task['uuid'])
            rule = self.db['ListRuleDB'].get_detail(urls['ruleId'])
        return rule.get("scripts", None)

    def init_process(self):
        if "listRule" in self.task and self.task['listRule']:
            rule = copy.deepcopy(self.task['listRule'])
        else:
            urls = self.db['UrlsDB'].get_detail(self.task['uuid'])
            rule = self.db['ListRuleDB'].get_detail(urls['ruleId'])
        self.process =  {
            "request": rule.get("request", self.DEFAULT_PROCESS),
            "parse": rule.get("parse", None),
            "paging": self.format_paging(rule.get("paging", None)),
            "unique": rule.get("unique", None),
        }

    def get_unique_setting(self, url, data):
        """
        获取生成唯一ID的字段
        """
        identify = self.process.get('unique', None)
        subdomain, domain = utils.parse_domain(url)
        if not subdomain:
            parsed = urlparse(url)
            arr = list(parsed)
            arr[1] = "www.%s" % arr[1]
            u = urlunparse(arr)
        else:
            u = url
        if identify:
            if 'url' in identify and identify['url']:
                rule, key = utils.rule2pattern(identify['url'])
                if rule and key:
                    ret = re.search(rule, url)
                    if ret:
                        u = ret.group(key)
                else:
                    ret = re.search(identify['url'], url)
                    if ret:
                        u = ret.group(0)
            if 'query' in identify and identify['query']:
                u = utils.build_filter_query(url, identify['query'])
            if 'data' in identify and identify['data']:
                udict = dict.fromkeys(identify['data'])
                query = utils.dictunion(data, udict)
                return utils.build_query(u, query)
        return u

    def run_parse(self, rule):
        parser = ListParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
        self.response['parsed'] = parser.parse()

    def _build_crawl_info(self, final_url):
        return {
                "uid": self.task.get("uuid"),
                "url": final_url,
                "crawl_id": self.crawl_id,
        }

    def _build_result_info(self, **kwargs):
        now = int(time.time())
        result = kwargs.get('result', {})
        pubtime = TimeParser.timeformat(str(result.pop('pubtime', '')))
        if pubtime and pubtime > now:
            pubtime = now
        r = {
            "status": kwargs.get('status', ArticlesDB.STATUS_INIT),
            'url': kwargs['final_url'],
            'domain': kwargs.get("typeinfo", {}).get('domain', None),          # 站点域名
            'subdomain': kwargs.get("typeinfo", {}).get('subdomain', None),    # 站点域名
            'title': result.pop('title', None),                                # 标题
            'author': result.pop('author', None),                              # 作者
            'pubtime': pubtime,                                                # 发布时间
            'channel': result.pop('channel', None),                            # 频道信息
            'crawlinfo': kwargs.get('crawlinfo'),
            'acid': kwargs['unid'],                                            # unique str
            'ctime': kwargs.get('ctime', self.crawl_id),
            }
        return r

    def _domain_info(self, url):
        subdomain, domain = utils.parse_domain(url)
        if not subdomain:
            subdomain = 'www'
        return "%s.%s" % (subdomain, domain), domain

    def _typeinfo(self, url):
        subdomain, domain = self._domain_info(url)
        return {"domain": domain, "subdomain": subdomain}

    def run_result(self, save):
        if self.response['parsed']:
            ctime = self.crawl_id
            new_count = self.crawl_info['crawl_count']['new_count']
            formated = self.build_url_by_rule(self.response['parsed'], self.response['final_url'])
            for item in formated:
                inserted, unid = self.db['UniqueDB'].insert(self.get_unique_setting(item['url'], {}), ctime)
                self.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
                if inserted:
                    crawlinfo =  self._build_crawl_info(self.response['final_url'])
                    typeinfo = self._typeinfo(item['url'])
                    result = self._build_result_info(final_url=item['url'], typeinfo=typeinfo, crawlinfo=crawlinfo, result=item, **unid)
                    result_id = self.db['ArticlesDB'].insert(result)
                    if not result_id:
                        raise CDSpiderDBError("Result insert failed")
                    self.crawl_info['crawl_count']['new_count'] += 1
                    self.build_item_task(result_id)
            if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
                self.crawl_info['crawl_count']['repeat_count'] += 1
                self.on_repetition()

    def url_prepare(self, url):
        return url

    def build_url_by_rule(self, data, base_url = None):
        if not base_url:
            base_url = self.task.get('url')
        urlrule = self.process.get("url")
        formated = []
        for item in data:
            if not 'url' in item or not item['url']:
                raise CDSpiderError("url no exists: %s @ %s" % (str(item), str(task)))
            if item['url'].startswith('javascript') or item['url'] == '/':
                continue
            try:
                item['url'] = self.url_prepare(item['url'])
            except:
                continue
            if urlrule and 'name' in urlrule and urlrule['name']:
                parsed = {urlrule['name']: item['url']}
                item['url'] = utils.build_url_by_rule(urlrule, parsed)
            else:
                item['url'] = urljoin(base_url, item['url'])
            formated.append(item)
        return formated


    def build_item_task(self, rid):
        """
        生成详情抓取任务并入队
        """
        message = {
            'mode': HANDLER_MODE_DEFAULT_ITEM,
            'rid': rid,
        }
        self.queue['scheduler2spider'].put_nowait(message)
