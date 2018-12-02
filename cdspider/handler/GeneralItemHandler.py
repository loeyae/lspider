#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-11-28 21:45:37
"""
import copy
from . import BaseHandler
from cdspider.libs import utils
from cdspider.libs.constants import *
from cdspider.parser import ItemParser

class GeneralItemHandler(BaseHandler):
    """
    general item handler
    """

    def get_scripts(self):
        rule = self.match_rule()
        return rule.get("scripts", None)

    def init_process(self):
        self.process = self.match_rule()
        self.process['paging'] = self.format_paging(self.process['paging'])

    def match_rule(self):
        parse_rule = self.task.get("detailRule", {})
        url = self.task.get("url", None)
        if not url:
            rid = self.task['rid']
            article = self.db['ArticlesDB'].get_detail(rid, select=['url'])
            url = article['url']
            self.task.setdefault("url", url)
        if not parse_rule:
            subdomain, domain = utils.parse_domain(url)
            if subdomain:
                parserule_list = self.db['ParseRuleDB'].get_list_by_subdomain(subdomain)
                for item in parserule_list:
                    if not parse_rule:
                        parse_rule = item
                    if  'urlPattern' in item and item['urlPattern']:
                        u = utils.preg(url, item['urlPattern'])
                        if u:
                            return item
            else:
                parserule_list = self.db['ParseRuleDB'].get_list_by_domain(domain)
                for item in parserule_list:
                    if not parse_rule:
                        parse_rule = item
                    if  'urlPattern' in item and item['urlPattern']:
                        u = utils.preg(url, item['urlPattern'])
                        if u:
                            return item
        return parse_rule

    def run_parse(self, rule):
        parser = ItemParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
        self.response['parsed'] = parser.parse()

    def _build_crawl_info(self, final_url):
        return {
                "uid": self.task.get("uuid"),
                "url": final_url,
                "crawl_id": self.crawl_id,
        }

    def _build_result_info(self, **kwargs):
        now = int(time.time())
        pubtime = TimeParser.timeformat(str(result.pop('pubtime', '')))
        if pubtime and pubtime > now:
            pubtime = now
        r = {
            "status": kwargs.get('status', ArticlesDB.STATUS_INIT),
            'url': kwargs['final_url'],
            'title': result.pop('title', None),                                # 标题
            'author': result.pop('author', None),                              # 作者
            'pubtime': pubtime,                                                # 发布时间
            'channel': result.pop('channel', None),                            # 频道信息
            'crawlinfo': kwargs.get('crawlinfo')
        }
        if "unid" in kwargs:
            r['acid'] = kwargs['unid']                                         # unique str
        if "ctime" in kwargs:
            r['ctime'] = kwargs['ctime']
        if "typeinfo" in kwargs:
            r['domain'] = kwargs.get("typeinfo", {}).get('domain', None),          # 站点域名
            r['subdomain'] = kwargs.get("typeinfo", {}).get('subdomain', None),    # 站点域名
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
            result_id = self.task.get("rid", None)
            if not result_id:
                if self.testing_mode:
                    inserted, unid = (True, {"acid": "test_mode", "ctime": self.crawl_id})
                    self.debug("%s test mode: %s" % (self.__class__.__name__, unid))
                else:
                    inserted, unid = self.db['UniqueDB'].insert(self.get_unique_setting(self.response['final_url'], self.response['parsed']), ctime)
                    self.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
                crawlinfo = self._build_crawl_info(final_url=self.response['final_url'])
                typeinfo = self._typeinfo(self.response['final_url'])
                result = self._build_result_info(final_url=self.response['final_url'], typeinfo=typeinfo, result=self.response['parsed'], crawlinfo=crawlinfo, source=utils.decode(page_source), **unid)
                if self.testing_mode:
                    self.debug("%s on_result: %s" % (self.__class__.__name__, result))
                else:
                    self.debug("%s on_result formated data: %s" % (self.__class__.__name__, str(result)))
                    if inserted:
    #                    result['parentid'] = parentid
                        result_id = self.db['ArticlesDB'].insert(result)
                        self.task['rid'] = result_id
                    else:
                        item = self.db['ArticlesDB'].get_detail_by_unid(**unid)
                        result_id = item['rid']
                        self.db['ArticlesDB'].update(result_id, result)
            else:
                if self.page == 1:
                    crawlinfo = self._build_crawl_info(final_url=self.response['final_url'])
                    typeinfo = self._typeinfo(self.response['final_url'])
                    result = self._build_result_info(final_url=self.response['final_url'], typeinfo=typeinfo, result=self.response['parsed'], crawlinfo=crawlinfo, source=utils.decode(page_source))

                    if self.testing_mode:
                        self.debug("%s on_result: %s" % (self.__class__.__name__, result))
                    else:
                        self.debug("%s on_result formated data: %s" % (self.__class__.__name__, str(result)))
                        self.db['ArticlesDB'].update(result_id, result)
                else:
                    if self.testing_mode:
                        self.debug("%s on_result: %s" % (self.__class__.__name__, self.response['parsed']))
                    else:
                        result = self.db['ArticlesDB'].get_detail(result_id)
                        content = result['content']
                        if 'content' in self.response['parsed'] and self.response['parsed']['content']:
                            content = '%s\r\n\r\n%s' % (content, self.response['parsed']['content'])
                            self.debug("%s on_result content: %s" % (self.__class__.__name__, content))
                            self.db['ArticlesDB'].update(result_id, {"content": content})

            if not result_id:
                raise CDSpiderDBError("Result insert failed")
