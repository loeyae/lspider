#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-11-28 21:45:37
"""
import time
import copy
from . import BaseHandler
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.libs.constants import *
from cdspider.parser import ItemParser, CustomParser
from cdspider.parser.lib import TimeParser

class GeneralItemHandler(BaseHandler):
    """
    general item handler
    """

    def get_scripts(self):
        rule = self.match_rule()
        return rule.get("scripts", None)

    def init_process(self):
        self.process = self.match_rule()
        if 'paging' in self.process and self.process['paging']:
            self.process['paging']['url'] = 'base_url'

    def match_rule(self):
        parse_rule = self.task.get("detailRule", {})
        rid = self.task.get('rid', None)
        if rid:
            article = self.db['ArticlesDB'].get_detail(rid, select=['url', 'crawlinfo'])
            if not article:
                raise CDSpiderHandlerError("aritcle: %s not exists" % rid)
            if not 'ulr' in self.task or not self.task['url']:
                self.task["url"] = article['url']
            self.task.setdefault('crawlinfo', article.get('crawlinfo', {}))
        else:
            self.task.setdefault('crawlinfo', {})
        url = self.task.get("url")
        if not url:
            raise CDSpiderHandlerError("url not exists")
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
        if not 'final_url' in self.task['crawlinfo']:
            self.task['crawlinfo']['final_url'] = {str(self.page): final_url}
        else:
            self.task['crawlinfo']['final_url'][str(self.page)] = final_url
        if not 'detailRule' in self.task['crawlinfo']:
            self.task['crawlinfo']['detailRule'] = self.process.get('uuid', 0)
        self.task['crawlinfo']['page'] = self.page

    def _build_result_info(self, **kwargs):
        now = int(time.time())
        result = kwargs.pop('result')
        pubtime = TimeParser.timeformat(str(result.pop('pubtime', '')))
        if pubtime and pubtime > now:
            pubtime = now
        r = {
            "status": kwargs.get('status', ArticlesDB.STATUS_ACTIVE),
            'url': kwargs['final_url'],
            'title': result.pop('title', None),                                # 标题
            'author': result.pop('author', None),                              # 作者
            'content': result.pop('content', None),
            'pubtime': pubtime,                                                # 发布时间
            'channel': result.pop('channel', None),                            # 频道信息
            'crawlinfo': kwargs.get('crawlinfo')
        }
        if all((r['title'], r['author'], r['content'], r['pubtime'])):
            r['status'] = ArticlesDB.STATUS_PARSED
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
        self._build_crawl_info(final_url=self.response['final_url'])
        if self.response['parsed']:
            typeinfo = self._typeinfo(self.response['final_url'])
            self.result2db(save, typeinfo)
            self.result2attach(save, **typeinfo)

    def result2db(self, save, typeinfo):
        result_id = self.task.get("rid", None)
        if not result_id:
            if self.testing_mode:
                inserted, unid = (True, {"acid": "testing_mode", "ctime": self.crawl_id})
                self.debug("%s test mode: %s" % (self.__class__.__name__, unid))
            else:
                inserted, unid = self.db['UniqueDB'].insert(self.get_unique_setting(self.response['final_url'], self.response['parsed']), ctime)
                self.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
            result = self._build_result_info(final_url=self.response['final_url'], typeinfo=typeinfo, result=self.response['parsed'], crawlinfo=self.task['crawlinfo'], **unid)
            if self.testing_mode:
                result_id = 'testing_mode'
                self.debug("%s on_result: %s" % (self.__class__.__name__, result))
            else:
                self.debug("%s on_result formated data: %s" % (self.__class__.__name__, str(result)))
                if inserted:
#                    result['parentid'] = parentid
                    result_id = self.db['ArticlesDB'].insert(result)
                else:
                    item = self.db['ArticlesDB'].get_detail_by_unid(**unid)
                    result_id = item['rid']
                    self.db['ArticlesDB'].update(result_id, result)
            self.task['rid'] = result_id
        else:
            if self.page == 1:
                result = self._build_result_info(final_url=self.response['final_url'], typeinfo=typeinfo, result=self.response['parsed'], crawlinfo=self.task['crawlinfo'])

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

    def finish(self):
        super(GeneralItemHandler, self).finish()
        if self.task.get('rid') and self.task.get('crawlinfo') and not self.testing_mode:
            self.db['ArticlesDB'].update(self.task['rid'], {"crawlinfo": self.task['crawlinfo']})

    def result2attach(self, save, domain, subdomain=None):
        if self.page != 1:
            return
        self.result2comment(save, domain, subdomain)
        self.result2interact(save, domain, subdomain)

    def result2comment(self, save, domain, subdomain = None):
        ruleset = self.db['CommentRuleDB'].get_list_by_subdomain(subdomain, where={"status": self.db['CommentRuleDB'].STATUS_ACTIVE})
        for rule in ruleset:
            url = self.build_attach_url(rule)
            if url:
                cid = self.build_comment_task(url, rule)
                self.task['crawlinfo']['commentRule'] = rule['uuid']
                self.task['crawlinfo']['commentTaskId'] = cid
                self.debug("%s new comment task: %s" % (self.__class__.__name__, str(cid)))
                return
        ruleset = self.db['CommentRuleDB'].get_list_by_domain(domain, where={"status": self.db['CommentRuleDB'].STATUS_ACTIVE})
        for rule in ruleset:
            url = self.build_attach_url(rule)
            if url:
                cid = self.build_comment_task(url, rule)
                self.task['crawlinfo']['commentRule'] = rule['uuid']
                self.task['crawlinfo']['commentTaskId'] = cid
                self.debug("%s new comment task: %s" % (self.__class__.__name__, str(cid)))
                return

    def result2interact(self, save, domain, subdomain = None):
        ruleset = self.db['AttachmentDB'].get_list_by_subdomain(subdomain, where={"status": self.db['AttachmentDB'].STATUS_ACTIVE})
        for rule in ruleset:
            url = self.build_attach_url(rule)
            if url:
                cid = self.build_interact_task(url, rule)
                self.task['crawlinfo']['interactRule'] = rule['uuid']
                self.task['crawlinfo']['interactTaskId'] = cid
                self.debug("%s new interact task: %s" % (self.__class__.__name__, str(cid)))
                return
        ruleset = self.db['AttachmentDB'].get_list_by_domain(domain, where={"status": self.db['AttachmentDB'].STATUS_ACTIVE})
        for rule in ruleset:
            url = self.build_attach_url(rule)
            if url:
                cid = self.build_interact_task(url, rule)
                self.task['crawlinfo']['interactRule'] = rule['uuid']
                self.task['crawlinfo']['interactTaskId'] = cid
                self.debug("%s new interact task: %s" % (self.__class__.__name__, str(cid)))
                return

    def build_attach_url(self, rule):
        if 'preparse' in rule and rule['preparse']:
            parsed = self.attach_preparse(rule['preparse'].get('parse', None))
            if parsed:
                urlrule = rule['preparse'].get('url', None)
                if urlrule:
                    if urlrule['base'] == 'parent_url':
                        urlrule['base'] = self.response['final_url']
                    return utils.build_url_by_rule(urlrule, parsed)
        return None

    def build_comment_task(self, url, rule):
        task = {
            'mode': HANDLER_MODE_COMMENT,                           # handler mode
            'pid': self.task['crawlinfo'].get('pid', 0),            # project id
            'sid': self.task['crawlinfo'].get('sid', 0),            # site id
            'tid': self.task['crawlinfo'].get('tid', 0),            # task id
            'uid': self.task['crawlinfo'].get('uid', 0),            # url id
            'kid': rule['uuid'],                                    # rule id
            'url': url,                                             # url
            'parentid': self.task['rid'],                           # article id
            'expire': 0 if int(rule['expire']) == 0 else int(time.time()) + int(rule['expire'])
        }
        self.debug("%s build comment task: %s" % (self.__class__.__name__, str(task)))
        if not self.testing_mode:
            return self.db['SpiderTaskDB'].insert(task)
        else:
            return 'testing_mode'

    def attach_preparse(self, rule):
        if not rule:
            return None
        def build_rule(item):
            key = item.pop('key')
            if key and item['filter']:
                if item['filter'] == '@value:parent_url':
                    item['filter'] = '@value:%s' % self.response['final_url']
                elif item['filter'].startswith('@url:'):
                    r = item['filter'][5:]
                    v = utils.preg(self.response['final_url'], r)
                    if not v:
                        return None
                    item['filter'] = '@value:%s' % v
                return {key: item}
            return None
        parse = {}
        if isinstance(rule, (list, tuple)):
            for item in rule:
                ret = build_rule(item)
                if ret:
                    parse.update(ret)
        elif isinstance(rule, dict):
            for item in rule.values():
                ret = build_rule(item)
                if ret:
                    parse.update(ret)
        if not parse:
            return None
        parser = CustomParser(source=self.response['last_source'], ruleset=copy.deepcopy(parse), log_level=self.log_level, url=self.response['final_url'])
        parsed = parser.parse()
        return utils.filter(parsed)
