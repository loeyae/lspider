#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2019-1-14 9:57:13
"""
import copy
import time
from . import BaseHandler
from cdspider.database.base import *
from cdspider.libs.constants import *
from cdspider.libs import utils
from cdspider.parser import CustomParser
from cdspider.parser.lib import TimeParser

class ExtendedHandler(BaseHandler):
    """
    extended handler
    :property task 爬虫任务信息 {"mode": "extend", "uuid": SpiderTask.extend uuid}
                   当测试该handler，数据应为 {"mode": "extend", "url": url, "extendRule": 扩展那规则，参考扩展规则}
    """

    def get_scripts(self):
        """
        获取自定义脚本
        """
        try:
            if "uuid" in self.task and self.task['uuid']:
                task = self.db['SpiderTaskDB'].get_detail(self.task['uuid'], self.task['mode'])
                if not task:
                    raise CDSpiderDBDataNotFound("SpiderTask: %s not exists" % self.task['uuid'])
                self.task.update(task)
            rule = self.match_rule() or {}
            return rule.get("scripts", None)
        except:
            return None

    def init_process(self, save):
        """
        初始化爬虫流程
        :output self.process {"request": 请求设置, "parse": 解析规则, "paging": 分页规则, "unique": 唯一索引规则}
        """
        if "extendRule" in self.task:
            self.task['parent_url'] = self.task['url']
            self.task['acid'] = "testing_mode"
            typeinfo = utils.typeinfo(self.task['parent_url'])
            if typeinfo['domain'] != self.task['extendRule']['domain'] or (self.task['extendRule']['subdomain'] and typeinfo['subdomain'] != self.task['extendRule']['subdomain']):
                raise CDSpiderNotUrlMatched()
            crawler = self.get_crawler(self.task.get('extendRule', {}).get('request'))
            crawler.crawl(url=self.task['parent_url'])
            data = utils.get_attach_data(CustomParser, crawler.page_source, self.task['parent_url'], self.task['extendRule'], self.log_level)
            if data == False:
                return None
            url, params = utils.build_attach_url(data, self.task['extendRule'], self.task['parent_url'])
            del crawler
            if not url:
                raise CDSpiderNotUrlMatched()
            self.task['url'] = url
            save['base_url'] = url
            save["hard_code"] = params
            self.task['extendRule']['request']['hard_code'] = params
        else:
            mediaType = self.task.get('mediaType', MEDIA_TYPE_OTHER)
            if mediaType == MEDIA_TYPE_WEIBO:
                article = self.db['WeiboInfoDB'].get_detail(self.task.get('parentid', '0'), select=['url', 'acid'])
            else:
                article = self.db['ArticlesDB'].get_detail(self.task.get('parentid', '0'), select=['url', 'acid'])
            if not article:
                raise CDSpiderHandlerError("aritcle: %s not exists" % self.task['parentid'])
            self.task['parent_url'] = article['url']
            self.task['acid'] = article['acid']
        self.process = self.match_rule()  or {"unique": {"data": None}}
        if not 'data' in self.process['unique'] or not self.process['unique']['data']:
            self.process['unique']['data'] = ','. join(self.process['parse']['item'].keys())
        save['paging'] = True

    def match_rule(self):
        """
        获取匹配的规则
        """
        parse_rule = self.task.get("extendRule", {})
        if not parse_rule:
            '''
            如果task中包含列表规则，则读取相应的规则，否则在数据库中查询
            '''
            ruleId = self.task.get('rid', 0)
            parse_rule = self.db['ExtendRuleDB'].get_detail(ruleId)
            if not parse_rule:
                raise CDSpiderDBDataNotFound("ExtendRule: %s not exists" % ruleId)
            if parse_rule['status'] != ExtendRuleDB.STATUS_ACTIVE:
                raise CDSpiderHandlerError("comment rule not active")
        return parse_rule

    def run_parse(self, rule):
        """
        根据解析规则解析源码，获取相应数据
        :param rule 解析规则
        :input self.response 爬虫结果 {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        :output self.response {"parsed": 解析结果}
        """
        parser = CustomParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
        self.response['parsed'] = parser.parse()

    def run_result(self, save):
        """
        爬虫结果处理
        :param save 保存的上下文信息
        :input self.response {"parsed": 解析结果, "final_url": 请求的url}
        """
        self.crawl_info['crawl_urls'][str(self.page)] = self.response['final_url']
        self.crawl_info['crawl_count']['page'] += 1
        if self.response['parsed']:
            result = copy.deepcopy(self.response['parsed'])
            self.debug("%s result: %s" % (self.__class__.__name__, result))
            if not self.testing_mode:
                '''
                testing_mode打开时，数据不入库
                '''
                result['utime'] = self.crawl_id
                mediaType = self.task.get('mediaType', MEDIA_TYPE_OTHER)
                if mediaType == MEDIA_TYPE_WEIBO:
                    self.db['WeiboInfoDB'].update(self.task.get('parentid', '0'), result)
                else:
                    self.db['ArticlesDB'].update(self.task.get('parentid', '0'), result)
                self.crawl_info['crawl_count']['new_count'] += 1

    def finish(self, save):
        """
        记录抓取日志
        """
        super(ExtendedHandler, self).finish(save)
        if 'uuid' in self.task and self.task['uuid']:
            crawlinfo = self.task.get('crawlinfo', {}) or {}
            self.crawl_info['crawl_end'] = int(time.time())
            crawlinfo[str(self.crawl_id)] = self.crawl_info
            crawlinfo_sorted = [(k, crawlinfo[k]) for k in sorted(crawlinfo.keys())]
            if len(crawlinfo_sorted) > self.CRAWL_INFO_LIMIT_COUNT:
                del crawlinfo_sorted[0]
            s = self.task.get("save")
            if not s:
                s = {}
            s.update(save)
            self.db['SpiderTaskDB'].update(self.task['uuid'], self.task['mode'], {"crawltime": self.crawl_id, "crawlinfo": dict(crawlinfo_sorted), "save": s})
