#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-2 15:16:45
"""
import copy
import time
from . import BaseHandler
from cdspider.database.base import *
from cdspider.libs.constants import *
from cdspider.libs import utils
from cdspider.parser import CustomParser

class InteractHandler(BaseHandler):
    """
    interact handler
    :property task 爬虫任务信息 {"mode": "interact", "uuid": SpiderTask.interact uuid}
                   当测试该handler，数据应为 {"mode": "interact", "url": url, "interactionNumRule": 互动数规则，参考互动数规则}
    """
    def get_scripts(self):
        """
        获取自定义脚本
        """
        try:
            rule = self.match_rule()
            return rule.get("scripts", None)
        except:
            return None

    def init_process(self, save):
        """
        初始化爬虫流程
        :output self.process {"request": 请求设置, "parse": 解析规则, "paging": 分页规则, "unique": 唯一索引规则}
        """
        if "interactionNumRule" in self.task:
            self.task['parent_url'] = self.task['url']
            self.task['acid'] = "testing_mode"
            typeinfo = utils.typeinfo(self.task['parent_url'])
            if typeinfo['domain'] != self.task['interactionNumRule']['domain'] or (self.task['interactionNumRule']['subdomain'] and typeinfo['subdomain'] != self.task['interactionNumRule']['subdomain']):
                raise CDSpiderNotUrlMatched()
            crawler = self.get_crawler(self.task.get('interactionNumRule', {}).get('request'))
            crawler.crawl(url=self.task['parent_url'])
            data = utils.get_attach_data(CustomParser, crawler.page_source, self.task['parent_url'], self.task['interactionNumRule'], self.log_level)
            if data == False:
                return None
            url, params = utils.build_attach_url(data, self.task['interactionNumRule'], self.task['parent_url'])
            del crawler
            if not url:
                raise CDSpiderNotUrlMatched()
            self.task['url'] = url
            save['base_url'] = url
            save["hard_code"] = params
            self.task['interactionNumRule']['request']['hard_code'] = params
        else:
            mediaType = self.task.get('mediaType', MEDIA_TYPE_OTHER)
            if mediaType == MEDIA_TYPE_WEIBO:
                article = self.db['WeiboInfoDB'].get_detail(self.task.get('parentid', '0'), select=['url', 'acid'])
            else:
                article = self.db['ArticlesDB'].get_detail(self.task.get('parentid', '0'), select=['url', 'acid'])
            if not article:
                raise CDSpiderHandlerError("aritcle: %s not exists" % self.task['parentid'])
            self.task['acid'] = article['acid']
        self.process = self.match_rule()

    def match_rule(self):
        """
        获取匹配的规则
        """
        parse_rule = self.task.get("interactionNumRule", {})
        if not parse_rule:
            '''
            如果task中包含列表规则，则读取相应的规则，否则在数据库中查询
            '''
            ruleId = self.task.get('rid', 0)
            parse_rule = self.db['InteractDB'].get_detail(ruleId)
            if not parse_rule:
                raise CDSpiderDBDataNotFound("interactionNumRule: %s not exists" % ruleId)
            if parse_rule['status'] != InteractDB.STATUS_ACTIVE:
                raise CDSpiderHandlerError("interaction num rule not active")
        return parse_rule

    def run_parse(self, rule):
        """
        根据解析规则解析源码，获取相应数据
        :param rule 解析规则
        :input self.response 爬虫结果 {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        :output self.response {"parsed": 解析结果}
        """
        def build_rule(item):
            key = item.pop('key')
            if key and item['filter']:
                return {key: item}
            return None
        r = {}
        for item in rule:
            _r = build_rule(item)
            if _r:
                r.update(_r)
        parser = CustomParser(source=self.response['last_source'], ruleset=r, log_level=self.log_level, url=self.response['final_url'])
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
            rid = self.task.get('parentid', None)
            result = copy.deepcopy(self.response['parsed'])
            attach_data = self.db['AttachDataDB'].get_detail(rid)
            if attach_data:
                if not "crawlinfo" in attach_data or not attach_data['crawlinfo']:
                    #爬虫信息记录
                    result['crawlinfo'] = {
                        'pid': self.task['pid'],                        # project id
                        'sid': self.task['sid'],                        # site id
                        'tid': self.task['tid'],                        # task id
                        'uid': self.task['uid'],                        # url id
                        'kid': self.task['kid'],                        # keyword id
                        'stid': self.task['uuid'],                      # spider task id
                        'ruleId': self.task['rid'],                     # interactionNumRule id
                        'final_url': self.response['final_url'],        # 请求url
                    }
                elif not "ruleId" in attach_data['crawlinfo'] or not attach_data['crawlinfo']['ruleId']:
                    crawlinfo = attach_data['crawlinfo']
                    crawlinfo['ruleId'] = self.task['rid']
                    result['crawlinfo'] = crawlinfo
                result['utime'] = int(time.time())
                result['mediaType'] = self.task.get('mediaType', MEDIA_TYPE_OTHER)
                self.debug("%s result: %s" % (self.__class__.__name__, result))
                if not self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    self.db['AttachDataDB'].update(rid, result)
                    self.build_sync_task(rid)
                self.crawl_info['crawl_count']['repeat_count'] += 1
            else:
                #爬虫信息记录
                result['crawlinfo'] = {
                    'pid': self.task['pid'],                        # project id
                    'sid': self.task['sid'],                        # site id
                    'tid': self.task['tid'],                        # task id
                    'uid': self.task['uid'],                        # url id
                    'kid': self.task['kid'],                        # keyword id
                    'ruleId': self.task['rid'],                     # interactionNumRule id
                    'list_url': self.response['final_url'],         # 列表url
                }
                result['ctime'] = self.crawl_id
                result['acid'] = self.task['acid']
                result['utime'] = 0
                result['rid'] = rid
                self.debug("%s result: %s" % (self.__class__.__name__, result))
                if not self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    self.db['AttachDataDB'].insert(result)
                    self.build_sync_task(rid)

                self.crawl_info['crawl_count']['new_count'] += 1

    def finish(self, save):
        """
        记录抓取日志
        """
        super(InteractHandler, self).finish(save)
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

    def build_sync_task(self, rid):
        """
        生成同步任务并入队
        """
        message = {'rid': rid}
        self.queue['attach2kafka'].put_nowait(message)
