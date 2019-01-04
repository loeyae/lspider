#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-22 10:28:27
"""
import re
import copy
import time
import traceback
import urllib.request
from . import GeneralSearchHandler, Loader
from urllib.parse import urljoin, urlparse, quote_plus
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.libs.constants import *
from cdspider.parser import ListParser
from cdspider.parser.lib import TimeParser


class WechatSearchHandler(GeneralSearchHandler):
    """
    general search handler
    :property task 爬虫任务信息 {"mode": "search", "uuid": SpiderTask.list uuid}
                   当测试该handler，数据应为 {"mode": "search", "keyword": 关键词规则, "authorListRule": 列表规则，参考列表规则}
    """

    def route(self, mode, save):
        """
        schedule 分发
        :param mode  project|site 分发模式: 按项目|按站点
        :param save 传递的上下文
        :return 包含uuid的迭代器，项目模式为项目的uuid，站点模式为站点的uuid
        :notice 该方法返回的迭代器用于router生成queue消息，以便plantask听取，消息格式为:
        {"mode": route mode, "h-mode": handler mode, "uuid": uuid}
        """
        if not "id" in save:
            '''
            初始化上下文中的id参数,该参数用于数据查询
            '''
            save["id"] = 0
        if mode == ROUTER_MODE_PROJECT:
            '''
            按项目分发
            '''
            for item in self.db['ProjectsDB'].get_new_list(save['id'], select=["uuid"]):
                if item['uuid'] > save['id']:
                    save['id'] = item["uuid"]
                yield item['uuid']
        elif mode == ROUTER_MODE_SITE:
            '''
            按站点分发
            '''
            if not "pid" in save:
                '''
                初始化上下文中的pid参数,该参数用于项目数据查询
                '''
                save["pid"] = 0
            for item in self.db['ProjectsDB'].get_new_list(save['pid'], select=["uuid"]):
                while True:
                    has_item = False
                    for each in self.db['SitesDB'].get_new_list(save['id'], item['uuid'], select=["uuid"]):
                        has_item = True
                        if each['uuid'] > save['id']:
                            save['id'] = each['uuid']
                        yield each['uuid']
                    if not has_item:
                        break
                if item['uuid'] > save['pid']:
                    save['pid'] = item['uuid']
        elif mode == ROUTER_MODE_TASK:
            '''
            按任务分发
            '''
            if not "pid" in save:
                '''
                初始化上下文中的pid参数,该参数用于项目数据查询
                '''
                save["pid"] = 0
            for item in self.db['ProjectsDB'].get_new_list(save['pid'], select=["uuid"]):
                while True:
                    has_item = False
                    for each in self.db['TaskDB'].get_new_list(save['id'], where={"pid": item['uuid'], "type": {"$in": [TASK_TYPE_SEARCH]}, "mediaType": {"$in": [MEDIA_TYPE_WECHAT]}}, select=["uuid"]):
                        has_item = True
                        if each['uuid'] > save['id']:
                            save['id'] = each['uuid']
                        yield each['uuid']
                    if not has_item:
                        break
                if item['uuid'] > save['pid']:
                    save['pid'] = item['uuid']

    def schedule(self, message, save):
        """
        根据router的queue消息，计划爬虫任务
        :param message route传递过来的消息
        :param save 传递的上下文
        :return 包含uuid, url的字典迭代器，为SpiderTaskDB中数据
        :notice 该方法返回的迭代器用于plantask生成queue消息，以便fetch听取，消息格式为
        {"mode": handler mode, "uuid": SpiderTask uuid, "url": SpiderTask url}
        """
        mode = message['mode']
        if not 'id' in save:
            '''
            初始化上下文中的id参数,该参数用于数据查询
            '''
            save['id'] = 0
        if mode == ROUTER_MODE_PROJECT:
            '''
            按项目分发的计划任务
            '''
            if not 'tid' in save:
                '''
                初始化上下文中的tid参数,该参数用于站点数据查询
                '''
                save['tid'] = 0
            for item in self.db['TaskDB'].get_new_list(save['tid'], where={"pid": message['item'], "type": {"$in": [TASK_TYPE_SEARCH]}, "mediaType": {"$in": [MEDIA_TYPE_WECHAT]}}):
                self.debug("%s schedule task: %s" % (self.__class__.__name__, str(item)))
                while True:
                    has_item = False
                    #以站点为单位获取计划中的爬虫任务
                    for each in self.schedule_by_task(item, message['h-mode'], save):
                        yield each
                        has_item = True
                    if not has_item:
                        self.debug("%s schedule task end" % (self.__class__.__name__))
                        break
                if item['uuid'] > save['tid']:
                    save['tid'] = item['uuid']
        elif mode == ROUTER_MODE_SITE:
            '''
            按站点分发的计划任务
            '''
            if not 'tid' in save:
                '''
                初始化上下文中的tid参数,该参数用于站点数据查询
                '''
                save['tid'] = 0
            for item in self.db['TaskDB'].get_new_list(save['tid'], where={"pid": message['item'], "type": {"$in": [TASK_TYPE_SEARCH]}, "mediaType": {"$in": [MEDIA_TYPE_WECHAT]}}):
                self.debug("%s schedule task: %s" % (self.__class__.__name__, str(item)))
                #获取该站点计划中的爬虫任务
                while True:
                    has_item = False
                    for each in self.schedule_by_task(item, message['h-mode'], save):
                        yield each
                        has_item = True
                    if not has_item:
                        self.debug("%s schedule task end" % (self.__class__.__name__))
                        break
                if item['uuid'] > save['tid']:
                    save['tid'] = item['uuid']
        elif mode == ROUTER_MODE_TASK:
            '''
            按站点分发的计划任务
            '''
            task = self.db['TaskDB'].get_detail(message['item'])
            #获取该站点计划中的爬虫任务
            for each in self.schedule_by_task(task, message['h-mode'], save):
                yield each

    def schedule_by_task(self, task, mode, save):
        """
        获取站点下计划中的爬虫任务
        :param site 站点信息
        :param mode handler mode
        :param save 上下文参数
        :return 包含爬虫任务uuid, url的字典迭代器
        """
        plantime = int(save['now']) + int(self.ratemap[str(task.get('frequency', self.DEFAULT_RATE))][0])
        for item in self.db['SpiderTaskDB'].get_plan_list(mode, save['id'], plantime=save['now'], where={"tid": task['uuid']}, select=['uuid', 'url']):
            if not self.testing_mode:
                '''
                testing_mode打开时，数据不入库
                '''
                self.db['SpiderTaskDB'].update(item['uuid'], mode, {"plantime": plantime})
            if item['uuid'] > save['id']:
                save['id'] = item['uuid']
            yield item

    def newtask(self, message):
        """
        新建爬虫任务
        :param message [{"tid": task id, "kid": keyword uuid, "mode": handler mode}]
        """
        if 'tid' in message and message['tid']:
            tid = message['tid']
            if not isinstance(tid, (list, tuple)):
                tid = [tid]
            for each in tid:
                tasks = self.db['SpiderTaskDB'].get_list(message['mode'], {"tid": each})
                if len(list(tasks)) > 0:
                    continue
                task = self.db['TaskDB'].get_detail(each)
                if not task:
                    raise CDSpiderDBDataNotFound("task: %s not found" % each)
                uuid = 0
                while True:
                    has_word = False
                    for item in self.db['KeywordsDB'].get_new_list(uuid, select=['uuid']):
                        t = {
                            'mode': message['mode'],     # handler mode
                            'pid': task['pid'],          # project uuid
                            'sid': task['sid'],          # site uuid
                            'tid': each,   # task uuid
                            'uid': 0,                 # url uuid
                            'kid': item['uuid'],                    # keyword id
                            'url': 'base_url',          # url
                            'status': SpiderTaskDB.STATUS_ACTIVE
                        }
                        self.debug("%s newtask: %s" % (self.__class__.__name__, str(t)))
                        if not self.testing_mode:
                            '''
                            testing_mode打开时，数据不入库
                            '''
                            self.db['SpiderTaskDB'].insert(t)
                        uuid = item['uuid']
                        has_word = True
                    if not has_word:
                        break
        else:
            kid = message['kid']
            if not isinstance(kid, (list, tuple)):
                kid = [kid]
            for each in kid:
                tasks = self.db['SpiderTaskDB'].get_list(message['mode'], {"kid": each})
                if len(list(tasks)) > 0:
                    continue
                word = self.db['KeywordsDB'].get_detail(each)
                if not word:
                    raise CDSpiderDBDataNotFound("word: %s not found" % each)
                uuid = 0
                while True:
                    has_word = False
                    for item in self.db['TaskDB'].get_new_list(uuid, where={"type": TASK_TYPE_SEARCH}, select=['uuid', 'pid', 'sid', 'mediaType']):
                        t = {
                            'mode': message['mode'],     # handler mode
                            'pid': item['pid'],          # project uuid
                            'sid': item['sid'],          # site uuid
                            'tid': item['uuid'],         # task uuid
                            'uid': 0,                    # url uuid
                            'kid': each,                 # keyword id
                            'url': 'base_url',           # url
                            'status': SpiderTaskDB.STATUS_ACTIVE
                        }
                        self.debug("%s newtask: %s" % (self.__class__.__name__, str(t)))
                        if not self.testing_mode:
                            '''
                            testing_mode打开时，数据不入库
                            '''
                            self.db['SpiderTaskDB'].insert(t)
                        uuid = item['uuid']
                        has_word = True
                    if not has_word:
                        break

    def run_parse(self, rule):
        """
        根据解析规则解析源码，获取相应数据
        :param rule 解析规则
        :input self.response 爬虫结果 {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        :output self.response {"parsed": 解析结果}
        """
        parser = ListParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
        parsed = parser.parse()
        self.debug("%s parsed: %s" % (self.__class__.__name__, parsed))
        self.response['parsed'] = parsed

    def run_parse(self, rule):
        """
        根据解析规则解析源码，获取相应数据
        :param rule 解析规则
        :input self.response 爬虫结果 {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        :output self.response {"parsed": 解析结果}
        """
        parser = ListParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
        parsed = parser.parse()
        if not parsed:
            raise CDSpiderCrawlerForbidden()
        self.response['parsed'] = parsed

    def update_crawl_info(self, save):
        """
        构造文章数据的爬虫信息
        """
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
        self.db['SpiderTaskDB'].update(self.task['uuid'], self.task['mode'], {"crawltime": self.crawl_id, "crawlinfo": dict(crawlinfo_sorted), "save": save})


    def run_result(self, save):
        """
        爬虫结果处理
        :param save 保存的上下文信息
        :input self.response {"parsed": 解析结果, "final_url": 请求的url}
        """
        self.crawl_info['crawl_urls'][str(self.page)] = self.response['last_url']
        self.crawl_info['crawl_count']['page'] += 1
        self.crawl_info['crawl_count']['total'] = len(self.response['parsed'])
        self.update_crawl_info(save)
        save['update_crawlinfo'] = True
        if self.response['parsed']:
            #格式化url
            item_save = {"base_url": self.response['last_url']}
            formated = self.build_url_by_rule(self.response['parsed'], self.response['final_url'])
            item_handler = Loader(self.ctx, task = self.build_item_task(self.response['last_url']), spider = self.spider, no_sync = self.no_sync).load()
            item_handler.init(item_save)
            for item in formated:
                try:
                    item_task = self.build_item_task(item['url'])
                    item_handler.run_next(item_task)
                    item_save['retry'] = 0
                    while True:
                        self.info('Item Spider crawl start')
                        item_handler.crawl(item_save)
                        if isinstance(item_handler.response['broken_exc'], CONTINUE_EXCEPTIONS):
                            item_handler.on_continue(item_handler.response['broken_exc'], item_save)
                            continue
                        elif item_handler.response['broken_exc']:
                            raise item_handler.response['broken_exc']
                        if not item_handler.response['last_source']:
                            raise CDSpiderCrawlerError('Item Spider crawl failed')
                        self.info("Item Spider crawl end, source: %s" % utils.remove_whitespace(item_handler.response["last_source"]))
                        self.info("Item Spider parse start")
                        item_handler.parse()
                        self.info("Item Spider parse end, result: %s" % str(item_handler.response["parsed"]))
                        self.info("Item Spider result start")
                        item_handler.on_result(save)
                        self.info("Item Spider result end")
                        break
                except Exception as e:
                    self.on_error(e, save)
            if item_handler:
                item_handler.finish(item_save)

    def url_prepare(self, url):
        return url

    def build_item_task(self, url):
        """
        生成详情抓取任务并入队
        """
        message = {
            'parent-mode': HANDLER_MODE_WECHAT_SEARCH,
            'mode': HANDLER_MODE_WECHAT_ITEM,
            'url': url,
            'crawlid': self.crawl_id,
            'stid': self.task['uuid'],
        }
        return message
#        self.queue['scheduler2spider'].put_nowait(message)

    def finish(self, save):
        """
        记录抓取日志
        """
        super(WechatSearchHandler, self).finish(save)
        _u = save.pop('update_crawlinfo', False)
        if not _u:
            self.update_crawl_info(save)
        else:
            s = self.task.get("save")
            if not s:
                s = {}
            s.update(save)
            self.db['SpiderTaskDB'].update(self.task['uuid'], self.task['mode'], {"crawltime": self.crawl_id, "save": s})
