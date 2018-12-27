#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-27 15:06:26
"""
import re
import copy
import time
import traceback
import urllib.request
from . import GeneralSearchHandler
from urllib.parse import urljoin, urlparse, quote_plus
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.libs.constants import *
from cdspider.parser import ListParser
from cdspider.parser.lib import TimeParser

class WeiboSearchHandler(GeneralSearchHandler):
    """
    weibo search handler
    :property task 爬虫任务信息 {"mode": "weibo-search", "uuid": SpiderTask.list uuid}
                   当测试该handler，数据应为 {"mode": "weibo-search", "keyword": 关键词规则, "authorListRule": 列表规则，参考列表规则}
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
                    for each in self.db['TaskDB'].get_new_list(save['id'], where={"pid": item['uuid'], "type": {"$in": [TASK_TYPE_SEARCH]}, "mediaType": {"$in": [MEDIA_TYPE_WEIBO]}}, select=["uuid"]):
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
            for item in self.db['TaskDB'].get_new_list(save['tid'], where={"pid": message['item'], "type": {"$in": [TASK_TYPE_SEARCH]}, "mediaType": {"$in": [MEDIA_TYPE_WEIBO]}}):
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
            for item in self.db['TaskDB'].get_new_list(save['tid'], where={"pid": message['item'], "type": {"$in": [TASK_TYPE_SEARCH]}, "mediaType": {"$in": [MEDIA_TYPE_WEIBO]}}):
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

    def run_result(self, save):
        """
        爬虫结果处理
        :param save 保存的上下文信息
        :input self.response {"parsed": 解析结果, "final_url": 请求的url}
        """
        self.crawl_info['crawl_urls'][str(self.page)] = self.response['final_url']
        self.crawl_info['crawl_count']['page'] += 1
        if self.response['parsed']:
            ctime = self.crawl_id
            new_count = self.crawl_info['crawl_count']['new_count']
            for each in self.response['parsed']:
                if not each['url']:
                    continue
                self.crawl_info['crawl_count']['total'] += 1
                if self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    inserted, unid = (True, {"acid": "test_mode", "ctime": ctime})
                    self.debug("%s test mode: %s" % (self.__class__.__name__, unid))
                else:
                    #生成唯一ID, 并判断是否已存在
                    inserted, unid = self.db['UniqueDB'].insert(self.get_unique_setting(each['url'], {}), ctime)
                    self.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
                if inserted:
                    result = self.build_weibo_info(result=each, final_url=self.response['final_url'], **unid)
                    self.debug("%s result: %s" % (self.__class__.__name__, result))
                    if not self.testing_mode:
                        '''
                        testing_mode打开时，数据不入库
                        '''
                        result_id = self.db['WeiboInfoDB'].insert(result)
                        if not result_id:
                            raise CDSpiderDBError("Result insert failed")
                    self.crawl_info['crawl_count']['new_count'] += 1
                else:
                    self.crawl_info['crawl_count']['repeat_count'] += 1
            if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
                self.crawl_info['crawl_count']['repeat_page'] += 1
                self.on_repetition(save)

    def build_weibo_info(self, **kwargs):
        """
        构造评论数据
        """
        now = int(time.time())
        result = kwargs.pop('result')
        #格式化发布时间
        pubtime = TimeParser.timeformat(str(result.pop('pubtime', '')))
        if pubtime and pubtime > now:
            pubtime = now
        #爬虫信息记录
        result['crawlinfo'] = {
            'pid': self.task['pid'],                        # project id
            'sid': self.task['sid'],                        # site id
            'tid': self.task['tid'],                        # task id
            'uid': self.task['uid'],                        # url id
            'kid': self.task['kid'],                        # url id
            'ruleId': self.process['uuid'],                 # authorListRule id
            'list_url': self.task['url'],            # 列表url
        }
        result['acid'] = kwargs.pop('unid')
        result['ctime'] = kwargs.pop('ctime')
        return result

    def finish(self, save):
        """
        记录抓取日志
        """
        super(WeiboHandler, self).finish(save)
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

    def result2attach(self, save, domain, subdomain=None):
        """
        根据详情页生成附加任务
        :param save 传递的上下文信息
        :param domain 域名
        :param subdomain 子域名
        """
        self.debug("%s new attach task starting" % (self.__class__.__name__))
        if self.page != 1:
            '''
            只在第一页时执行
            '''
            return
        self.debug("%s new comment task starting" % (self.__class__.__name__))
        self.result2comment(save, domain, subdomain)
        self.debug("%s new comment task end" % (self.__class__.__name__))
        self.debug("%s new interact task starting" % (self.__class__.__name__))
        self.result2interact(save, domain, subdomain)
        self.debug("%s new interact task end" % (self.__class__.__name__))
        self.debug("%s new attach task end" % (self.__class__.__name__))

    def result2comment(self, save, domain, subdomain = None):
        """
        根据详情页生成评论任务
        :param save 传递的上下文信息
        :param domain 域名
        :param subdomain 子域名
        """
        def build_task(rule):
            try:
                url, data = utils.build_attach_url(CustomParser, self.response['last_source'], self.response['final_url'], rule, self.log_level)
                if url:
                    '''
                    根据规则生成出任务url，则为成功
                    '''
                    cid = self.build_comment_task(url, data, rule)
                    if cid:
                        self.task['crawlinfo']['commentRule'] = rule['uuid']
                        self.task['crawlinfo']['commentTaskId'] = cid
                        self.debug("%s new comment task: %s" % (self.__class__.__name__, str(cid)))
                    return True
                return False
            except:
                self.error(traceback.format_exc())
                return False
        #通过子域名获取评论任务
        ruleset = self.db['CommentRuleDB'].get_list_by_subdomain(subdomain, where={"status": self.db['CommentRuleDB'].STATUS_ACTIVE})
        for rule in ruleset:
            self.debug("%s comment task rule: %s" % (self.__class__.__name__, str(rule)))
            if build_task(rule):
                return
        #通过域名获取评论任务
        ruleset = self.db['CommentRuleDB'].get_list_by_domain(domain, where={"status": self.db['CommentRuleDB'].STATUS_ACTIVE})
        for rule in ruleset:
            self.debug("%s comment task rule: %s" % (self.__class__.__name__, str(rule)))
            if build_task(rule):
                return

    def result2interact(self, save, domain, subdomain = None):
        """
        根据详情页生成互动数任务
        :param save 传递的上下文信息
        :param domain 域名
        :param subdomain 子域名
        """
        def buid_task(rule):
            try:
                url, data = utils.build_attach_url(CustomParser, self.response['last_source'], self.response['final_url'], rule, self.log_level)
                if url:
                    '''
                    根据规则生成出任务url，则为成功
                    '''
                    cid = self.build_interact_task(url, data, rule)
                    if cid:
                        self.task['crawlinfo']['interactRule'] = rule['uuid']
                        self.task['crawlinfo']['interactTaskId'] = cid
                        if 'interactRuleList' in  self.task['crawlinfo']:
                             self.task['crawlinfo']['interactRuleList'][str(rule['uuid'])] = cid
                        else:
                            self.task['crawlinfo']['interactRuleList'] = {str(rule['uuid']): cid}
                        self.debug("%s new interact task: %s" % (self.__class__.__name__, str(cid)))
            except:
                self.error(traceback.format_exc())
        #通过子域名获取互动数任务
        ruleset = self.db['AttachmentDB'].get_list_by_subdomain(subdomain, where={"status": self.db['AttachmentDB'].STATUS_ACTIVE})
        for rule in ruleset:
            self.debug("%s interact task rule: %s" % (self.__class__.__name__, str(rule)))
            buid_task(rule)
        #通过域名获取互动数任务
        ruleset = self.db['AttachmentDB'].get_list_by_domain(domain, where={"status": self.db['AttachmentDB'].STATUS_ACTIVE})
        for rule in ruleset:
            self.debug("%s interact task rule: %s" % (self.__class__.__name__, str(rule)))
            buid_task(rule)

    def build_comment_task(self, url, data, rule):
        """
        构造评论任务
        :param url taks url
        :param rule 评论任务规则
        """
        task = {
            'mediaType': MEDIA_TYPE_WEIBO,
            'mode': HANDLER_MODE_COMMENT,                           # handler mode
            'pid': self.task['crawlinfo'].get('pid', 0),            # project id
            'sid': self.task['crawlinfo'].get('sid', 0),            # site id
            'tid': self.task['crawlinfo'].get('tid', 0),            # task id
            'uid': self.task['crawlinfo'].get('uid', 0),            # url id
            'kid': rule['uuid'],                                    # rule id
            'url': url,                                             # url
            'parentid': self.task['rid'],                           # article id
            'status': self.db['SpiderTaskDB'].STATUS_ACTIVE,
            'expire': 0 if int(rule['expire']) == 0 else int(time.time()) + int(rule['expire']),
            'save': {"hard_code": data}
        }
        self.debug("%s build comment task: %s" % (self.__class__.__name__, str(task)))
        if not self.testing_mode:
            '''
            testing_mode打开时，数据不入库
            '''
            try:
                l = self.db['SpiderTaskDB'].get_list(HANDLER_MODE_COMMENT, where={"uid": task['uid'], "kid": task['kid'], "parentid": task['parentid']})
                if len(list(l)) == 0:
                    return self.db['SpiderTaskDB'].insert(task)
                return None
            except:
                return None
        else:
            return 'testing_mode'

    def build_interact_task(self, url, data, rule):
        """
        构造互动数任务
        :param url taks url
        :param rule 互动数任务规则
        """
        task = {
            'mediaType': MEDIA_TYPE_WEIBO,
            'mode': HANDLER_MODE_INTERACT,                          # handler mode
            'pid': self.task['crawlinfo'].get('pid', 0),            # project id
            'sid': self.task['crawlinfo'].get('sid', 0),            # site id
            'tid': self.task['crawlinfo'].get('tid', 0),            # task id
            'uid': self.task['crawlinfo'].get('uid', 0),            # url id
            'kid': rule['uuid'],                                    # rule id
            'url': url,                                             # url
            'parentid': self.task['rid'],                           # article id
            'status': self.db['SpiderTaskDB'].STATUS_ACTIVE,
            'expire': 0 if int(rule['expire']) == 0 else int(time.time()) + int(rule['expire']),
            'save': {"hard_code": data}
        }
        self.debug("%s build interact task: %s" % (self.__class__.__name__, str(task)))
        if not self.testing_mode:
            '''
            testing_mode打开时，数据不入库
            '''
            try:
                l = self.db['SpiderTaskDB'].get_list(HANDLER_MODE_COMMENT, where={"uid": task['uid'], "kid": task['kid'], "parentid": task['parentid']})
                if len(list(l)) == 0:
                    return self.db['SpiderTaskDB'].insert(task)
                return None
            except:
                return None
        else:
            return 'testing_mode'
