#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-11-21 20:45:56
"""
import copy
import time
import traceback
import re
from . import WemediaListHandler
from urllib.parse import urljoin
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.libs.constants import *
from cdspider.parser import ListParser
from cdspider.parser.lib import TimeParser
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium import webdriver
from selenium import webdriver
from selenium.common.exceptions import *
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

class ToutiaoListHandler(WemediaListHandler):
    """
    general list handler
    :property task 爬虫任务信息 {"mode": "list", "uuid": SpiderTask.list uuid}
                   当测试该handler，数据应为 {"mode": "list", "url": url, "listRule": 列表规则，参考列表规则}
    """

    LIST_URL = 'https://www.toutiao.com/pgc/ma/?page_type=1&max_behot_time=0&uid={uid}&media_id={mediaId}&output=json&is_json=1&count=20&from=user_profile_app&version=2&as={as}&cp={cp}&callback=jsonp4'

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
                    for each in self.db['TaskDB'].get_new_list(save['id'], where={"pid": item['uuid'], "type": {"$in": [TASK_TYPE_AUTHOR]}, "mediaType": {"$in": [MEDIA_TYPE_TOUTIAO]}}, select=["uuid"]):
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
            for item in self.db['TaskDB'].get_new_list(save['tid'], where={"pid": message['item'], "type": {"$in": [TASK_TYPE_AUTHOR]}, "mediaType": {"$in": [MEDIA_TYPE_TOUTIAO]}}):
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
            for item in self.db['TaskDB'].get_new_list(save['tid'], where={"pid": message['item'], "type": {"$in": [TASK_TYPE_AUTHOR]}, "mediaType": {"$in": [MEDIA_TYPE_TOUTIAO]}}):
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
            message['item']
            task = self.db['TaskDB'].get_detail(message['item'])
            #获取该站点计划中的爬虫任务
            for each in self.schedule_by_task(task, message['h-mode'], save):
                yield each

    def prepare(self, save):
        super(ToutiaoListHandler, self).prepare(save)
        uid = save['request']['hard_code'][0]['value']
        if not 'save' in self.task or not self.task['save']:
            self.task['save'] = {}
        if not self.task['save'].get('honey') or not self.task['save'].get('mediaId'):
            crawler = self.get_crawler({"crawler": "selenium", "method": "open", "proxy": "never"})
            request_params = copy.deepcopy(self.request_params)
            request_params['method'] = 'open'
            crawler.crawl(**request_params)
            getHoneyjs='return (ascp.getHoney())'
            honey = dict(crawler._driver.execute_script(getHoneyjs))
            save['honey'] = honey
    #        _signatureJs='return TAC.sign('+ uid +')'
    #        self.debug("%s sign js: %s" % (self.__class__.__name__, _signatureJs))
    #        _signature = crawler._driver.execute_script(_signatureJs)
    #        save['_signature'] = _signature
            mediaIdJs = 'return userInfo.mediaId'
            mediaId = crawler._driver.execute_script(mediaIdJs)
            save['mediaId'] = mediaId
            self.request_params['url'] = self.LIST_URL.format(uid=uid, mediaId=mediaId, **honey)
#        self.request_params['url'] = 'https://www.toutiao.com/c/user/article/?page_type=1&user_id=' + uid + '&max_behot_time=0&count=20&as='+ honey['as'] +'&cp='+ honey['cp'] +'&_signature='+ _signature
        else:
            self.request_params['url'] = self.LIST_URL.format(uid=uid, mediaId=self.task['save']['mediaId'], **self.task['save']['honey'])
        self.request_params['headers'] = {'Host': 'www.toutiao.com', 'User-Agent': 'Mozilla/5.0'}
