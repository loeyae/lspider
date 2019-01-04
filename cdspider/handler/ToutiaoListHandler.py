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
from cdspider.database.base import *
from cdspider.libs.constants import *
from cdspider.parser import ListParser, CustomParser
from cdspider.libs import utils
from cdspider.parser.lib import TimeParser

class ToutiaoListHandler(WemediaListHandler):
    """
    general list handler
    :property task 爬虫任务信息 {"mode": "list", "uuid": SpiderTask.list uuid}
                   当测试该handler，数据应为 {"mode": "list", "url": url, "listRule": 列表规则，参考列表规则}
    """

    TAB_ARTICLE = 'article'
    TAB_VIDEO = 'video'
    TAB_TOUTIAO = 'toutiao'

    LIST_URL = 'https://www.toutiao.com/pgc/ma/?page_type=1&max_behot_time={max_behot_time}&uid={uid}&media_id={mediaId}&output=json&is_json=1&count=20&from=user_profile_app&version=2&as={as}&cp={cp}&callback=jsonp4'
    VIDEO_URL = 'https://www.toutiao.com/pgc/ma/?page_type=0&max_behot_time={max_behot_time}&uid={uid}&media_id={mediaId}&output=json&is_json=1&count=20&from=user_profile_app&version=2&as={as}&cp={cp}&callback=jsonp4'
    TOUTIAO_URL = 'https://www.toutiao.com/api/pc/feed/?category=pc_profile_ugc&utm_source=toutiao&visit_user_id={uid}&max_behot_time={max_behot_time}'

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
                    for each in self.db['TaskDB'].get_new_list(save['id'], where={"pid": item['uuid'], "type": {"$in": [TASK_TYPE_AUTHOR]}, "mediaType": MEDIA_TYPE_WEMEDIA, "wemediaType": WEMEDIA_TYPE_TOUTIAO}, select=["uuid"]):
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
            for item in self.db['TaskDB'].get_new_list(save['tid'], where={"pid": message['item'], "type": {"$in": [TASK_TYPE_AUTHOR]}, "mediaType": MEDIA_TYPE_WEMEDIA, "wemediaType": WEMEDIA_TYPE_TOUTIAO}):
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
            for item in self.db['TaskDB'].get_new_list(save['tid'], where={"pid": message['item'], "type": {"$in": [TASK_TYPE_AUTHOR]}, "mediaType": MEDIA_TYPE_WEMEDIA, "wemediaType": WEMEDIA_TYPE_TOUTIAO}):
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

    def newtask(self, message):
        """
        新建爬虫任务
        :param message [{"uid": url uuid, "mode": handler mode}]
        """
        uid = message['uid']
        if not isinstance(uid, (list, tuple)):
            uid = [uid]
        for each in uid:
            spiderTasks = self.db['SpiderTaskDB'].get_list(message['mode'], {"uid": each})
            if len(list(spiderTasks)) > 0:
                continue
            author = self.db['AuthorDB'].get_detail(each)
            if not author:
                raise CDSpiderDBDataNotFound("author: %s not found" % each)
            ruleList = self.db['AuthorListRuleDB'].get_list(where={"tid": author['tid']}, select=["uuid"])
            for rule in ruleList:
                task = {
                    'mode': message['mode'],     # handler mode
                    'pid': author['pid'],        # project uuid
                    'sid': author['sid'],        # site uuid
                    'tid': author['tid'],        # task uuid
                    'uid': each,                 # url uuid
                    'kid': rule['uuid'],         # keyword id
                    'url': "base_url",           # url
                    'status': self.db['SpiderTaskDB'].STATUS_ACTIVE,
                    'save': {'tab': self.TAB_ARTICLE}
                }
                self.debug("%s newtask: %s" % (self.__class__.__name__, str(task)))
                if not self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    self.db['SpiderTaskDB'].insert(copy.deepcopy(task))

    def init_process(self, save):
        """
        初始化爬虫流程
        :output self.process {"request": 请求设置, "parse": 解析规则, "paging": 分页规则, "unique": 唯一索引规则}
        """
        rule = self.match_rule(save)
        self.process = rule

    def match_rule(self, save):
        """
        获取匹配的规则
        """
        if "authorListRule" in self.task:
            '''
            如果task中包含列表规则，则读取相应的规则，否则在数据库中查询
            '''
            rule = copy.deepcopy(self.task['authorListRule'])
            author = copy.deepcopy(self.task['author'])
            if not author:
                raise CDSpiderError("author not exists")
        else:
            author = self.db['AuthorDB'].get_detail(self.task['uid'])
            if not author:
                self.db['SpiderTaskDB'].delete(self.task['uuid'], self.task['mode'])
                raise CDSpiderDBDataNotFound("author: %s not exists" % self.task['uid'])
            if author['status'] != AuthorDB.STATUS_ACTIVE:
                self.db['SpiderTaskDB'].disable(self.task['uuid'], self.task['mode'])
                raise CDSpiderHandlerError("author: %s not active" % self.task['uid'])
            rule = self.db['AuthorListRuleDB'].get_detail(self.task['kid'])
            if not rule:
                self.db['SpiderTaskDB'].disable(self.task['uuid'], self.task['mode'])
                raise CDSpiderDBDataNotFound("author rule by tid: %s not exists" % author['tid'])
            if rule['status'] != AuthorListRuleDB.STATUS_ACTIVE:
                raise CDSpiderHandlerError("author rule: %s not active" % rule['uuid'])
        parameters = author.get('parameters')
        if parameters:
            save['request'] = {
                "hard_code": parameters.get('hard'),
                "random": parameters.get('randoms'),
            }
        self.task['url'] = rule['baseUrl']
        return rule

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
    #        _signatureJs='return TAC.sign('+ uid +')'
    #        self.debug("%s sign js: %s" % (self.__class__.__name__, _signatureJs))
    #        _signature = crawler._driver.execute_script(_signatureJs)
    #        save['_signature'] = _signature
            mediaIdJs = 'return userInfo.mediaId'
            mediaId = crawler._driver.execute_script(mediaIdJs)
            self.task['save']['mediaId'] = mediaId
            self.task['save']['honey'] = honey
#        self.request_params['url'] = 'https://www.toutiao.com/c/user/article/?page_type=1&user_id=' + uid + '&max_behot_time=0&count=20&as='+ honey['as'] +'&cp='+ honey['cp'] +'&_signature='+ _signature
        self.request_params['url'] = utils.format_(self.process['jsonUrl'], {"uid": uid, "mediaId": self.task['save']['mediaId'], "max_behot_time": 0, "as": self.task['save']['honey']['as'], "cp": self.task['save']['honey']['cp']})
        self.request_params['headers'] = {'Host': 'www.toutiao.com', 'User-Agent': 'Mozilla/5.0'}

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
            raise CDSpiderCrawlerNoResponse()
        self.response['parsed'] = parsed

    def run_result(self, save):
        """
        爬虫结果处理
        :param save 保存的上下文信息
        :input self.response {"parsed": 解析结果, "final_url": 请求的url}
        """
        self.crawl_info['crawl_urls'][str(self.page)] = self.response['last_url']
        self.crawl_info['crawl_count']['page'] += 1
        tab = self.task.get('save', {}).get('tab', self.TAB_ARTICLE)
        if self.response['parsed']:
            ctime = self.crawl_id
            new_count = self.crawl_info['crawl_count']['new_count']
            #格式化url
            formated = self.build_url_by_rule(self.response['parsed'], self.response['final_url'])
            for item in formated:
                self.crawl_info['crawl_count']['total'] += 1
                if self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    inserted, unid = (True, {"acid": "test_mode", "ctime": ctime})
                    self.debug("%s test mode: %s" % (self.__class__.__name__, unid))
                else:
                    #生成文章唯一索引并判断文章是否已经存在
                    inserted, unid = self.db['UniqueDB'].insert(self.get_unique_setting(item['url'], {}), ctime)
                    self.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
                if inserted:
                    crawlinfo =  self._build_crawl_info(self.response['final_url'])
                    typeinfo = utils.typeinfo(item['url'])
                    result = self._build_result_info(final_url=item['url'], typeinfo=typeinfo, crawlinfo=crawlinfo, result=item, **unid)
                    if self.testing_mode:
                        '''
                        testing_mode打开时，数据不入库
                        '''
                        self.debug("%s result: %s" % (self.__class__.__name__, result))
                    else:
                        result_id = self.db['ArticlesDB'].insert(result)
                        if not result_id:
                            raise CDSpiderDBError("Result insert failed")
                        self.add_interact(rid=result_id, result=item, **unid)
                        self.build_item_task(result_id)
                    self.crawl_info['crawl_count']['new_count'] += 1
                else:
                    self.crawl_info['crawl_count']['repeat_count'] += 1
            if self.crawl_info['crawl_count']['new_count'] - new_count == 0:
                self.crawl_info['crawl_count']['repeat_page'] += 1
                self.on_repetition(save)

    def _build_result_info(self, **kwargs):
        """
        构造文章数据
        :param result 解析到的文章信息 {"title": 标题, "author": 作者, "pubtime": 发布时间, "content": 内容}
        :param final_url 请求的url
        :param typeinfo 域名信息 {'domain': 一级域名, 'subdomain': 子域名}
        :param crawlinfo 爬虫信息
        :param unid 文章唯一索引
        :param ctime 抓取时间
        :param status 状态
        :input self.crawl_id 爬取时刻
        """
        now = int(time.time())
        result = kwargs.get('result', {})
        pubtime = TimeParser.timeformat(str(result.pop('pubtime', '')))
        if pubtime and pubtime > now:
            pubtime = now
        r = {
            'status': kwargs.get('status', ArticlesDB.STATUS_INIT),
            'mediaType': MEDIA_TYPE_TOUTIAO,
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

#    def on_next(self, save):
#        """
#        下一页解析
        #"""
#        if self.page > 10:
#            raise  CDSpiderCrawlerMoreThanMaximum()
#        self.page += 1
#        tab = self.task.get('save', {}).get('tab', self.TAB_ARTICLE)
#        rule = {
#            "has_more": {
#                "filter": "@json:has_more"
#            },
#            "max_behot_time": {
#                "filter": "@json:next.max_behot_time"
#            }
#        }
#        parser = CustomParser(source=self.response['last_source'], ruleset=rule, log_level=self.log_level, url=self.response['final_url'])
#        parsed = parser.parse()
#        if 'has_more' in parsed and parsed['has_more'] == '1':
#            uid = save['request']['hard_code'][0]['value']
#            if tab == self.TAB_VIDEO:
#                save['next_url'] = self.request_params['url'] = self.VIDEO_URL.format(uid=uid, mediaId=self.task['save']['mediaId'], max_behot_time=parsed['max_behot_time'], **self.task['save']['honey'])
#            elif tab == self.TAB_TOUTIAO:
#                save['next_url'] = self.request_params['url'] = self.TOUTIAO_URL.format(uid=uid, mediaId=self.task['save']['mediaId'], max_behot_time=parsed['max_behot_time'])
#            else:
#                save['next_url'] = self.request_params['url'] = self.LIST_URL.format(uid=uid, mediaId=self.task['save']['mediaId'], max_behot_time=parsed['max_behot_time'], **self.task['save']['honey'])
#        else:
#            raise CDSpiderCrawlerNoNextPage(base_url=save.get("base_url", ''), current_url=save.get('request_url'))
