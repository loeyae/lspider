#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-11 11:10:18
"""
import copy
import time
import traceback
from . import BaseHandler
from urllib.parse import urljoin
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.libs.constants import *
from cdspider.parser import ListParser
from cdspider.parser.lib import TimeParser

class WemediaListHandler(BaseHandler):
    """
    wemedia list handler
    :property task 爬虫任务信息 {"mode": "wemedia-list", "uuid": SpiderTask.author uuid}
                   当测试该handler，数据应为 {"mode": "wemedia-list", "author": 自媒体号设置，参考自媒体号, "authorListRule": 自媒体列表规则，参考自媒体列表规则}
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
                    for each in self.db['TaskDB'].get_new_list(save['id'], where={"pid": item['uuid'], "type": {"$in": [TASK_TYPE_AUTHOR]}, "mediaType": MEDIA_TYPE_WEMEDIA, "wemediaType": WEMEDIA_TYPE_GENERAL}, select=["uuid"]):
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
        :notice 该方法返回的�代��用于plantask生成queue消息，以便fetch听取，消息格式为
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
            for item in self.db['TaskDB'].get_new_list(save['tid'], where={"pid": message['item'], "type": {"$in": [TASK_TYPE_AUTHOR]}, "mediaType": MEDIA_TYPE_WEMEDIA, "wemediaType": WEMEDIA_TYPE_GENERAL}):
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
            for item in self.db['TaskDB'].get_new_list(save['tid'], where={"pid": message['item'], "type": {"$in": [TASK_TYPE_AUTHOR]}, "mediaType": MEDIA_TYPE_WEMEDIA, "wemediaType": WEMEDIA_TYPE_GENERAL}):
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
        rule = self.db['AuthorListRuleDB'].get_detail_by_tid(task['uuid'])
        for item in self.db['SpiderTaskDB'].get_plan_list(mode, save['id'], plantime=save['now'], where={"tid": task['uuid']}, select=['uuid', 'url', 'uid']):
            if not self.testing_mode:
                '''
                testing_mode打开时，数据不入库
                '''
                author = self.db['AuthorDB'].get_detail(item['uid'])
                if not author:
                    self.db['SpiderTaskDB'].delete(item['uuid'], mode)
                    continue
                frequency = str(author.get('frequency', self.DEFAULT_RATE))
                plantime = int(save['now']) + int(self.ratemap[frequency][0])
                self.db['SpiderTaskDB'].update(item['uuid'], mode, {"plantime": plantime, "frequency": frequency, 'rid': rule['uuid']})
            if item['uuid'] > save['id']:
                save['id'] = item['uuid']
            yield item

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
            task = {
                'mode': message['mode'],     # handler mode
                'pid': author['pid'],        # project uuid
                'sid': author['sid'],        # site uuid
                'tid': author['tid'],        # task uuid
                'uid': each,                 # url uuid
                'kid': 0,                    # keyword id
                'url': "base_url",           # url
                'status': self.db['SpiderTaskDB'].STATUS_ACTIVE,
            }
            self.debug("%s newtask: %s" % (self.__class__.__name__, str(task)))
            if not self.testing_mode:
                '''
                testing_mode打开时，数据不入库
                '''
                self.db['SpiderTaskDB'].insert(task)

    def get_scripts(self):
        """
        获取列表规则中的自定义脚本
        :return 自定义脚本
        """
        save = {}
        try:
            if "uuid" in self.task and self.task['uuid']:
                task = self.db['SpiderTaskDB'].get_detail(self.task['uuid'], self.task['mode'])
                if not task:
                    raise CDSpiderDBDataNotFound("SpiderTask: %s not exists" % self.task['uuid'])
                self.task.update(task)
            rule = self.match_rule(save)
            return rule.get("scripts", None)
        except:
            self.error(traceback.format_exc())
            return None

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
            rule = self.db['AuthorListRuleDB'].get_detail_by_tid(author['tid'])
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

    def run_parse(self, rule):
        """
        根据解析规则解析源码，获取相应数据
        :param rule 解析规则
        :input self.response 爬虫结果 {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        :output self.response {"parsed": 解析结果}
        """
        parser = ListParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
        self.response['parsed'] = parser.parse()

    def _build_crawl_info(self, final_url):
        """
        构造文章数据的爬虫信息
        :param final_url 请求的url
        :input self.task 爬虫任务信息
        :input self.crawl_id 爬虫运行时刻
        """
        return {
                'listMode': self.task['mode'],
                "stid": self.task.get("uuid", 0),   # SpiderTask uuid
                "uid": self.task.get("uid", 0),     # url id
                "pid": self.task.get('pid', 0),     # project id
                "sid": self.task.get('sid', 0),     # site id
                "tid": self.task.get('tid', 0),     # task id
                "kid": self.task.get('kid', 0),     # keyword id
                "listRule": self.process.get('uuid', 0),   # 规则ID
                "list_url": final_url,              # 列表url
                "list_crawl_id": self.crawl_id,     # 列表抓取时间
        }

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
            'url': kwargs['final_url'],
            'domain': kwargs.get("typeinfo", {}).get('domain', None),          # 站点域名
            'subdomain': kwargs.get("typeinfo", {}).get('subdomain', None),    # 站点域名
            'mediaType': self.process.get('mediaType', self.task['task'].get('mediaType', MEDIA_TYPE_OTHER)),
            'title': result.pop('title', None),                                # 标题
            'author': result.pop('author', None),                              # 作者
            'pubtime': pubtime,                                                # 发布时间
            'channel': result.pop('channel', None),                            # 频道信息
            'crawlinfo': kwargs.get('crawlinfo'),
            'acid': kwargs['unid'],                                            # unique str
            'ctime': kwargs.get('ctime', self.crawl_id),
            }
        return r

    def _build_interact_info(self, **kwargs):
        result = kwargs.get('result', {})
        r = {
            'crawlinfo': {
                    'pid': self.task['pid'],                        # project id
                    'sid': self.task['sid'],                        # site id
                    'tid': self.task['tid'],                        # task id
                    'uid': self.task['uid'],                        # url id
                    'ruleId': 0,                                    # interactionNumRule id
                    'final_url': self.response['final_url'],         # 列表url
                },
            'status': kwargs.get('status', ArticlesDB.STATUS_INIT),
            'viewNum': result.get('view', kwargs.get('viewNum', 0)),
            'spreadNum': result.get('repost', kwargs.get('spreadNum', 0)),
            'commentNum': result.get('comment', kwargs.get('commentNum', 0)),
            'likeNum': result.get('praise', kwargs.get('likeNum', 0)),
            'playNum': result.get('play_count', kwargs.get('playNum', 0)),
            'rid': kwargs['rid'],
            'acid': kwargs['unid'],                                            # unique str
            'ctime': kwargs.get('ctime', self.crawl_id),
            }
        return r

    def add_interact(self, **kwargs):
        r = self._build_interact_info(**kwargs)
        try:
            self.db['AttachDataDB'].insert(r)
        except:
            pass

    def run_result(self, save):
        """
        爬虫结果处理
        :param save 保存的上下文信息
        :input self.response {"parsed": 解析结果, "final_url": 请求的url}
        """
        self.crawl_info['crawl_urls'][str(self.page)] = self.response['last_url']
        self.crawl_info['crawl_count']['page'] += 1
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

    def url_prepare(self, url):
        return url

    def build_url_by_rule(self, data, base_url = None):
        """
        根据url规则格式化url
        :param data 解析到的数据
        :param base_url 基本url
        """
        if not base_url:
            base_url = self.task.get('url')
        urlrule = self.process.get("url")
        formated = []
        for item in data:
            if not 'url' in item or not item['url']:
                continue
            if item['url'].startswith('javascript') or item['url'] == '/':
                continue
            try:
                item['url'] = self.url_prepare(item['url'])
            except:
                self.error(traceback.format_exc())
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
            'mediaType': self.process.get('mediaType', self.task['task'].get('mediaType', MEDIA_TYPE_OTHER)),
            'rid': rid,
        }
        self.queue['scheduler2spider'].put_nowait(message)

    def finish(self, save):
        """
        记录抓取日志
        """
        super(WemediaListHandler, self).finish(save)
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
